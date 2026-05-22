"""Unreg block-trade detection.

A block sold by an affiliate / 10%+ holder is typically
signalled by a Form 144 (notice of intent to sell) the
day before, plus one or more Form 4s (executed sale)
within 1-3 business days. For 5%+ holders below the
Section 16 threshold only the 144 fires; for board /
officer insiders only the Form 4 fires.

For each touched CIK we collect all 144 + Form 4
filings in a sliding window, group them into clusters,
resolve via UnregDeal, qualify against size gates, and
emit one candidate row per cluster.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from app.detect.reg import _parse_idx_date  # shared
from app.detect.thresholds import qualifies
from app.edgar.fetch import get_cached
from app.edgar.parse.form144 import parse_144
from app.edgar.parse.form4 import parse_form4
from app.mds.massive.refs import RefRow
from app.mds.massive.splits import cumulative_factor
from app.parsers.unreg import resolve_unreg_deal
from app.trades.banks import parse_banks

CLUSTER_DAYS = 5
INDEX_DIR = Path('data/indices')


@dataclass
class FilingRef:
    cik: str
    form: str
    date_filed: date
    filename: str


def _is_unreg_form(form: str) -> bool:
    return form.startswith('144') or form in ('4', '4/A')


def _index_unreg_by_cik(
    ciks: set[str],
    lo: date,
    hi: date,
) -> dict[str, list[FilingRef]]:
    out: dict[str, list[FilingRef]] = defaultdict(list)
    sources = sorted(
        list(INDEX_DIR.glob('full.*.idx'))
        + list(INDEX_DIR.glob('daily.*.idx'))
    )
    seen: set[str] = set()
    for f in sources:
        for line in f.read_text().splitlines():
            parts = line.split('|')
            if len(parts) < 5:
                continue
            form = parts[2]
            if not _is_unreg_form(form):
                continue
            cik = parts[0].lstrip('0') or '0'
            if cik not in ciks:
                continue
            d = _parse_idx_date(parts[3])
            if d is None or d < lo or d > hi:
                continue
            fn = parts[4]
            if fn in seen:
                continue
            seen.add(fn)
            out[cik].append(
                FilingRef(cik, form, d, fn)
            )
    for cik in out:
        out[cik].sort(key=lambda x: x.date_filed)
    return out


def _split_into_clusters(
    filings: list[FilingRef],
) -> list[list[FilingRef]]:
    if not filings:
        return []
    clusters: list[list[FilingRef]] = []
    cur = [filings[0]]
    for f in filings[1:]:
        if (f.date_filed - cur[-1].date_filed).days <= CLUSTER_DAYS:
            cur.append(f)
        else:
            clusters.append(cur)
            cur = [f]
    clusters.append(cur)
    return clusters


def _cluster_to_deal(cluster, sym, cik):
    """Parse + resolve. Returns (deal, sale_date) or
    (None, None)."""
    f4_txns = []
    f144s = []
    sale_dates: list[date] = []
    for fr in cluster:
        raw = get_cached(fr.filename)
        if raw is None:
            continue
        if fr.form in ('4', '4/A'):
            for t in parse_form4(raw):
                f4_txns.append(t)
                try:
                    d = date.fromisoformat(t.txn_date)
                    sale_dates.append(d)
                except (ValueError, TypeError):
                    pass
        elif fr.form.startswith('144'):
            p = parse_144(raw)
            if p:
                f144s.append(p)
                if p.sale_date:
                    # Form 144 sale_date is MM/DD/YYYY
                    try:
                        m, d_, y = p.sale_date.split('/')
                        sale_dates.append(
                            date(int(y), int(m), int(d_))
                        )
                    except (ValueError, AttributeError):
                        pass

    if not f4_txns and not f144s:
        return None, None

    # Best price_date / trade_date guess:
    # - prefer earliest sale_date across the cluster
    # - else earliest filing date in cluster
    if sale_dates:
        trade_date = min(sale_dates)
    else:
        trade_date = min(fr.date_filed for fr in cluster)

    deal = resolve_unreg_deal(
        cik=cik, symbol=sym,
        price_date=trade_date,
        trade_date=trade_date,
        intraday=False,
        form4_txns=f4_txns,
        f144_filings=f144s,
    )
    return deal, trade_date


def _row_from_deal(deal, sym, ref, trade_date):
    if deal is None:
        return None
    shares = deal.block_shares
    if shares <= 0:
        return None
    price = deal.block_price
    if price <= 0:
        return None
    f = cumulative_factor(sym, trade_date)
    adj_shares = int(round(shares * f))
    adj_price = price / f
    adj_notional = adj_shares * adj_price
    if not qualifies(
        notional=adj_notional,
        adj_shares=adj_shares,
        mkt_cap=ref.mkt_cap,
        ref_price=ref.price,
        adv=ref.adv,
    ):
        return None

    # Top broker -> canonical bank code
    banks: list[str] = []
    for br in deal.brokers:
        mapped = parse_banks(br)
        if mapped and mapped != ['Other']:
            banks = mapped[:1]
            break

    relationship = ''
    if deal.n_144 > 0:
        relationship = 'affiliate'
    elif deal.n_form4 > 0:
        relationship = 'insider'

    seller = '; '.join(deal.sellers[:3]) if deal.sellers else ''

    return {
        'price_date': trade_date,
        'symbol': sym,
        'offer_price': float(price),
        'type': 'Unreg',
        'trade_date': trade_date,
        'intraday': False,
        'shares': shares,
        'notional': shares * price,
        'split_factor': float(f),
        'adj_shares': adj_shares,
        'adj_price': float(adj_price),
        'seller': seller,
        'relationship': relationship,
        'banks': banks,
        'cik': deal.cik,
        'evidence': 'detected',
        'source': 'sync',
    }


def detect_unreg_blocks(
    *,
    touched_ciks: Iterable[str],
    cik_map: dict[str, RefRow],
    lo: date,
    hi: date,
) -> list[dict]:
    eligible = {c for c in touched_ciks if c in cik_map}
    if not eligible:
        return []

    by_cik = _index_unreg_by_cik(
        eligible,
        lo - timedelta(days=CLUSTER_DAYS),
        hi + timedelta(days=CLUSTER_DAYS),
    )

    rows: list[dict] = []
    for cik, filings in by_cik.items():
        ref = cik_map[cik]
        sym = ref.symbol
        for cluster in _split_into_clusters(filings):
            if not any(lo <= f.date_filed <= hi for f in cluster):
                continue
            deal, tdt = _cluster_to_deal(cluster, sym, cik)
            if deal is None or tdt is None:
                continue
            row = _row_from_deal(deal, sym, ref, tdt)
            if row:
                rows.append(row)
    return rows
