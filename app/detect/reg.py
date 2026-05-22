"""Reg block-trade detection.

For each touched CIK we already have 424B* filings on
disk. We group them into clusters within a ±CLUSTER_DAYS
window, resolve each cluster into a RegDeal, qualify
against the size gates, and return rows in the
trades.parquet schema (one row per resolved deal).

This is the sync-time counterpart to seed_reg() in
tools/seed_goldens.py: the seeder uses golden anchor
dates; detect walks the cluster forward from any
in-window 424B and picks the FINAL filing's date as
the deal's price_date.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from app.detect.thresholds import qualifies
from app.edgar.fetch import get_cached
from app.mds.massive.refs import RefRow
from app.mds.massive.splits import cumulative_factor
from app.parsers.reg import REG_FORMS
from app.parsers.reg_deal import parse_member, resolve_deal
from app.trades.banks import parse_banks

# Window for grouping prelim+final into one deal.
# Preliminary B5/B7 typically lead the final by 1 day;
# concurrent base prospectus 424B2/B3 can lead by 2-5.
CLUSTER_DAYS = 5

INDEX_DIR = Path('data/indices')


@dataclass
class FilingRef:
    cik: str
    form: str
    date_filed: date
    filename: str


def _parse_idx_date(s: str) -> date | None:
    """Full quarterly: YYYY-MM-DD; daily: YYYYMMDD."""
    if not s:
        return None
    try:
        if len(s) >= 10 and s[4] == '-':
            return date(
                int(s[:4]), int(s[5:7]), int(s[8:10])
            )
        if len(s) == 8 and s.isdigit():
            return date(
                int(s[:4]), int(s[4:6]), int(s[6:8])
            )
    except (ValueError, IndexError):
        return None
    return None


def _index_reg_filings_by_cik(
    ciks: set[str],
    lo: date,
    hi: date,
) -> dict[str, list[FilingRef]]:
    """Walk all indices once, return per-CIK list of
    424B filings within [lo, hi]."""
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
            if form not in REG_FORMS:
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
    """Group filings whose dates lie within CLUSTER_DAYS
    of any other member. One simple chain-merge pass —
    we trade exact match accuracy for simplicity."""
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


def _row_from_deal(
    deal,
    sym: str,
    ref: RefRow,
    price_date: date,
) -> dict | None:
    """Translate a RegDeal into a trades.parquet row.
    Returns None if size gates aren't met."""
    if not deal or not deal.offer_price or not deal.shares_offered:
        return None
    f = cumulative_factor(sym, price_date)
    adj_shares = int(round(deal.shares_offered * f))
    adj_price = deal.offer_price / f
    adj_notional = adj_shares * adj_price
    if not qualifies(
        notional=adj_notional,
        adj_shares=adj_shares,
        mkt_cap=ref.mkt_cap,
        ref_price=ref.price,
        adv=ref.adv,
    ):
        return None
    banks = parse_banks(deal.underwriter) if deal.underwriter else []
    if isinstance(banks, tuple):
        banks = list(banks)
    return {
        'price_date': price_date,
        'symbol': sym,
        'offer_price': float(deal.offer_price),
        'type': 'Reg',
        'trade_date': price_date,
        'intraday': False,
        'shares': int(deal.shares_offered),
        'notional': deal.shares_offered * deal.offer_price,
        'split_factor': float(f),
        'adj_shares': adj_shares,
        'adj_price': float(adj_price),
        'seller': deal.issuer_name or '',
        'relationship': (
            'selling stockholder'
            if deal.has_selling_stockholder
            else 'company'
        ),
        'banks': banks,
        'cik': deal.cik,
        'evidence': 'detected',
        'source': 'sync',
    }


def detect_reg_blocks(
    *,
    touched_ciks: Iterable[str],
    cik_map: dict[str, RefRow],
    lo: date,
    hi: date,
) -> list[dict]:
    """Detect reg block candidates for each touched CIK.

    `cik_map` keys are unpadded CIKs -> RefRow (the live
    trading universe). Only CIKs present in the map are
    eligible — that's where we have mkt_cap/ADV/price
    to evaluate the size gates.
    """
    eligible = {c for c in touched_ciks if c in cik_map}
    if not eligible:
        return []

    # Expand the window to cover full clusters reachable
    # from any in-window filing.
    by_cik = _index_reg_filings_by_cik(
        eligible,
        lo - timedelta(days=CLUSTER_DAYS),
        hi + timedelta(days=CLUSTER_DAYS),
    )

    rows: list[dict] = []
    for cik, filings in by_cik.items():
        ref = cik_map[cik]
        sym = ref.symbol
        for cluster in _split_into_clusters(filings):
            # Skip clusters that don't intersect the
            # [lo, hi] notification window — they were
            # included only for chaining.
            if not any(lo <= f.date_filed <= hi for f in cluster):
                continue
            members = []
            for fr in cluster:
                raw = get_cached(fr.filename)
                if raw is None:
                    continue
                members.append(parse_member(
                    filename=fr.filename,
                    filing_date=fr.date_filed,
                    form_type=fr.form,
                    cik=cik,
                    raw=raw,
                ))
            deal = resolve_deal(members, sym)
            if deal is None:
                continue
            # Use price_date from the resolver (final
            # filing's date), fall back to announce.
            pdt = deal.price_date or deal.announce_date
            if pdt is None:
                continue
            row = _row_from_deal(deal, sym, ref, pdt)
            if row:
                rows.append(row)
    return rows
