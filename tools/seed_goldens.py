"""Seed trades + blocks tables from confirmed goldens.

Both tables get the same rows. trades is the loose
flagging layer; blocks is the confirmed truth (here
status='confirmed' because goldens are pre-vetted).

  reg goldens   -> RegDeal eval -> offer_price from
                   parser, shares + banks from parsed
                   cluster, type='Reg'
  unreg goldens -> OfferPx from golden, shares from
                   UnregDeal eval, type='Unreg'

    uv run python tools/seed_goldens.py [--reg] [--unreg]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.edgar.fetch import get_cached  # noqa: E402
from app.edgar.parse.form144 import parse_144  # noqa: E402
from app.edgar.parse.form4 import parse_form4  # noqa: E402
from app.mds.massive.splits import (  # noqa: E402
    cumulative_factor,
)
from app.mds.syms import resolve_cik  # noqa: E402
from app.parsers.reg_deal import (  # noqa: E402
    parse_member,
    resolve_deal,
)
from app.parsers.unreg import resolve_unreg_deal  # noqa: E402
from app.trades.banks import parse_banks  # noqa: E402
from app.trades.blocks import upsert_blocks  # noqa: E402
from app.trades.table import upsert_trades  # noqa: E402
from app.util.log import log  # noqa: E402

REG_GOLDEN = Path('data/bootstrap/regs_golden.20260516.json')
UNREG_GOLDEN = Path('data/bootstrap/unreg_golden.20260517.json')
REG_LABELS = Path('data/corpus/reg_labels.parquet')
REG_CORPUS = Path('data/corpus/reg_corpus.parquet')
INDEX_DIR = Path('data/indices')
# Direct-from-legacy seed for blocks with no SEC filing
# (foreign issuers like GFL/TMUS/CIGI etc.)
LEGACY_SEED = Path('data/bootstrap/legacy_seed.csv')
# Reference dataset used to fill in `shares` for unreg
# rows we couldn't extract from SEC filings (Rule 144A,
# foreign-private issuers, etc.). Looked up by
# (cik, PxDt) so CCCS<->CCC ticker renames resolve.
LEGACY_BLOCKS = Path('data/bootstrap/block_trades.20260321.json')
# Final fallback for unreg `shares` when neither SEC
# filings nor the legacy bootstrap has a count.
MANUAL_SHARES = Path('data/bootstrap/manual_shares.csv')

# Legacy uses shorthand bank codes; map to our canonical
# keys (compare_old_blocks.py shares this convention).
_LEGACY_BANK_MAP = {
    'BAML': 'BAC', 'Citi': 'C', 'Jefferies': 'JEF',
    'BCS': 'BCS', 'GS': 'GS', 'JPM': 'JPM',
    'MS': 'MS', 'RBC': 'RBC', 'WFC': 'WFC',
    'BMO': 'BMO', 'UBS': 'UBS', 'Cantor': 'Cantor',
    'CanGen': 'CF',  # Canaccord Genuity
}
# Manual gross-price overrides for bought-deal block
# reg filings (the filing only states the net to seller;
# the public reoffer price is outside the document).
BLOCK_OFFER_PX = Path('data/bootstrap/block_deals_for_offerpx.csv')


def _load_block_offer_px() -> dict[tuple[str, str], float]:
    """(Ticker, PriceDt) -> gross OfferPx override."""
    import csv
    out: dict[tuple[str, str], float] = {}
    if not BLOCK_OFFER_PX.exists():
        return out
    with BLOCK_OFFER_PX.open() as fh:
        for r in csv.DictReader(fh):
            px = r.get('OfferPx')
            if not px:
                continue
            try:
                out[(r['Ticker'], r['PriceDt'])] = float(px)
            except ValueError:
                continue
    return out


_BLOCK_OVERRIDES = _load_block_offer_px()


def _load_legacy_shares() -> dict[tuple[str, str], int]:
    """(cik_unpadded, PxDt) -> Shares from legacy json."""
    if not LEGACY_BLOCKS.exists():
        return {}
    legacy = json.loads(LEGACY_BLOCKS.read_text())
    out: dict[tuple[str, str], int] = {}
    for r in legacy:
        cik = resolve_cik(r.get('Ticker', '').upper())
        if not cik:
            continue
        sh = r.get('Shares')
        if not sh:
            continue
        out[(cik, r['PxDt'])] = int(sh)
    return out


_LEGACY_SHARES = _load_legacy_shares()


def _load_manual_shares() -> dict[tuple[str, str], int]:
    """(Ticker, PriceDt) -> manual Shares override."""
    import csv
    out: dict[tuple[str, str], int] = {}
    if not MANUAL_SHARES.exists():
        return out
    with MANUAL_SHARES.open() as fh:
        for r in csv.DictReader(fh):
            sh = r.get('shares', '').strip()
            if not sh:
                continue
            try:
                out[(r['Ticker'], r['PriceDt'])] = int(sh)
            except ValueError:
                continue
    return out


_MANUAL_SHARES = _load_manual_shares()


def _parse_golden_date(s: str) -> date | None:
    try:
        return datetime.strptime(s, '%d-%b-%Y').date()
    except (ValueError, TypeError):
        return None


def _parse_iso(s: str) -> date | None:
    """Parse an EDGAR index date. Full quarterly indices
    use YYYY-MM-DD; daily indices use YYYYMMDD (no dashes)."""
    if not s:
        return None
    try:
        if len(s) >= 10 and s[4] == '-':
            return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        if len(s) == 8 and s.isdigit():
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, IndexError, TypeError):
        return None
    return None


# ----- Reg seeding -----


def _split_cols(symbol: str, pdt, shares: int, px: float) -> dict:
    """Compute split-adjusted shares + price."""
    f = cumulative_factor(symbol, pdt)
    return {
        'split_factor': float(f),
        'adj_shares': int(round(shares * f)),
        'adj_price': float(px / f),
    }


def _row_from_reg_deal(deal, golden) -> dict | None:
    if (
        deal is None
        or not deal.offer_price
        or not deal.shares_offered
    ):
        return None
    pdt = _parse_golden_date(golden['PriceDt'])
    if pdt is None:
        return None

    sym = deal.symbol or golden['Ticker']
    # For bought-deal blocks the filing only states the
    # net price (what the underwriter paid the seller).
    # The public reoffer / gross OfferPx is captured
    # manually in data/bootstrap/block_deals_for_offerpx.csv
    # and overrides the parsed value here.
    override = _BLOCK_OVERRIDES.get(
        (golden['Ticker'], golden['PriceDt'])
    )
    if override:
        offer_price = float(override)
        evidence = 'golden+parser+override'
    else:
        offer_price = float(deal.offer_price)
        evidence = 'golden+parser'

    notional = deal.shares_offered * offer_price
    banks = (
        parse_banks(deal.underwriter)
        if deal.underwriter else []
    )
    if isinstance(banks, tuple):
        banks = list(banks)
    seller_rel = (
        'selling stockholder'
        if deal.has_selling_stockholder
        else 'company'
    )
    return {
        'price_date': pdt,
        'symbol': sym,
        'offer_price': offer_price,
        'type': 'Reg',
        'trade_date': pdt,
        'intraday': False,
        'shares': int(deal.shares_offered),
        'notional': float(notional),
        **_split_cols(
            sym, pdt,
            deal.shares_offered,
            offer_price,
        ),
        'seller': deal.issuer_name or '',
        'relationship': seller_rel,
        'banks': banks,
        'cik': deal.cik,
        'evidence': evidence,
        'source': REG_GOLDEN.name,
    }


def seed_reg() -> list[dict]:
    """Re-build the labeled corpus clusters and produce
    one row per resolved RegDeal."""
    labels = pl.read_parquet(REG_LABELS)
    corpus = pl.read_parquet(REG_CORPUS)
    cik_for = {
        r['filename']: r['cik']
        for r in corpus.to_dicts()
    }
    golden = json.loads(REG_GOLDEN.read_text())

    by_idx = defaultdict(list)
    for r in labels.to_dicts():
        by_idx[r['golden_idx']].append(r)

    out = []
    for gi, rows in by_idx.items():
        g = golden[gi]
        symbol = (g.get('Ticker') or '').upper()
        members = []
        for r in rows:
            raw = get_cached(r['candidate_filename'])
            if raw is None:
                continue
            members.append(parse_member(
                filename=r['candidate_filename'],
                filing_date=datetime.fromisoformat(
                    r['candidate_date']
                ).date(),
                form_type=r['form_type'],
                cik=cik_for.get(
                    r['candidate_filename'], ''
                ),
                raw=raw,
            ))
        deal = resolve_deal(members, symbol)
        row = _row_from_reg_deal(deal, g)
        if row:
            out.append(row)
    return out


# ----- Unreg seeding -----


def _scan_unreg_candidates(
    cik: str, lo: date, hi: date,
) -> list[tuple[date, str, str]]:
    out: list[tuple[date, str, str]] = []
    seen: set[str] = set()
    for f in sorted(
        list(INDEX_DIR.glob('full.*.idx'))
        + list(INDEX_DIR.glob('daily.*.idx'))
    ):
        for line in f.read_text().splitlines():
            parts = line.split('|')
            if len(parts) < 5:
                continue
            form = parts[2]
            if not (
                form.startswith('144')
                or form in ('4', '4/A')
            ):
                continue
            c = parts[0].lstrip('0') or '0'
            if c != cik:
                continue
            d = _parse_iso(parts[3])
            if d is None or d < lo or d > hi:
                continue
            fn = parts[4]
            if fn in seen:
                continue
            seen.add(fn)
            out.append((d, form, fn))
    return out


def _row_from_unreg(deal, golden) -> dict | None:
    pdt = _parse_golden_date(golden['PriceDt'])
    tdt = _parse_golden_date(golden['TradeDt'])
    px = float(golden.get('OfferPx') or 0.0)
    if pdt is None or px <= 0:
        return None

    # Size resolution order:
    #   1. Form 4 sum (block_shares from resolver)
    #   2. 144 sum (block_shares falls back to it)
    #   3. legacy bootstrap by (CIK, PriceDt)
    #   4. manual override by (Ticker, PriceDt)
    shares = deal.block_shares if deal else 0
    if shares == 0:
        cik = (
            deal.cik if deal
            else (resolve_cik(golden['Ticker']) or '')
        )
        if cik:
            legacy_sh = _LEGACY_SHARES.get(
                (cik, golden['PriceDt'])
            )
            if legacy_sh:
                shares = legacy_sh
    if shares == 0:
        manual_sh = _MANUAL_SHARES.get(
            (golden['Ticker'], golden['PriceDt'])
        )
        if manual_sh:
            shares = manual_sh
    notional = shares * px if shares else 0.0

    # Seller: top entity by aggregated size; join the
    # next few with semicolons for cluster sellers.
    seller_name = ''
    if deal and deal.sellers:
        seller_name = '; '.join(deal.sellers[:5])
    # Relationship: 144-derived sellers are typically
    # "affiliate" (control/restricted), Form 4-only is
    # "insider". Both can coexist.
    relationship = ''
    if deal:
        if deal.n_144 > 0:
            relationship = 'affiliate'
        elif deal.n_form4 > 0:
            relationship = 'insider'

    return {
        'price_date': pdt,
        'symbol': golden['Ticker'],
        'offer_price': px,
        'type': 'Unreg',
        'trade_date': tdt or pdt,
        'intraday': bool(golden.get('Intraday')),
        'shares': int(shares),
        'notional': float(notional),
        **_split_cols(
            golden['Ticker'], pdt, shares, px,
        ),
        'seller': seller_name,
        'relationship': relationship,
        'banks': [],
        'cik': (deal.cik if deal else '')
        or (resolve_cik(golden['Ticker']) or ''),
        'evidence': deal.evidence if deal else 'golden',
        'source': UNREG_GOLDEN.name,
    }


def seed_unreg() -> list[dict]:
    golden = json.loads(UNREG_GOLDEN.read_text())
    out = []
    for g in golden:
        sym = g['Ticker']
        cik = resolve_cik(sym)
        pdt = _parse_golden_date(g['PriceDt'])
        tdt = _parse_golden_date(g['TradeDt'])
        if cik is None or pdt is None:
            # Still seed (golden is truth) with no
            # filing-derived size — shares=0 is fine.
            out.append(_row_from_unreg(None, g))
            continue
        lo = pdt - timedelta(days=2)
        hi = (tdt or pdt) + timedelta(days=5)
        cands = _scan_unreg_candidates(cik, lo, hi)

        f4s, f144s = [], []
        for _, form, fn in cands:
            raw = get_cached(fn)
            if raw is None:
                continue
            if form in ('4', '4/A'):
                f4s.extend(parse_form4(raw))
            elif form.startswith('144'):
                p = parse_144(raw)
                if p:
                    f144s.append(p)
        deal = resolve_unreg_deal(
            cik=cik, symbol=sym,
            price_date=pdt, trade_date=tdt,
            intraday=bool(g.get('Intraday')),
            form4_txns=f4s, f144_filings=f144s,
        )
        row = _row_from_unreg(deal, g)
        if row:
            out.append(row)
    return [r for r in out if r is not None]


# ----- Legacy direct-seed (no SEC filings) -----


def seed_legacy() -> list[dict]:
    """Seed blocks straight from legacy json for deals
    with no SEC filing (foreign issuers etc.). All fields
    come from the legacy record — no parsing involved."""
    import csv as _csv
    if not LEGACY_SEED.exists():
        return []
    out = []
    with LEGACY_SEED.open() as fh:
        for r in _csv.DictReader(fh):
            sym = r['Ticker']
            pdt = _parse_golden_date(r['PxDt'])
            tdt = _parse_golden_date(r['TradeDt']) or pdt
            if pdt is None:
                continue
            try:
                px = float(r['OfferPx'])
                sh = int(r['Shares'])
            except (ValueError, TypeError):
                continue
            cik = resolve_cik(sym) or ''
            lb = (r.get('LeftBank') or '').strip()
            banks = (
                [_LEGACY_BANK_MAP.get(lb, lb)]
                if lb else []
            )
            out.append({
                'price_date': pdt,
                'symbol': sym,
                'offer_price': px,
                'type': r.get('Type') or 'Unreg',
                'trade_date': tdt,
                'intraday': tdt == pdt,
                'shares': sh,
                'notional': sh * px,
                **_split_cols(sym, pdt, sh, px),
                'seller': '',
                'relationship': '',
                'banks': banks,
                'cik': cik,
                'evidence': 'legacy_bootstrap',
                'source': 'block_trades.20260321.json',
            })
    return out


# ----- Driver -----


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reg', action='store_true')
    ap.add_argument('--unreg', action='store_true')
    ap.add_argument('--legacy', action='store_true')
    args = ap.parse_args()

    any_flag = args.reg or args.unreg or args.legacy
    do_reg = args.reg or not any_flag
    do_unreg = args.unreg or not any_flag
    do_legacy = args.legacy or not any_flag

    rows: list[dict] = []
    if do_reg:
        print('seeding reg...')
        r = seed_reg()
        print(f'  {len(r)} reg rows')
        rows.extend(r)

    if do_unreg:
        print('seeding unreg...')
        u = seed_unreg()
        print(f'  {len(u)} unreg rows')
        rows.extend(u)

    if do_legacy:
        print('seeding legacy (no-filing blocks)...')
        L = seed_legacy()
        print(f'  {len(L)} legacy rows')
        rows.extend(L)

    if not rows:
        return

    upsert_trades(rows)

    # Promote to blocks with status='confirmed'
    now = datetime.now().isoformat(timespec='seconds')
    blocks_rows = [
        {**r, 'status': 'confirmed', 'reviewed_at': now}
        for r in rows
    ]
    upsert_blocks(blocks_rows)


if __name__ == '__main__':
    main()
