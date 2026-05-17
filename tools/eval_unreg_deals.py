"""End-to-end evaluation of unreg block detection.

For each covered golden row (a row whose CIK resolves
and has 144/Form 4 filings in window):

  1. Walk indices for 144 + Form 4 candidates in
     [PriceDt-2, TradeDt+5]
  2. Fetch any that aren't on disk (SEC_RPS=4)
  3. Parse 144s (parse_144) + Form 4s (parse_form4)
  4. Resolve into one UnregDeal via app.parsers.unreg
  5. Compare UnregDeal.txn_price_wavg to golden OfferPx

Reports:
  - resolve rate (got at least one Form 4 'S' txn)
  - price delta histogram (within 0.5%, 1%, 5%, worse)
  - size summary

    uv run python tools/eval_unreg_deals.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.edgar import fetch as fetch_mod  # noqa: E402
from app.edgar.fetch import (  # noqa: E402
    FILING_BASE,
    fetch_many_async,
    get_cached,
)
from app.edgar.parse.form144 import parse_144  # noqa: E402
from app.edgar.parse.form4 import parse_form4  # noqa: E402
from app.mds.syms import resolve_cik  # noqa: E402
from app.parsers.unreg import (  # noqa: E402
    resolve_unreg_deal,
)
from app.util.asyncio import run_coro  # noqa: E402

GOLDEN = Path('data/bootstrap/unreg_golden.20260517.json')
INDEX_DIR = Path('data/indices')

# slower fetch — SEC throttles aggressively
fetch_mod.SEC_RPS = 4


def _parse_iso(s: str) -> date | None:
    try:
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    except (ValueError, IndexError):
        return None


def _parse_golden_date(s: str) -> date | None:
    try:
        return datetime.strptime(s, '%d-%b-%Y').date()
    except ValueError:
        return None


def _scan_candidates(
    cik: str, lo: date, hi: date,
) -> list[tuple[date, str, str]]:
    """Walk every index, return [(date, form, filename)]
    for 144/Form 4 filings under `cik` in [lo, hi]."""
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


def _pct(a: float, b: float) -> float:
    """Signed pct delta a vs b, anchored on b."""
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def main() -> None:
    golden = json.loads(GOLDEN.read_text())
    print(f'loaded {len(golden)} golden rows')

    # Phase 1: collect candidate filenames per row
    print('scanning indices for candidates...')
    rows: list[dict] = []
    all_fetch: list[str] = []
    seen: set[str] = set()
    for g in golden:
        sym = g['Ticker']
        cik = resolve_cik(sym)
        if cik is None:
            continue
        pd = _parse_golden_date(g['PriceDt'])
        td = _parse_golden_date(g['TradeDt'])
        if not pd or not td:
            continue
        lo = pd - timedelta(days=2)
        hi = td + timedelta(days=5)
        cands = _scan_candidates(cik, lo, hi)
        if not cands:
            continue
        rows.append({
            'g': g, 'cik': cik, 'sym': sym,
            'price_dt': pd, 'trade_dt': td,
            'candidates': cands,
        })
        for _, _, fn in cands:
            if fn not in seen and get_cached(fn) is None:
                seen.add(fn)
                all_fetch.append(fn)

    print(f'  {len(rows)} rows with candidates, '
          f'{len(all_fetch)} uncached filings to fetch')

    # Phase 2: fetch everything once
    if all_fetch:
        run_coro(fetch_many_async(
            all_fetch,
            lambda x: x,
            lambda x: FILING_BASE + x,
            lambda x, y: None,
        ))

    # Phase 3: parse + resolve + compare
    print('\nresolving deals...')
    bucket = Counter()
    diffs = []  # (pct, abs, golden_px, our_px, sym, trade_dt)
    sizes = []
    for r in rows:
        f4s = []
        f144s = []
        for d, form, fn in r['candidates']:
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
            cik=r['cik'],
            symbol=r['sym'],
            price_date=r['price_dt'],
            trade_date=r['trade_dt'],
            intraday=r['g']['Intraday'],
            form4_txns=f4s,
            f144_filings=f144s,
        )

        bucket['rows'] += 1
        offer_px = r['g']['OfferPx']

        ev = deal.evidence
        bucket[f'ev_{ev}'] += 1
        if ev == 'none':
            continue
        bucket['resolved'] += 1
        sizes.append(deal.block_shares)

        # Price delta: use Form 4 weighted avg when
        # available, else 144 implicit price.
        ours = deal.block_price
        if ours <= 0:
            bucket['resolved_no_price'] += 1
            continue
        pct = _pct(ours, offer_px)
        abs_pct = abs(pct)
        diffs.append((
            pct, abs_pct, offer_px, ours,
            r['sym'], r['trade_dt'].isoformat(),
            ev,
        ))

        if abs_pct < 0.5:
            bucket['px_within_0.5pct'] += 1
        elif abs_pct < 1.0:
            bucket['px_within_1pct'] += 1
        elif abs_pct < 5.0:
            bucket['px_within_5pct'] += 1
        else:
            bucket['px_off_5pct+'] += 1

    n = bucket['rows']
    res = bucket['resolved']
    print(f'\n=== resolution ===')
    print(f'  rows with candidates: {n}')
    print(f'  resolved (size from any source): {res}'
          f' ({res / n * 100:.1f}%)')
    print(f'    evidence: Form 4 + 144  {bucket["ev_both"]}')
    print(f'    evidence: Form 4 only   {bucket["ev_form4"]}')
    print(f'    evidence: 144 only      {bucket["ev_144"]}')
    print(f'    evidence: none          {bucket["ev_none"]}')
    if bucket['resolved_no_price']:
        print(f'  resolved w/o price:     '
              f'{bucket["resolved_no_price"]}')

    # Price validation by evidence source
    print(f'\n=== price delta vs OfferPx ===')
    by_ev = Counter()
    for pct, abs_pct, _, _, _, _, ev in diffs:
        by_ev[(ev, 'n')] += 1
        if abs_pct < 0.5: by_ev[(ev, '<0.5%')] += 1
        elif abs_pct < 1.0: by_ev[(ev, '<1%')] += 1
        elif abs_pct < 5.0: by_ev[(ev, '<5%')] += 1
        else: by_ev[(ev, '>5%')] += 1

    print(f'  {"source":12s}  {"n":>4s}  {"<0.5%":>6s}'
          f'  {"<1%":>6s}  {"<5%":>6s}  {"≥5%":>6s}')
    for ev in ('both', 'form4', '144'):
        n_ev = by_ev[(ev, 'n')]
        if not n_ev:
            continue
        print(
            f'  {ev:12s}  {n_ev:>4d}  '
            f'{by_ev[(ev, "<0.5%")]:>6d}  '
            f'{by_ev[(ev, "<1%")]:>6d}  '
            f'{by_ev[(ev, "<5%")]:>6d}  '
            f'{by_ev[(ev, ">5%")]:>6d}'
        )

    if sizes:
        sizes.sort()
        med = sizes[len(sizes) // 2]
        print(f'\n=== size summary ===')
        print(f'  median: {med:,}  min: {min(sizes):,}  '
              f'max: {max(sizes):,}')

    # Worst outliers — by abs_pct
    diffs.sort(key=lambda x: -x[1])
    print(f'\n--- top 10 price-delta outliers ---')
    for pct, _, gpx, opx, sym, td, ev in diffs[:10]:
        print(f'  {sym:6s}  {td}  ev={ev:6s}  '
              f'golden=${gpx:7.2f}  ours=${opx:7.2f}  '
              f'delta={pct:+7.2f}%')


if __name__ == '__main__':
    main()
