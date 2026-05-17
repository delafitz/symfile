"""Coverage check: unreg golden -> filings on disk.

For each row in unreg_golden, look for at least one
candidate filing (144 or Form 4) within ±5 days of
TradeDt for the same issuer.

Both 144 and Form 4 filings get an issuer-CIK row in
the full quarterly index, so we walk those:
    data/indices/full.YYYYQX.idx

(form4.parquet covers only the post-2026 sync window —
the golden list spans 2024-2026 so we'd miss almost
everything if we joined against that.)

    uv run python tools/eval_unreg_coverage.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.mds.syms import load_syms  # noqa: E402

GOLDEN = Path('data/bootstrap/unreg_golden.20260517.json')
INDEX_DIR = Path('data/indices')

WINDOW_DAYS = 5


def _parse_golden_date(s: str) -> date | None:
    try:
        return datetime.strptime(s, '%d-%b-%Y').date()
    except ValueError:
        return None


def _parse_iso(s: str) -> date | None:
    try:
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    except (ValueError, IndexError):
        return None


def _load_index_by_cik() -> dict[str, list[tuple[date, str, str]]]:
    """Walk all full + daily indices, group 144/4 lines
    by issuer CIK. Returns {cik_unpadded: [(date, form, filename), ...]}."""
    by_cik: dict[str, list] = defaultdict(list)
    sources = sorted(INDEX_DIR.glob('full.*.idx')) + sorted(
        INDEX_DIR.glob('daily.*.idx')
    )
    n_lines = 0
    for f in sources:
        for line in f.read_text().splitlines():
            parts = line.split('|')
            if len(parts) < 5:
                continue
            form = parts[2]
            if not (form.startswith('144') or form == '4' or form == '4/A'):
                continue
            d = _parse_iso(parts[3])
            if d is None:
                continue
            cik = parts[0].lstrip('0') or '0'
            by_cik[cik].append((d, form, parts[4]))
            n_lines += 1
    print(f'  indexed {n_lines} 144/4 lines across {len(by_cik)} CIKs')
    return by_cik


def main() -> None:
    golden = json.loads(GOLDEN.read_text())
    print(f'loaded {len(golden)} golden rows')

    syms = load_syms()
    sym_to_cik = {
        s: r.cik.lstrip('0') or '0'
        for s, r in syms.items()
    }

    print('indexing 144/4 by issuer CIK...')
    by_cik = _load_index_by_cik()

    c = Counter()
    not_in_syms = []
    no_filings = []

    for g in golden:
        sym = g['Ticker']
        trade = _parse_golden_date(g['TradeDt'])
        price = _parse_golden_date(g['PriceDt'])
        if trade is None or price is None:
            continue

        # Window: from PriceDt-2 (early notice) to
        # TradeDt+WINDOW_DAYS (form 4 lag).
        lo = price - timedelta(days=2)
        hi = trade + timedelta(days=WINDOW_DAYS)

        if sym not in sym_to_cik:
            not_in_syms.append((sym, g['PriceDt']))
            continue

        cik = sym_to_cik[sym]
        hits = [
            (d, form, fn)
            for d, form, fn in by_cik.get(cik, [])
            if lo <= d <= hi
        ]
        has_144 = any(f.startswith('144') for _, f, _ in hits)
        has_f4 = any(f in ('4', '4/A') for _, f, _ in hits)

        if has_144 and has_f4:
            c['both'] += 1
        elif has_f4:
            c['form4_only'] += 1
        elif has_144:
            c['144_only'] += 1
        else:
            c['miss'] += 1
            no_filings.append({
                'sym': sym,
                'price_dt': g['PriceDt'],
                'trade_dt': g['TradeDt'],
                'intraday': g['Intraday'],
                'offer_px': g['OfferPx'],
            })

    n = len(golden)
    covered = c['both'] + c['form4_only'] + c['144_only']
    print(f'\n=== coverage ===')
    print(f'  golden rows:     {n}')
    print(f'  in sym universe: {n - len(not_in_syms)}')
    print(f'  covered:         {covered}'
          f' ({covered / n * 100:.1f}%)')
    print(f'    both 144+F4:   {c["both"]}')
    print(f'    F4 only:       {c["form4_only"]}')
    print(f'    144 only:      {c["144_only"]}')
    print(f'  not in syms:     {len(not_in_syms)}')
    print(f'  missed:          {len(no_filings)}')

    if not_in_syms:
        print('\n--- not in syms universe ---')
        for s, d in not_in_syms[:30]:
            print(f'  {s:8s}  {d}')

    if no_filings:
        print('\n--- in syms but no filings in window ---')
        for r in no_filings[:30]:
            print(f'  {r["sym"]:8s}  price={r["price_dt"]:11s}  '
                  f'trade={r["trade_dt"]:11s}  '
                  f'intraday={r["intraday"]}  px={r["offer_px"]}')


if __name__ == '__main__':
    main()
