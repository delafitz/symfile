"""Compare our blocks.parquet vs the legacy
bootstrap file block_trades.20260321.json.

The legacy file has 491 rows from Apr 2024 – Sep 2025
with PxDt, Ticker, OfferPx, Shares, LeftBank. Our new
table uses (price_date, symbol, offer_price) as the
primary key and carries the same data through fresh
seed + parser pipelines.

Report:
  - rows in legacy not in ours and vice-versa
  - within matched (symbol+price_date), how often the
    OfferPx, Shares, and LeftBank agree
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl  # noqa: E402

from app.trades.blocks import load_blocks  # noqa: E402

LEGACY = Path('data/bootstrap/block_trades.20260321.json')

# Bank-name normalisation between the two systems.
# Legacy uses short hand ("BAML", "MS", "BAML", "JPM",
# "RBC", "Citi", ...). Ours uses parse_banks canonical
# keys ("BAC", "MS", "JPM", "RBC", "C", ...).
BANK_ALIASES = {
    'BAML': 'BAC',
    'BofA': 'BAC',
    'BofA Securities': 'BAC',
    'JPM': 'JPM',
    'JPMorgan': 'JPM',
    'J.P. Morgan': 'JPM',
    'MS': 'MS',
    'Morgan Stanley': 'MS',
    'GS': 'GS',
    'Goldman': 'GS',
    'Goldman Sachs': 'GS',
    'Citi': 'C',
    'Citigroup': 'C',
    'RBC': 'RBC',
    'Barclays': 'BCS',
    'WFC': 'WFC',
    'Wells Fargo': 'WFC',
    'Wells': 'WFC',
    'UBS': 'UBS',
    'DB': 'DB',
    'Deutsche Bank': 'DB',
    'Jefferies': 'JEF',
    'Cantor': 'Cantor',
    'TD': 'TD',
    'TD Securities': 'TD',
    'Stifel': 'Stifel',
    'BMO': 'BMO',
    'Piper': 'Piper',
    'Piper Sandler': 'Piper',
    'Truist': 'Truist',
    'Raymond James': 'RJ',
}


def _norm_bank(s: str) -> str:
    if not s:
        return ''
    s = s.strip()
    return BANK_ALIASES.get(s, s)


def _parse_date(s: str):
    return datetime.strptime(s, '%d-%b-%Y').date()


def main() -> None:
    legacy = json.loads(LEGACY.read_text())
    print(f'legacy rows: {len(legacy)}')

    legacy_by_key: dict[tuple, dict] = {}
    for r in legacy:
        d = _parse_date(r['PxDt'])
        sym = r['Ticker'].upper()
        legacy_by_key[(d, sym)] = r

    blocks = load_blocks()
    print(f'our blocks: {blocks.height}')

    ours_by_key: dict[tuple, dict] = {}
    for r in blocks.to_dicts():
        ours_by_key[(r['price_date'], r['symbol'])] = r

    legacy_keys = set(legacy_by_key)
    ours_keys = set(ours_by_key)
    both = legacy_keys & ours_keys

    print(f'\n=== membership ===')
    print(f'  both:           {len(both)}')
    print(f'  legacy only:    {len(legacy_keys - ours_keys)}')
    print(f'  ours only:      {len(ours_keys - legacy_keys)}')

    # restrict comparison to legacy's window for ours-only
    leg_min = min(k[0] for k in legacy_keys)
    leg_max = max(k[0] for k in legacy_keys)
    ours_in_window = {
        k for k in ours_keys
        if leg_min <= k[0] <= leg_max
    }
    print(f'\n=== ours window {leg_min} - {leg_max} ===')
    print(f'  ours in window:   {len(ours_in_window)}')
    print(f'  ours-in-window only: '
          f'{len(ours_in_window - legacy_keys)}')

    # --- field-level agreement on matched rows ---
    # Compare against split-adjusted values too (legacy
    # appears to store post-split numbers).
    c = Counter()
    px_diffs = []
    sh_diffs = []
    bank_mismatches = []
    for k in sorted(both):
        l = legacy_by_key[k]
        o = ours_by_key[k]
        c['matched'] += 1
        # OfferPx — prefer split-adjusted to match legacy
        lpx = l.get('OfferPx')
        opx = o.get('adj_price') or o.get('offer_price')
        if lpx is not None and opx is not None and opx > 0:
            d = abs(opx - lpx) / lpx
            px_diffs.append((d, k, lpx, opx))
            if d < 0.001:
                c['px_match_<0.1%'] += 1
            elif d < 0.01:
                c['px_match_<1%'] += 1
            elif d < 0.05:
                c['px_match_<5%'] += 1
            else:
                c['px_mismatch_>=5%'] += 1
        else:
            c['px_missing'] += 1
        # Shares — adjusted to match legacy split basis
        lsh = l.get('Shares')
        osh = o.get('adj_shares') or o.get('shares')
        if lsh and osh and osh > 0:
            d = abs(osh - lsh) / lsh
            sh_diffs.append((d, k, lsh, osh))
            if d < 0.001:
                c['sh_match_<0.1%'] += 1
            elif d < 0.05:
                c['sh_match_<5%'] += 1
            else:
                c['sh_mismatch_>=5%'] += 1
        else:
            c['sh_missing'] += 1
        # Banks (legacy LeftBank vs ours banks list)
        lbank = _norm_bank(l.get('LeftBank') or '')
        obanks = {_norm_bank(b) for b in (o.get('banks') or [])}
        if not lbank:
            c['lbank_legacy_blank'] += 1
        elif lbank in obanks:
            c['lbank_match'] += 1
        else:
            c['lbank_mismatch'] += 1
            bank_mismatches.append(
                (k, l.get('LeftBank'), o.get('banks'))
            )

    print(f'\n=== field-level agreement (on {c["matched"]} matched) ===')
    print('  offer_price:')
    for k_ in ('px_match_<0.1%', 'px_match_<1%',
              'px_match_<5%', 'px_mismatch_>=5%',
              'px_missing'):
        print(f'    {k_:24s}  {c[k_]}')
    print('  shares:')
    for k_ in ('sh_match_<0.1%', 'sh_match_<5%',
              'sh_mismatch_>=5%', 'sh_missing'):
        print(f'    {k_:24s}  {c[k_]}')
    print('  left bank:')
    for k_ in ('lbank_match', 'lbank_mismatch',
              'lbank_legacy_blank'):
        print(f'    {k_:24s}  {c[k_]}')

    print('\n--- top 10 offer_price mismatches ---')
    px_diffs.sort(reverse=True)
    for d, k, lpx, opx in px_diffs[:10]:
        print(f'  {k[1]:6s}  {k[0]}  legacy=${lpx:7.2f}  '
              f'ours=${opx:7.2f}  delta={d * 100:+5.2f}%')

    print('\n--- top 10 shares mismatches ---')
    sh_diffs.sort(reverse=True)
    for d, k, lsh, osh in sh_diffs[:10]:
        print(f'  {k[1]:6s}  {k[0]}  '
              f'legacy={lsh:>12,}  ours={osh:>12,}  '
              f'delta={d * 100:+5.2f}%')

    print('\n--- legacy rows not in ours (sample) ---')
    for k in sorted(legacy_keys - ours_keys)[:20]:
        r = legacy_by_key[k]
        print(f'  {k[1]:6s}  {k[0]}  '
              f'type={r.get("Type") or "Unreg":5s}  '
              f'px=${r.get("OfferPx", 0):.2f}  '
              f'sh={r.get("Shares", 0):>10,}  '
              f'bank={r.get("LeftBank")}')

    print('\n--- ours-only in legacy window (sample) ---')
    for k in sorted(ours_in_window - legacy_keys)[:20]:
        r = ours_by_key[k]
        print(f'  {k[1]:6s}  {k[0]}  '
              f'type={r["type"]:5s}  '
              f'px=${r["offer_price"]:.2f}  '
              f'sh={r["shares"]:>10,}  '
              f'banks={r["banks"]}')


if __name__ == '__main__':
    main()
