"""Flatten data/bootstrap/unreg.csv into a JSON
golden list for unregistered block trades.

The CSV is three side-by-side (date, symbol, intraday)
triples — one per year column (2026 / 2025 / 2024).
Each row of a triple is one block. Intraday='y' means
the announcement was during market hours and the trade
priced/executed the same day; blank means announced
after-close, executed the next weekday.

Output: data/bootstrap/unreg_golden.YYYYMMDD.json
  [
    {
      "Ticker": "...",
      "Type": "Unreg",
      "PriceDt": "30-Apr-2026",
      "TradeDt": "1-May-2026",
      "Intraday": false
    },
    ...
  ]
"""

from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

CSV_PATH = Path('data/bootstrap/unreg.csv')
OUT_DIR = Path('data/bootstrap')
TODAY = date.today()


def _next_weekday(d: date) -> date:
    d = d + timedelta(days=1)
    while d.weekday() >= 5:
        d = d + timedelta(days=1)
    return d


def _parse_md(md: str, year: int) -> date | None:
    """'4/30' + 2026 -> date(2026,4,30)."""
    md = md.strip()
    if not md or '/' not in md:
        return None
    m, day = md.split('/')
    try:
        return date(year, int(m), int(day))
    except ValueError:
        return None


def main() -> None:
    rows = list(csv.reader(CSV_PATH.open()))
    header = rows[0]

    # Build (year, col_offset) triples by scanning header
    triples: list[tuple[int, int]] = []
    for i, h in enumerate(header):
        h = h.strip()
        if h.isdigit():
            triples.append((int(h), i))

    out = []
    for line in rows[1:]:
        for year, base in triples:
            md = line[base] if base < len(line) else ''
            sym = line[base + 1] if base + 1 < len(line) else ''
            intra = (
                line[base + 2].strip().lower() == 'y'
                if base + 2 < len(line) else False
            )
            sym = sym.strip()
            if not sym:
                continue
            d = _parse_md(md, year)
            if d is None:
                continue
            trade = d if intra else _next_weekday(d)
            out.append({
                'Ticker': sym,
                'Type': 'Unreg',
                'PriceDt': d.strftime('%-d-%b-%Y'),
                'TradeDt': trade.strftime('%-d-%b-%Y'),
                'Intraday': intra,
            })

    out.sort(key=lambda r: (r['Ticker'], r['PriceDt']))

    stamp = TODAY.strftime('%Y%m%d')
    out_path = OUT_DIR / f'unreg_golden.{stamp}.json'
    out_path.write_text(json.dumps(out, indent=2))
    print(f'{len(out)} rows -> {out_path}')

    n_intra = sum(1 for r in out if r['Intraday'])
    print(f'  intraday: {n_intra}  next-day: {len(out) - n_intra}')


if __name__ == '__main__':
    main()
