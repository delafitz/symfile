"""Flatten data/bootstrap/unreg.csv into a JSON
golden list for unregistered block trades.

The CSV has four side-by-side columns per year
(2026 / 2025 / 2024):
  date, symbol, intraday-flag, offer-price.

Intraday='y' means the announcement was during market
hours and the trade priced/executed the same day;
blank means announced after-close, executed next
weekday. OfferPx is the block clear price.

Output: data/bootstrap/unreg_golden.YYYYMMDD.json
  [
    {
      "Ticker": "KNTK",
      "Type": "Unreg",
      "PriceDt": "30-Apr-2026",
      "TradeDt": "1-May-2026",
      "Intraday": false,
      "OfferPx": 49.8
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
            def cell(off: int) -> str:
                i = base + off
                return line[i].strip() if i < len(line) else ''
            sym = cell(1)
            if not sym:
                continue
            d = _parse_md(cell(0), year)
            if d is None:
                continue
            intra = cell(2).lower() == 'y'
            px_raw = cell(3)
            try:
                offer_px = float(px_raw) if px_raw else 0.0
            except ValueError:
                offer_px = 0.0
            trade = d if intra else _next_weekday(d)
            out.append({
                'Ticker': sym,
                'Type': 'Unreg',
                'PriceDt': d.strftime('%-d-%b-%Y'),
                'TradeDt': trade.strftime('%-d-%b-%Y'),
                'Intraday': intra,
                'OfferPx': offer_px,
            })

    out.sort(key=lambda r: (r['Ticker'], r['PriceDt']))

    stamp = TODAY.strftime('%Y%m%d')
    out_path = OUT_DIR / f'unreg_golden.{stamp}.json'
    out_path.write_text(json.dumps(out, indent=2))
    print(f'{len(out)} rows -> {out_path}')

    n_intra = sum(1 for r in out if r['Intraday'])
    n_px = sum(1 for r in out if r['OfferPx'] > 0)
    print(f'  intraday: {n_intra}  next-day: {len(out) - n_intra}')
    print(f'  with OfferPx: {n_px}/{len(out)}')


if __name__ == '__main__':
    main()
