"""Stock-split metadata from Polygon.

Fetches the full list of stock splits and caches them
to data/mds/splits.YYYYMMDD.csv. Used to convert
as-filed historical share counts and prices to today's
split-adjusted basis.

  load_splits(symbol)        — list[(date, factor)]
                               where factor = split_to/split_from
  cumulative_factor(symbol, since)
                             — product of factors with
                               execution_date > since.
                               >1 = forward split,
                               <1 = reverse split,
                               =1 = no change.

A deal of N shares at $P on date D adjusts to:
  shares_adjusted = N * cumulative_factor(sym, D)
  price_adjusted  = P / cumulative_factor(sym, D)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.mds import DATA_DIR
from app.mds.massive.session import get_client
from app.util.log import log

MAX_AGE_DAYS = 7

# Backfill window — we go back far enough to cover any
# golden bootstrap (currently goes back to Jan 2024).
START_DATE = date(2023, 1, 1)


@dataclass
class Split:
    symbol: str
    execution_date: date
    factor: float  # split_to / split_from


def _find_cached() -> tuple[Path, date] | None:
    pattern = re.compile(r'splits\.(\d{8})\.csv$')
    best: tuple[Path, date] | None = None
    if not DATA_DIR.exists():
        return None
    for f in DATA_DIR.iterdir():
        m = pattern.match(f.name)
        if m:
            d = date.fromisoformat(
                f'{m.group(1)[:4]}-{m.group(1)[4:6]}'
                f'-{m.group(1)[6:]}'
            )
            if best is None or d > best[1]:
                best = (f, d)
    return best


def _fetch_splits() -> list[Split]:
    """Pull every stock split from START_DATE forward."""
    client = get_client()
    rows: list[Split] = []
    for s in client.list_splits(
        execution_date_gte=START_DATE.isoformat(),
        limit=1000,
    ):
        try:
            ed = date.fromisoformat(s.execution_date)
            sf = float(s.split_from)
            st = float(s.split_to)
            if sf <= 0 or st <= 0:
                continue
            rows.append(Split(
                symbol=s.ticker,
                execution_date=ed,
                factor=st / sf,
            ))
        except (AttributeError, ValueError):
            continue
    return rows


def _save(rows: list[Split]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'splits.{stamp}.csv'
    df = pl.DataFrame(
        [
            {
                'symbol': r.symbol,
                'execution_date': r.execution_date.isoformat(),
                'factor': r.factor,
            }
            for r in rows
        ],
        schema={
            'symbol': pl.Utf8,
            'execution_date': pl.Utf8,
            'factor': pl.Float64,
        },
    )
    df.write_csv(path)
    log.info('saved splits', count=len(rows), path=str(path))
    return path


_by_symbol: dict[str, list[Split]] | None = None


def _load(max_age_days: int = MAX_AGE_DAYS) -> None:
    """Populate the in-memory cache, fetching if stale."""
    global _by_symbol
    cached = _find_cached()
    cutoff = date.today() - timedelta(days=max_age_days)

    if cached and cached[1] >= cutoff:
        df = pl.read_csv(cached[0])
        rows = [
            Split(
                symbol=r['symbol'],
                execution_date=date.fromisoformat(
                    r['execution_date']
                ),
                factor=r['factor'],
            )
            for r in df.to_dicts()
        ]
        log.debug('cached splits', file=cached[0].name)
    else:
        log.info('fetching splits')
        rows = _fetch_splits()
        _save(rows)

    by_sym: dict[str, list[Split]] = {}
    for s in rows:
        by_sym.setdefault(s.symbol, []).append(s)
    for sym in by_sym:
        by_sym[sym].sort(key=lambda x: x.execution_date)
    _by_symbol = by_sym
    log.info(
        'splits loaded',
        symbols=len(by_sym),
        total=len(rows),
    )


def load_splits(symbol: str) -> list[Split]:
    """Return every split for `symbol` sorted ascending."""
    if _by_symbol is None:
        _load()
    return _by_symbol.get(symbol, [])  # type: ignore[union-attr]


def cumulative_factor(symbol: str, since: date) -> float:
    """Product of split factors with execution_date > since.

    A trade dated `since` should be adjusted by this
    factor: shares *= factor, price /= factor.
    """
    if _by_symbol is None:
        _load()
    factor = 1.0
    for s in _by_symbol.get(symbol, []):  # type: ignore[union-attr]
        if s.execution_date > since:
            factor *= s.factor
    return factor
