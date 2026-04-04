"""Symbol-to-CIK mapping via Polygon tickers endpoint.

Caches to data/mds/tickers.YYYYMMDD.csv. Reuses cache
if less than MAX_AGE_DAYS old, otherwise refetches.
"""

import csv
import re
from datetime import date, timedelta
from pathlib import Path

from symfile.mds import DATA_DIR
from symfile.mds.massive.session import get_client
from symfile.util.log import log

MAX_AGE_DAYS = 30


def _find_cached() -> tuple[Path, date] | None:
    """Find most recent tickers cache file."""
    pattern = re.compile(r'tickers\.(\d{8})\.csv$')
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


def _fetch_tickers() -> list[dict]:
    """Fetch all tickers from Polygon."""
    client = get_client()
    rows = []
    for t in client.list_tickers(
        market='stocks',
        active=True,
        limit=1000,
    ):
        cik = getattr(t, 'cik', None)
        if not cik:
            continue
        rows.append(
            {
                'symbol': t.ticker,
                'name': getattr(t, 'name', ''),
                'cik': str(cik),
                'type': getattr(t, 'type', ''),
            }
        )
    return rows


def _save(rows: list[dict]) -> Path:
    """Save tickers to dated CSV."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'tickers.{stamp}.csv'
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                'symbol', 'name', 'cik', 'type',
            ],
        )
        w.writeheader()
        w.writerows(rows)
    log.info('saved tickers', count=len(rows), path=str(path))
    return path


def load_tickers(
    max_age_days: int = MAX_AGE_DAYS,
) -> dict[str, dict]:
    """Load symbol->{name, cik} mapping.

    Returns cached data if fresh, otherwise refetches.
    """
    cached = _find_cached()
    cutoff = date.today() - timedelta(
        days=max_age_days
    )

    if cached and cached[1] >= cutoff:
        path = cached[0]
        log.debug('cached tickers', file=path.name)
        result = {}
        with open(path) as f:
            for row in csv.DictReader(f):
                result[row['symbol']] = row
        log.info('tickers loaded', count=len(result))
        return result

    log.info('fetching tickers')
    rows = _fetch_tickers()
    _save(rows)
    return {r['symbol']: r for r in rows}


def build_cik_map(
    tickers: dict[str, dict],
    types: set[str] = {'CS'},
) -> dict[str, str]:
    """Build CIK->symbol mapping, filtered to given
    Polygon types (default: CS = common stock only).

    CIK keys are unpadded to match EDGAR index format.
    """
    cik_to_sym: dict[str, str] = {}
    for sym, info in tickers.items():
        if info.get('type', '') not in types:
            continue
        cik = info['cik'].lstrip('0') or '0'
        cik_to_sym[cik] = sym
    return cik_to_sym
