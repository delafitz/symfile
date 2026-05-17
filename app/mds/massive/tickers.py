"""Symbol-to-CIK mapping via Polygon tickers endpoint.

Two flavors, both cached separately:

  tickers.YYYYMMDD.csv          — active symbols (the
                                   trading universe input)
  tickers_inactive.YYYYMMDD.csv — delisted/acquired
                                   symbols (resolution-only)

Reuses cache if less than MAX_AGE_DAYS old, otherwise refetches.
"""

import re
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.mds import DATA_DIR
from app.mds.massive.session import get_client
from app.util.log import log

MAX_AGE_DAYS = 30

_PATTERNS = {
    True: re.compile(r'tickers\.(\d{8})\.csv$'),
    False: re.compile(r'tickers_inactive\.(\d{8})\.csv$'),
}
_FILE_PREFIX = {True: 'tickers', False: 'tickers_inactive'}


def _find_cached(active: bool) -> tuple[Path, date] | None:
    pattern = _PATTERNS[active]
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


def _fetch_tickers(active: bool) -> list[dict]:
    """Fetch tickers from Polygon."""
    client = get_client()
    rows = []
    for t in client.list_tickers(
        market='stocks',
        active=active,
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


def _save(rows: list[dict], active: bool) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'{_FILE_PREFIX[active]}.{stamp}.csv'
    df = pl.DataFrame(
        rows,
        schema=['symbol', 'name', 'cik', 'type'],
    )
    df.write_csv(path)
    log.info(
        'saved tickers',
        active=active,
        count=len(rows),
        path=str(path),
    )
    return path


def load_tickers(
    max_age_days: int = MAX_AGE_DAYS,
    active: bool = True,
) -> dict[str, dict]:
    """Load symbol->{name, cik, type} mapping.

    active=True returns the current trading universe input.
    active=False returns delisted/acquired tickers — used
    only for historical symbol resolution, not for building
    refs.
    """
    cached = _find_cached(active)
    cutoff = date.today() - timedelta(days=max_age_days)

    if cached and cached[1] >= cutoff:
        path = cached[0]
        log.debug(
            'cached tickers', active=active, file=path.name
        )
        df = pl.read_csv(path, infer_schema=False)
        result = {
            row['symbol']: row for row in df.to_dicts()
        }
        log.info(
            'tickers loaded',
            active=active,
            count=len(result),
        )
        return result

    log.info('fetching tickers', active=active)
    rows = _fetch_tickers(active=active)
    _save(rows, active=active)
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
