"""CUSIP-to-symbol mapping via Polygon tickers API.

Given a set of CUSIPs (from 13F filings), resolves
each to a ticker symbol using Polygon's
/v3/reference/tickers?cusip=XXX endpoint.

Filters to symbols in our sym universe (mkt_cap>=1B).
Caches to data/mds/cusips.YYYYMMDD.csv.
"""

import asyncio
import csv
import re
from datetime import date, timedelta
from pathlib import Path

from symfile.mds import DATA_DIR
from symfile.mds.massive.session import get_client

MAX_AGE_DAYS = 30
CONCURRENCY = 20


def _find_cached() -> tuple[Path, date] | None:
    pattern = re.compile(r'cusips\.(\d{8})\.csv$')
    best: tuple[Path, date] | None = None
    if not DATA_DIR.exists():
        return None
    for f in DATA_DIR.iterdir():
        m = pattern.match(f.name)
        if m:
            d = date.fromisoformat(
                f'{m.group(1)[:4]}'
                f'-{m.group(1)[4:6]}'
                f'-{m.group(1)[6:]}'
            )
            if best is None or d > best[1]:
                best = (f, d)
    return best


async def _fetch_cusips_async(
    cusips: set[str],
    universe: set[str],
) -> dict[str, str]:
    """Map CUSIPs to symbols via Polygon.

    Only returns mappings where the resolved ticker
    is in the given universe set.
    """
    client = get_client()

    sem = asyncio.Semaphore(CONCURRENCY)
    result: dict[str, str] = {}
    lock = asyncio.Lock()
    done = 0
    total = len(cusips)

    async def fetch_one(cusip: str) -> None:
        nonlocal done
        async with sem:
            try:
                tickers = await asyncio.to_thread(
                    lambda: list(
                        client.list_tickers(
                            cusip=cusip,
                            market='stocks',
                            active=True,
                            limit=5,
                        )
                    )
                )
            except Exception:
                return
            finally:
                async with lock:
                    done += 1
                    if done % 500 == 0:
                        print(
                            f'  {done}/{total}'
                            f' ({len(result)} mapped)'
                        )

            for t in tickers:
                sym = t.ticker
                if sym in universe:
                    async with lock:
                        result[cusip] = sym
                    return

    await asyncio.gather(
        *(fetch_one(c) for c in cusips)
    )
    return result


def _save(
    mapping: dict[str, str],
) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'cusips.{stamp}.csv'
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(
            f, fieldnames=['cusip', 'symbol']
        )
        w.writeheader()
        for cusip, sym in sorted(
            mapping.items()
        ):
            w.writerow(
                {'cusip': cusip, 'symbol': sym}
            )
    print(f'saved {len(mapping)} cusips to {path}')
    return path


def _load_csv(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            result[row['cusip']] = row['symbol']
    return result


def load_cusips(
    cusips: set[str] | None = None,
    universe: set[str] | None = None,
    max_age_days: int = MAX_AGE_DAYS,
) -> dict[str, str]:
    """Load CUSIP->symbol mapping.

    Returns cached if fresh. If cusips and universe
    are provided and cache is stale, fetches fresh.
    """
    cached = _find_cached()
    cutoff = date.today() - timedelta(
        days=max_age_days
    )

    if cached and cached[1] >= cutoff:
        path = cached[0]
        print(
            f'using cached cusips from {path.name}'
        )
        result = _load_csv(path)
        print(f'  {len(result)} mappings')
        return result

    if cusips is None or universe is None:
        if cached:
            print(
                'cusip cache stale, '
                'returning old data'
            )
            return _load_csv(cached[0])
        return {}

    print(
        f'resolving {len(cusips)} cusips '
        f'against {len(universe)} symbols...'
    )
    mapping = asyncio.run(
        _fetch_cusips_async(cusips, universe)
    )
    _save(mapping)
    return mapping
