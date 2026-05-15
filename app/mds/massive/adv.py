"""30-day ADV per symbol via Polygon daily bars.

Per-symbol pattern (mirrors symtools hist fetch +
analytics.get_advs): one list_aggs call per symbol
over a 60-calendar-day window, N=20 concurrent.
Take the mean volume over the most recent 30
trading days.

Caches to data/mds/adv.YYYYMMDD.csv. Refreshed
weekly (MAX_AGE_DAYS=7).
"""

import asyncio
import re
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.mds import DATA_DIR
from app.mds.massive.session import get_client
from app.util.log import log

MAX_AGE_DAYS = 7
CONCURRENCY = 20
WINDOW_DAYS = 30        # trading days for ADV mean
LOOKBACK_CAL = 60       # calendar days to request
REQUEST_TIMEOUT_S = 30  # per-call
MAX_RETRIES = 2


def _find_cached() -> tuple[Path, date] | None:
    pattern = re.compile(r'adv\.(\d{8})\.csv$')
    best: tuple[Path, date] | None = None
    if not DATA_DIR.exists():
        return None
    for f in DATA_DIR.iterdir():
        m = pattern.match(f.name)
        if m:
            d = date.fromisoformat(
                f'{m.group(1)[:4]}-'
                f'{m.group(1)[4:6]}-'
                f'{m.group(1)[6:]}'
            )
            if best is None or d > best[1]:
                best = (f, d)
    return best


async def _fetch_adv_async(
    symbols: list[str],
) -> dict[str, float]:
    """Fetch daily bars per symbol and compute
    mean volume over the last WINDOW_DAYS trading
    days."""
    client = get_client()

    end = date.today()
    start = end - timedelta(days=LOOKBACK_CAL)
    from_s = start.strftime('%Y-%m-%d')
    to_s = end.strftime('%Y-%m-%d')

    log.info(
        'fetching daily bars',
        symbols=len(symbols),
        window_days=WINDOW_DAYS,
        concurrency=CONCURRENCY,
    )

    sem = asyncio.Semaphore(CONCURRENCY)
    result: dict[str, float] = {}
    lock = asyncio.Lock()
    done = failed = 0

    def _fetch_sync(sym: str) -> list[float]:
        aggs = client.list_aggs(
            sym,
            1,
            'day',
            from_s,
            to_s,
            adjusted=True,
            sort='asc',
            limit=50000,
        )
        return [
            float(a.volume or 0.0) for a in aggs
        ]

    async def fetch_one(sym: str) -> None:
        nonlocal done, failed
        async with sem:
            vols: list[float] | None = None
            last_err: str = ''
            for attempt in range(MAX_RETRIES + 1):
                try:
                    vols = await asyncio.wait_for(
                        asyncio.to_thread(
                            _fetch_sync, sym
                        ),
                        timeout=REQUEST_TIMEOUT_S,
                    )
                    break
                except Exception as e:
                    last_err = str(e) or type(
                        e
                    ).__name__
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(
                            0.5 * (attempt + 1)
                        )

            if vols is None:
                async with lock:
                    failed += 1
                    if failed <= 20:
                        log.warning(
                            'adv fetch failed',
                            sym=sym, err=last_err,
                        )
                return

            recent = [
                v for v in vols[-WINDOW_DAYS:]
                if v > 0
            ]
            if recent:
                adv = sum(recent) / len(recent)
                async with lock:
                    result[sym] = adv
            async with lock:
                done += 1
                if done % 200 == 0:
                    log.info(
                        'adv progress',
                        done=done,
                        failed=failed,
                        total=len(symbols),
                    )

    await asyncio.gather(
        *(fetch_one(s) for s in symbols)
    )

    log.info(
        'adv computed',
        symbols=len(result),
        done=done,
        failed=failed,
    )
    return result


def _save(adv: dict[str, float]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'adv.{stamp}.csv'
    df = pl.DataFrame(
        [
            {'symbol': s, 'adv': v}
            for s, v in sorted(adv.items())
        ],
        schema={
            'symbol': pl.Utf8,
            'adv': pl.Float64,
        },
    )
    df.write_csv(path)
    log.info(
        'saved adv',
        count=len(adv),
        path=str(path),
    )
    return path


def _load_csv(path: Path) -> dict[str, float]:
    df = pl.read_csv(
        path,
        schema={
            'symbol': pl.Utf8,
            'adv': pl.Float64,
        },
    )
    return dict(zip(df['symbol'], df['adv']))


def load_adv(
    max_age_days: int = MAX_AGE_DAYS,
    build: bool = False,
    symbols: list[str] | None = None,
) -> dict[str, float]:
    """Load ADV.

    Returns the most recent cached `adv.*.csv` (any
    age). When `build=True`, refetches per-symbol
    daily bars if the cache is older than
    `max_age_days` or missing. Otherwise never
    makes a network call — returns {} if no cache
    exists.

    `symbols` restricts the build universe; if
    omitted, loads all symbols from refs.
    """
    cached = _find_cached()

    if not build:
        if cached is None:
            return {}
        path = cached[0]
        log.debug('cached adv', file=path.name)
        result = _load_csv(path)
        log.info('adv loaded', count=len(result))
        return result

    cutoff = date.today() - timedelta(
        days=max_age_days
    )
    if cached and cached[1] >= cutoff:
        path = cached[0]
        log.debug('cached adv', file=path.name)
        return _load_csv(path)

    if symbols is None:
        from app.mds.massive.refs import load_refs
        refs = load_refs()
        symbols = sorted(refs.keys())

    from app.util.asyncio import run_coro
    log.info('building adv', universe=len(symbols))
    adv = run_coro(_fetch_adv_async(symbols))
    _save(adv)
    return adv
