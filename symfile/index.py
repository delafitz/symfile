"""EDGAR daily-index and full-index fetchers.

Parses master.idx files (pipe-delimited):
    CIK|Company Name|Form Type|Date Filed|Filename

Caches index files and individual filings to disk
so repeated runs never re-fetch.
"""

import asyncio
import hashlib
import time
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / 'data'
CACHE_DIR = DATA_DIR / 'filings'

BASE = 'https://www.sec.gov/Archives/edgar'
DAILY_URL = (
    f'{BASE}/daily-index/{{year}}/QTR{{qtr}}'
    '/master.{stamp}.idx'
)
FULL_URL = (
    f'{BASE}/full-index/{{year}}/QTR{{qtr}}'
    '/master.idx'
)
USER_AGENT = 'symfile dev@symfile.dev'

FORM_PREFIXES = ('144',)

_last_fetch = 0.0
SEC_RPS = 6  # requests per second (safe under 10)
MAX_RETRIES = 3


@dataclass
class Filing:
    cik: str
    company: str
    form_type: str
    date_filed: str
    filename: str


# --- HTTP helpers ---


def _fetch(url: str) -> bytes | None:
    """Fetch URL with SEC rate limiting (10 req/s)."""
    global _last_fetch
    elapsed = time.time() - _last_fetch
    if elapsed < 0.12:
        time.sleep(0.12 - elapsed)
    req = urllib.request.Request(
        url, headers={'User-Agent': USER_AGENT}
    )
    try:
        _last_fetch = time.time()
        return urllib.request.urlopen(
            req, timeout=30
        ).read()
    except Exception as e:
        print(f'  fetch error: {url}: {e}')
        return None


def _fetch_retry(url: str) -> bytes | None:
    """Fetch URL with retry on 429. No rate limiting
    (caller handles via token bucket)."""
    req = urllib.request.Request(
        url, headers={'User-Agent': USER_AGENT}
    )
    for attempt in range(MAX_RETRIES):
        try:
            return urllib.request.urlopen(
                req, timeout=30
            ).read()
        except urllib.request.HTTPError as e:
            if (
                e.code == 429
                and attempt < MAX_RETRIES - 1
            ):
                time.sleep(2 ** attempt)
                continue
            print(f'  fetch error: {url}: {e}')
            return None
        except Exception as e:
            print(f'  fetch error: {url}: {e}')
            return None


# --- Filing cache ---


def _cache_key(filename: str) -> str:
    """Stable short key from EDGAR filename."""
    return hashlib.md5(
        filename.encode()
    ).hexdigest()[:12]


def _cache_path(filename: str) -> Path:
    return CACHE_DIR / f'{_cache_key(filename)}.txt'


def _get_cached(filename: str) -> bytes | None:
    p = _cache_path(filename)
    if p.exists():
        return p.read_bytes()
    return None


def _put_cache(filename: str, data: bytes) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(filename).write_bytes(data)


# --- Index parsing ---


def _parse_master_idx(raw: bytes) -> list[Filing]:
    """Parse a master.idx file into Filing objects."""
    filings = []
    in_data = False
    for line in raw.decode('latin-1').splitlines():
        if line.startswith('---'):
            in_data = True
            continue
        if not in_data:
            continue
        parts = line.split('|')
        if len(parts) != 5:
            continue
        filings.append(
            Filing(
                cik=parts[0].strip(),
                company=parts[1].strip(),
                form_type=parts[2].strip(),
                date_filed=parts[3].strip(),
                filename=parts[4].strip(),
            )
        )
    return filings


def _quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1


def fetch_daily_index(d: date) -> list[Filing]:
    """Fetch daily master.idx (cached to disk)."""
    stamp = d.strftime('%Y%m%d')
    cache_name = f'daily.{stamp}'
    cached = _get_cached(cache_name)
    if cached:
        return _parse_master_idx(cached)
    url = DAILY_URL.format(
        year=d.year, qtr=_quarter(d), stamp=stamp
    )
    raw = _fetch(url)
    if not raw:
        return []
    _put_cache(cache_name, raw)
    return _parse_master_idx(raw)


def fetch_full_index(
    year: int, quarter: int
) -> list[Filing]:
    """Fetch quarter master.idx (cached to disk)."""
    cache_name = f'full.{year}Q{quarter}'
    cached = _get_cached(cache_name)
    if cached:
        print(f'  (using cached index)')
        return _parse_master_idx(cached)
    url = FULL_URL.format(year=year, qtr=quarter)
    raw = _fetch(url)
    if not raw:
        return []
    _put_cache(cache_name, raw)
    return _parse_master_idx(raw)


def filter_forms(
    filings: list[Filing],
    prefixes: tuple[str, ...] = FORM_PREFIXES,
) -> list[Filing]:
    """Filter filings to matching form type prefixes."""
    return [
        f
        for f in filings
        if any(
            f.form_type.startswith(p)
            for p in prefixes
        )
    ]


# --- Async bulk fetch with filing cache ---


FILING_BASE = 'https://www.sec.gov/Archives/'


async def fetch_filings_async(
    filings: list[Filing],
    callback,
) -> None:
    """Fetch + process filings concurrently.

    Checks disk cache first; only fetches uncached
    filings from SEC. Token-bucket rate limiter
    stays under 10 req/s.
    """
    # Serve cached filings immediately
    to_fetch: list[Filing] = []
    for f in filings:
        cached = _get_cached(f.filename)
        if cached:
            callback(f, cached)
        else:
            to_fetch.append(f)

    cached_n = len(filings) - len(to_fetch)
    if cached_n:
        print(f'  {cached_n} from cache')
    if not to_fetch:
        return

    print(f'  {len(to_fetch)} to fetch...')
    bucket = asyncio.Queue(maxsize=SEC_RPS)
    done = 0
    total = len(to_fetch)

    async def refill() -> None:
        interval = 1.0 / SEC_RPS
        while True:
            await bucket.put(True)
            await asyncio.sleep(interval)

    async def fetch_one(f: Filing) -> None:
        nonlocal done
        await bucket.get()
        raw = await asyncio.to_thread(
            _fetch_retry,
            FILING_BASE + f.filename,
        )
        done += 1
        if done % 500 == 0:
            print(f'  ... {done}/{total}')
        if raw:
            _put_cache(f.filename, raw)
            callback(f, raw)

    filler = asyncio.create_task(refill())
    try:
        await asyncio.gather(
            *(fetch_one(f) for f in to_fetch)
        )
    finally:
        filler.cancel()
