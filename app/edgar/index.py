"""EDGAR daily-index and full-index fetchers.

Parses master.idx files (pipe-delimited):
    CIK|Company Name|Form Type|Date Filed|Filename
"""

from dataclasses import dataclass
from datetime import date

from app.util.dates import quarter as _quarter
from app.util.log import log

from app.edgar.fetch import (
    FILING_BASE,
    fetch_many_async,
    fetch_url,
    get_index,
    put_index,
)

BASE = 'https://www.sec.gov/Archives/edgar'
DAILY_URL = (
    f'{BASE}/daily-index/{{year}}/QTR{{qtr}}'
    '/master.{stamp}.idx'
)
FULL_URL = (
    f'{BASE}/full-index/{{year}}/QTR{{qtr}}'
    '/master.idx'
)

FORM_PREFIXES = ('144',)


@dataclass
class Filing:
    cik: str
    company: str
    form_type: str
    date_filed: str
    filename: str


def _parse_master_idx(
    raw: bytes,
) -> list[Filing]:
    """Parse a master.idx into Filing objects."""
    filings = []
    in_data = False
    for line in (
        raw.decode('latin-1').splitlines()
    ):
        if line.startswith('---'):
            in_data = True
            continue
        if not in_data:
            continue
        parts = line.split('|')
        if len(parts) != 5:
            continue
        df = parts[3].strip()
        if len(df) == 8 and '-' not in df:
            df = f'{df[:4]}-{df[4:6]}-{df[6:]}'
        filings.append(
            Filing(
                cik=parts[0].strip(),
                company=parts[1].strip(),
                form_type=parts[2].strip(),
                date_filed=df,
                filename=parts[4].strip(),
            )
        )
    return filings


def fetch_daily_index(
    d: date,
    force: bool = False,
) -> list[Filing]:
    """Fetch daily master.idx (cached).

    force=True re-fetches even if cached (for
    re-checking a day that may have grown).
    """
    stamp = d.strftime('%Y%m%d')
    name = f'daily.{stamp}'
    if not force:
        cached = get_index(name)
        if cached:
            return _parse_master_idx(cached)
    url = DAILY_URL.format(
        year=d.year,
        qtr=_quarter(d),
        stamp=stamp,
    )
    raw = fetch_url(url)
    if not raw:
        return []
    put_index(name, raw)
    return _parse_master_idx(raw)


def fetch_full_index(
    year: int, quarter: int
) -> list[Filing]:
    """Fetch quarter master.idx (cached)."""
    name = f'full.{year}Q{quarter}'
    cached = get_index(name)
    if cached:
        log.debug('cached index', name=name)
        return _parse_master_idx(cached)
    url = FULL_URL.format(
        year=year, qtr=quarter
    )
    raw = fetch_url(url)
    if not raw:
        return []
    put_index(name, raw)
    return _parse_master_idx(raw)


def filter_forms(
    filings: list[Filing],
    prefixes: tuple[str, ...] = FORM_PREFIXES,
) -> list[Filing]:
    """Filter filings to matching form types."""
    return [
        f
        for f in filings
        if any(
            f.form_type.startswith(p)
            for p in prefixes
        )
    ]


async def fetch_filings_async(
    filings: list[Filing],
    callback,
) -> None:
    """Fetch + process filings concurrently."""
    await fetch_many_async(
        filings,
        key_fn=lambda f: f.filename,
        url_fn=lambda f: FILING_BASE
        + f.filename,
        callback=callback,
    )
