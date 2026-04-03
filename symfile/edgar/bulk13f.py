"""SEC 13F bulk data set downloader.

Downloads quarterly 13F zips from SEC structured data:
  sec.gov/files/structureddata/data/form-13f-data-sets/

Each zip contains TSV files: INFOTABLE, COVERPAGE,
SUBMISSION, SUMMARYPAGE, etc.
"""

import csv
import io
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

from symfile.edgar.fetch import USER_AGENT

BASE_URL = (
    'https://www.sec.gov/files/structureddata'
    '/data/form-13f-data-sets'
)

QUARTERS = {
    (2025, 3): '01sep2025-30nov2025',
    (2025, 4): '01dec2025-28feb2026',
}

DATA_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / 'data'
    / '13f'
)


@dataclass
class InfoRow:
    accession: str
    issuer: str
    title: str
    cusip: str
    value: int
    shares: int
    sh_type: str
    put_call: str
    discretion: str
    sole: int
    shared: int
    none_auth: int


def _zip_url(year: int, quarter: int) -> str:
    key = (year, quarter)
    if key not in QUARTERS:
        raise ValueError(
            f'no bulk data for {year}/Q{quarter}'
        )
    slug = QUARTERS[key]
    return f'{BASE_URL}/{slug}_form13f.zip'


def _cache_path(
    year: int, quarter: int
) -> Path:
    return DATA_DIR / f'{year}Q{quarter}.zip'


def fetch_bulk_zip(
    year: int, quarter: int
) -> Path:
    """Download quarterly 13F zip (cached)."""
    path = _cache_path(year, quarter)
    if path.exists():
        print(f'using cached {path.name}')
        return path

    url = _zip_url(year, quarter)
    print(f'downloading {url}...')
    req = urllib.request.Request(
        url, headers={'User-Agent': USER_AGENT}
    )
    resp = urllib.request.urlopen(req, timeout=120)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path.write_bytes(resp.read())
    mb = path.stat().st_size / 1e6
    print(f'  saved {path.name} ({mb:.0f} MB)')
    return path


def _int_or_zero(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def read_infotable(
    zip_path: Path,
) -> list[InfoRow]:
    """Read INFOTABLE.tsv from a 13F bulk zip."""
    with zipfile.ZipFile(zip_path) as zf:
        raw = zf.read('INFOTABLE.tsv')
    text = raw.decode('utf-8', errors='replace')
    reader = csv.DictReader(
        io.StringIO(text), delimiter='\t'
    )
    rows = []
    for r in reader:
        rows.append(
            InfoRow(
                accession=r['ACCESSION_NUMBER'],
                issuer=r['NAMEOFISSUER'],
                title=r['TITLEOFCLASS'],
                cusip=r['CUSIP'],
                value=_int_or_zero(r['VALUE']),
                shares=_int_or_zero(
                    r['SSHPRNAMT']
                ),
                sh_type=r.get(
                    'SSHPRNAMTTYPE', ''
                ),
                put_call=r.get('PUTCALL', ''),
                discretion=r.get(
                    'INVESTMENTDISCRETION', ''
                ),
                sole=_int_or_zero(
                    r.get('VOTING_AUTH_SOLE', '')
                ),
                shared=_int_or_zero(
                    r.get(
                        'VOTING_AUTH_SHARED', ''
                    )
                ),
                none_auth=_int_or_zero(
                    r.get(
                        'VOTING_AUTH_NONE', ''
                    )
                ),
            )
        )
    return rows


def read_submissions(
    zip_path: Path,
) -> dict[str, dict]:
    """Read SUBMISSION.tsv -> accession->info."""
    with zipfile.ZipFile(zip_path) as zf:
        raw = zf.read('SUBMISSION.tsv')
    text = raw.decode('utf-8', errors='replace')
    reader = csv.DictReader(
        io.StringIO(text), delimiter='\t'
    )
    result = {}
    for r in reader:
        result[r['ACCESSION_NUMBER']] = {
            'cik': r['CIK'],
            'filing_date': r['FILING_DATE'],
            'form_type': r['SUBMISSIONTYPE'],
            'period': r['PERIODOFREPORT'],
        }
    return result


def extract_cusips(
    zip_path: Path,
) -> set[str]:
    """Extract unique CUSIPs from INFOTABLE.tsv."""
    with zipfile.ZipFile(zip_path) as zf:
        raw = zf.read('INFOTABLE.tsv')
    cusips = set()
    for line in raw.split(b'\n')[1:]:
        parts = line.split(b'\t')
        if len(parts) >= 5:
            c = parts[4].decode(
                'ascii', errors='ignore'
            ).strip()
            if c:
                cusips.add(c)
    return cusips
