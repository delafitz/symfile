"""Create dated, versioned tar.gz backups of downloaded data."""

import tarfile
from datetime import date
from pathlib import Path

from app.util.log import log

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
BACKUP_DIR = PROJECT_ROOT / 'backup'

BACKUP_DIRS = ['13f', 'filings', 'holdings', 'indices']


def _next_path(day: date) -> Path:
    """Return backup/symfile-YYYY-MM-DD-vN.tar.gz with auto-incremented N."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    prefix = f'symfile-{day.isoformat()}'
    existing = sorted(BACKUP_DIR.glob(f'{prefix}-v*.tar.gz'))
    if existing:
        last = existing[-1].stem  # e.g. symfile-2026-04-07-v2.tar
        last = last.removesuffix('.tar')
        ver = int(last.rsplit('-v', 1)[1]) + 1
    else:
        ver = 1
    return BACKUP_DIR / f'{prefix}-v{ver}.tar.gz'


def create_backup() -> Path:
    """Tar+gzip the data subdirectories into a versioned backup file."""
    dest = _next_path(date.today())
    dirs = [DATA_DIR / d for d in BACKUP_DIRS if (DATA_DIR / d).exists()]

    if not dirs:
        raise SystemExit('nothing to back up — no data dirs found')

    log.info(
        'creating backup',
        dest=str(dest.name),
        dirs=[d.name for d in dirs],
    )

    with tarfile.open(dest, 'w:gz') as tar:
        for d in dirs:
            tar.add(d, arcname=f'data/{d.name}')

    size_mb = dest.stat().st_size / 1e6
    log.info('backup complete', file=dest.name, size_mb=round(size_mb, 1))
    return dest
