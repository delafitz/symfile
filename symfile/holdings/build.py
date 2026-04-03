"""Build per-quarter holdings parquet files.

Each parquet covers our symref universe with schema:
  symbol, filing_date, holder, shares, value

Stored at data/holdings/YYYYQN.parquet
"""

import io
import zipfile
from pathlib import Path

import polars as pl

from symfile.edgar.bulk13f import fetch_bulk_zip
from symfile.mds import DATA_DIR as MDS_DIR

HOLDINGS_DIR = (
    Path(MDS_DIR).parent / 'holdings'
)


def _parquet_path(
    year: int, qtr: int
) -> Path:
    return HOLDINGS_DIR / f'{year}Q{qtr}.parquet'


def build_quarter(
    year: int,
    qtr: int,
    cusip_map: dict[str, str],
) -> Path:
    """Build holdings parquet for one quarter."""
    out = _parquet_path(year, qtr)
    if out.exists():
        print(f'  {out.name} exists, skipping')
        return out

    zp = fetch_bulk_zip(year, qtr)

    with zipfile.ZipFile(zp) as zf:
        info_raw = zf.read('INFOTABLE.tsv')
        cover_raw = zf.read('COVERPAGE.tsv')
        sub_raw = zf.read('SUBMISSION.tsv')

    info = pl.read_csv(
        io.BytesIO(info_raw),
        separator='\t',
        infer_schema_length=10000,
        schema_overrides={
            'OTHERMANAGER': pl.Utf8,
        },
        ignore_errors=True,
    )

    cover = pl.read_csv(
        io.BytesIO(cover_raw),
        separator='\t',
        infer_schema_length=10000,
        ignore_errors=True,
    )

    sub = pl.read_csv(
        io.BytesIO(sub_raw),
        separator='\t',
        infer_schema_length=10000,
        ignore_errors=True,
    )

    cusip_df = pl.DataFrame(
        {
            'cusip': list(cusip_map.keys()),
            'symbol': list(cusip_map.values()),
        }
    )

    df = (
        info.filter(
            pl.col('SSHPRNAMTTYPE') == 'SH'
        )
        .filter(
            pl.col('PUTCALL').is_null()
            | (pl.col('PUTCALL') == '')
        )
        .join(cusip_df, left_on='CUSIP', right_on='cusip')
        .join(
            cover.select(
                'ACCESSION_NUMBER',
                'FILINGMANAGER_NAME',
            ),
            on='ACCESSION_NUMBER',
        )
        .join(
            sub.select(
                'ACCESSION_NUMBER',
                'FILING_DATE',
            ),
            on='ACCESSION_NUMBER',
        )
        .select(
            pl.col('symbol'),
            pl.col('FILING_DATE').alias(
                'filing_date'
            ),
            pl.col('FILINGMANAGER_NAME').alias(
                'holder'
            ),
            pl.col('SSHPRNAMT')
            .cast(pl.Int64, strict=False)
            .alias('shares'),
        )
    )

    HOLDINGS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    print(
        f'  {out.name}: {df.height:,} rows, '
        f'{out.stat().st_size / 1e6:.1f} MB'
    )
    return out


def build_all(
    cusip_map: dict[str, str],
    quarters: list[tuple[int, int]] | None = None,
) -> list[Path]:
    """Build parquets for all configured quarters."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]
    paths = []
    for year, qtr in quarters:
        paths.append(
            build_quarter(year, qtr, cusip_map)
        )
    return paths


def load_quarter_parquet(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load a pre-built quarter parquet."""
    p = _parquet_path(year, qtr)
    if not p.exists():
        raise FileNotFoundError(
            f'{p} not found — run build first'
        )
    return pl.read_parquet(p)
