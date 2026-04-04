"""Build per-quarter holdings parquet files.

Base parquet: sym, filing_date, holder, shares
  from 13F-HR originals in bulk zip

Amendments parquet: same schema, from 13F-HR/A
  (bulk zip + individually fetched late filers)

Stored at data/holdings/YYYYQN.parquet
          data/holdings/YYYYQN_amends.parquet
"""

import io
import zipfile
from pathlib import Path

import polars as pl

from symfile.edgar.bulk13f import fetch_bulk_zip
from symfile.mds import DATA_DIR as MDS_DIR
from symfile.util.log import log

HOLDINGS_DIR = (
    Path(MDS_DIR).parent / 'holdings'
)


def _parquet_path(
    year: int, qtr: int
) -> Path:
    return HOLDINGS_DIR / f'{year}Q{qtr}.parquet'


def _read_bulk_tsv(zp, name):
    with zipfile.ZipFile(zp) as zf:
        raw = zf.read(name)
    return pl.read_csv(
        io.BytesIO(raw),
        separator='\t',
        infer_schema_length=10000,
        schema_overrides={
            'OTHERMANAGER': pl.Utf8,
        },
        ignore_errors=True,
    )


def _to_holdings(
    info: pl.DataFrame,
    cover: pl.DataFrame,
    sub: pl.DataFrame,
    cusip_df: pl.DataFrame,
    form_types: list[str],
) -> pl.DataFrame:
    """Filter + join TSV tables into holdings
    schema: symbol, filing_date, holder, shares."""
    accs = sub.filter(
        pl.col('SUBMISSIONTYPE').is_in(form_types)
    )['ACCESSION_NUMBER']

    return (
        info.filter(
            pl.col('ACCESSION_NUMBER').is_in(accs)
            & (pl.col('SSHPRNAMTTYPE') == 'SH')
            & (
                pl.col('PUTCALL').is_null()
                | (pl.col('PUTCALL') == '')
            )
        )
        .join(
            cusip_df,
            left_on='CUSIP',
            right_on='cusip',
        )
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


def _write_parquet(
    df: pl.DataFrame, path: Path
) -> Path:
    HOLDINGS_DIR.mkdir(
        parents=True, exist_ok=True
    )
    df.write_parquet(path)
    log.info(
        'built parquet',
        file=path.name,
        rows=df.height,
        mb=round(path.stat().st_size / 1e6, 1),
    )
    return path


def build_quarter(
    year: int,
    qtr: int,
    cusip_map: dict[str, str],
) -> Path:
    """Build base holdings parquet (originals)."""
    out = _parquet_path(year, qtr)
    if out.exists():
        log.debug('holdings cached', file=out.name)
        return out

    zp = fetch_bulk_zip(year, qtr)
    info = _read_bulk_tsv(zp, 'INFOTABLE.tsv')
    cover = _read_bulk_tsv(zp, 'COVERPAGE.tsv')
    sub = _read_bulk_tsv(zp, 'SUBMISSION.tsv')
    cusip_df = pl.DataFrame({
        'cusip': list(cusip_map.keys()),
        'symbol': list(cusip_map.values()),
    })

    df = _to_holdings(
        info, cover, sub, cusip_df, ['13F-HR']
    )
    return _write_parquet(df, out)


def _amends_path(
    year: int, qtr: int
) -> Path:
    return (
        HOLDINGS_DIR / f'{year}Q{qtr}_amends.parquet'
    )


def build_amendments(
    year: int,
    qtr: int,
    cusip_map: dict[str, str],
) -> Path:
    """Build amendments parquet from bulk zip.

    Keeps only the latest amendment per filer.
    """
    out = _amends_path(year, qtr)
    zp = fetch_bulk_zip(year, qtr)
    info = _read_bulk_tsv(zp, 'INFOTABLE.tsv')
    cover = _read_bulk_tsv(zp, 'COVERPAGE.tsv')
    sub = _read_bulk_tsv(zp, 'SUBMISSION.tsv')
    cusip_df = pl.DataFrame({
        'cusip': list(cusip_map.keys()),
        'symbol': list(cusip_map.values()),
    })

    df = _to_holdings(
        info, cover, sub, cusip_df, ['13F-HR/A']
    )

    if df.height == 0:
        _write_parquet(df, out)
        return out

    latest = (
        df.group_by('holder')
        .agg(pl.col('filing_date').max())
        .select('holder', 'filing_date')
    )

    df = df.join(
        latest,
        on=['holder', 'filing_date'],
        how='semi',
    )

    return _write_parquet(df, out)


def build_all(
    cusip_map: dict[str, str],
    quarters: list[tuple[int, int]]
    | None = None,
) -> list[Path]:
    """Build base + amendment parquets."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]
    paths = []
    for year, qtr in quarters:
        paths.append(
            build_quarter(year, qtr, cusip_map)
        )
        paths.append(
            build_amendments(year, qtr, cusip_map)
        )
    return paths


def load_quarter(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load base quarter parquet."""
    p = _parquet_path(year, qtr)
    if not p.exists():
        raise FileNotFoundError(
            f'{p} not found — run build first'
        )
    return pl.read_parquet(p)


def load_amendments(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load amendments parquet (may be empty)."""
    p = _amends_path(year, qtr)
    if not p.exists():
        return pl.DataFrame(
            schema={
                'symbol': pl.Utf8,
                'filing_date': pl.Utf8,
                'holder': pl.Utf8,
                'shares': pl.Int64,
            }
        )
    return pl.read_parquet(p)


def load_effective(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load base + overlay amendments.

    For holders with amendments, replaces base
    rows with amendment rows.
    """
    base = load_quarter(year, qtr)
    amends = load_amendments(year, qtr)

    if amends.height == 0:
        return base

    amended_holders = amends.select(
        'holder'
    ).unique()

    base_kept = base.join(
        amended_holders,
        on='holder',
        how='anti',
    )

    return pl.concat([base_kept, amends])
