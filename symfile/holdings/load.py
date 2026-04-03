"""Load 13F bulk data into polars DataFrames."""

import io
import zipfile

import polars as pl

from symfile.edgar.bulk13f import fetch_bulk_zip


def _read_tsv(
    zip_path, name: str
) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        raw = zf.read(name)
    return pl.read_csv(
        io.BytesIO(raw),
        separator='\t',
        infer_schema_length=10000,
        schema_overrides={
            'OTHERMANAGER': pl.Utf8,
            'OTHERMANAGER2': pl.Utf8,
        },
        ignore_errors=True,
    )


def load_quarter(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load a quarter's 13F data as a single
    DataFrame with holdings + filer info.

    Returns columns: accession, filer, cusip,
    issuer, title, value, shares, sh_type,
    put_call, discretion, sole, shared, none_auth
    """
    zp = fetch_bulk_zip(year, qtr)

    info = _read_tsv(zp, 'INFOTABLE.tsv').select(
        pl.col('ACCESSION_NUMBER').alias(
            'accession'
        ),
        pl.col('NAMEOFISSUER').alias('issuer'),
        pl.col('TITLEOFCLASS').alias('title'),
        pl.col('CUSIP').alias('cusip'),
        pl.col('VALUE')
        .cast(pl.Int64, strict=False)
        .alias('value'),
        pl.col('SSHPRNAMT')
        .cast(pl.Int64, strict=False)
        .alias('shares'),
        pl.col('SSHPRNAMTTYPE').alias('sh_type'),
        pl.col('PUTCALL')
        .fill_null('')
        .alias('put_call'),
        pl.col('INVESTMENTDISCRETION').alias(
            'discretion'
        ),
        pl.col('VOTING_AUTH_SOLE')
        .cast(pl.Int64, strict=False)
        .alias('sole'),
        pl.col('VOTING_AUTH_SHARED')
        .cast(pl.Int64, strict=False)
        .alias('shared'),
        pl.col('VOTING_AUTH_NONE')
        .cast(pl.Int64, strict=False)
        .alias('none_auth'),
    )

    cover = _read_tsv(zp, 'COVERPAGE.tsv').select(
        pl.col('ACCESSION_NUMBER').alias(
            'accession'
        ),
        pl.col('FILINGMANAGER_NAME').alias(
            'filer'
        ),
    )

    return info.join(cover, on='accession')


def load_with_syms(
    year: int,
    qtr: int,
    cusip_map: dict[str, str],
) -> pl.DataFrame:
    """Load quarter and map CUSIPs to symbols.

    Filters to rows in the cusip_map universe.
    Adds 'symbol' column.
    """
    df = load_quarter(year, qtr)
    mapping = pl.DataFrame(
        {
            'cusip': list(cusip_map.keys()),
            'symbol': list(cusip_map.values()),
        }
    )
    return df.join(mapping, on='cusip')
