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
from datetime import date
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
    """Build base + amendment parquets.

    Also truncates 13D table to keep only filings
    after the prior quarter end.
    """
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

    if quarters:
        from symfile.holdings.schedule13d import (
            truncate,
        )

        prev_y, prev_q = quarters[-2] if len(
            quarters
        ) > 1 else quarters[0]
        truncate(prev_y, prev_q)

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


def _quarter_end(
    year: int, qtr: int
) -> str:
    """Quarter-end date as YYYY-MM-DD."""
    ends = {1: '03-31', 2: '06-30',
            3: '09-30', 4: '12-31'}
    return f'{year}-{ends[qtr]}'


def load_effective(
    year: int, qtr: int
) -> pl.DataFrame:
    """Load base + overlay amendments + 13D.

    Layer 1: base 13F-HR originals
    Layer 2: 13F-HR/A amendments (full replace
             per holder)
    Layer 3: 13D positions (per holder+symbol,
             only where 13D event is after the
             quarter end and after the 13F date)
    """
    base = load_quarter(year, qtr)
    amends = load_amendments(year, qtr)

    if amends.height > 0:
        amended_holders = amends.select(
            'holder'
        ).unique()
        base = base.join(
            amended_holders,
            on='holder',
            how='anti',
        )
        base = pl.concat([base, amends])

    next_qtr_path = _parquet_path(
        year + (1 if qtr == 4 else 0),
        1 if qtr == 4 else qtr + 1,
    )
    is_latest = not next_qtr_path.exists()

    if is_latest:
        qe = _quarter_end(year, qtr)
        base = _overlay_13d(base, qe)

    return base


def _to_iso(s: str) -> str:
    """Convert various date formats to
    YYYY-MM-DD for comparison."""
    from datetime import datetime

    for fmt in (
        '%m/%d/%Y',
        '%d-%b-%Y',
        '%Y-%m-%d',
    ):
        try:
            return datetime.strptime(
                s, fmt
            ).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return s


def _overlay_13d(
    holdings: pl.DataFrame,
    quarter_end: str,
) -> pl.DataFrame:
    """Overlay 13D positions where more recent.

    Only applies 13D events that are AFTER the
    quarter end date AND after the holder's 13F
    filing date for that symbol.
    """
    from symfile.holdings.aliases import (
        build_matcher,
    )
    from symfile.holdings.schedule13d import (
        load_13d,
    )

    d13 = load_13d()
    if d13.height == 0:
        return holdings

    holders_13f = (
        holdings['holder'].unique().to_list()
    )
    match = build_matcher(holders_13f)

    updates: list[dict] = []
    for row in d13.iter_rows(named=True):
        f_name = match(row['holder'])
        if not f_name:
            continue

        sym = row['symbol']
        existing = holdings.filter(
            (pl.col('holder') == f_name)
            & (pl.col('symbol') == sym)
        )
        if existing.height == 0:
            continue

        f_date = _to_iso(
            existing['filing_date'].max()
        )
        e_date = _to_iso(row['event_date'])

        if e_date > quarter_end and e_date > f_date:
            updates.append({
                'symbol': sym,
                'filing_date': e_date,
                'holder': f_name,
                'shares': row['shares'],
            })

    if not updates:
        return holdings

    upd = pl.DataFrame(updates, schema={
        'symbol': pl.Utf8,
        'filing_date': pl.Utf8,
        'holder': pl.Utf8,
        'shares': pl.Int64,
    }).select(
        'symbol', 'filing_date',
        'holder', 'shares',
    )

    kept = holdings.join(
        upd.select('holder', 'symbol'),
        on=['holder', 'symbol'],
        how='anti',
    )

    return pl.concat([kept, upd])


def upsert_amendment(
    year: int,
    qtr: int,
    holder: str,
    filing_date: str,
    holdings: list[tuple[str, int]],
) -> None:
    """Upsert a single filer's amendment.

    holdings: list of (symbol, shares) tuples.
    Replaces any existing rows for this holder.
    """
    existing = load_amendments(year, qtr)

    new_rows = pl.DataFrame(
        {
            'symbol': [h[0] for h in holdings],
            'filing_date': [filing_date]
            * len(holdings),
            'holder': [holder] * len(holdings),
            'shares': [h[1] for h in holdings],
        },
        schema={
            'symbol': pl.Utf8,
            'filing_date': pl.Utf8,
            'holder': pl.Utf8,
            'shares': pl.Int64,
        },
    )

    kept = existing.filter(
        pl.col('holder') != holder
    )
    merged = pl.concat([kept, new_rows])

    out = _amends_path(year, qtr)
    HOLDINGS_DIR.mkdir(
        parents=True, exist_ok=True
    )
    merged.write_parquet(out)
    log.info(
        'upserted amendment',
        holder=holder,
        date=filing_date,
        positions=len(holdings),
    )
