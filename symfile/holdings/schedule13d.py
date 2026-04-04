"""Schedule 13D ownership table.

Stores >5% ownership changes from 13D/A filings.
Standalone table — not overlaid on 13F holdings
(holder names don't reliably match).

Schema: symbol, holder, holder_cik, event_date,
        filing_date, shares, pct_class
"""

from pathlib import Path

import polars as pl

from symfile.edgar.parse.schedule13d import (
    Filing13D,
    parse_13d,
)
from symfile.mds import DATA_DIR as MDS_DIR
from symfile.util.log import log

HOLDINGS_DIR = Path(MDS_DIR).parent / 'holdings'
TABLE_PATH = HOLDINGS_DIR / '13d.parquet'

SCHEMA = {
    'symbol': pl.Utf8,
    'holder': pl.Utf8,
    'holder_cik': pl.Utf8,
    'event_date': pl.Utf8,
    'filing_date': pl.Utf8,
    'shares': pl.Int64,
    'pct_class': pl.Float64,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_13d() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    return pl.read_parquet(TABLE_PATH)


def upsert_13d(
    filing_date: str,
    d: Filing13D,
    cusip_map: dict[str, str],
) -> None:
    """Upsert a parsed 13D filing into the table.

    Keyed on (holder_cik, symbol) — latest filing
    per holder+issuer wins.
    """
    sym = cusip_map.get(d.issuer_cusip)
    if not sym:
        return

    existing = load_13d()

    new_row = pl.DataFrame(
        [{
            'symbol': sym,
            'holder': d.holder,
            'holder_cik': d.holder_cik,
            'event_date': d.event_date,
            'filing_date': filing_date,
            'shares': d.shares,
            'pct_class': d.pct_class,
        }],
        schema=SCHEMA,
    )

    kept = existing.filter(
        ~(
            (pl.col('holder_cik') == d.holder_cik)
            & (pl.col('symbol') == sym)
        )
    )

    merged = pl.concat([kept, new_row])
    HOLDINGS_DIR.mkdir(parents=True, exist_ok=True)
    merged.write_parquet(TABLE_PATH)
    log.info(
        'upserted 13d',
        holder=d.holder,
        symbol=sym,
        shares=d.shares,
        pct=d.pct_class,
        event_date=d.event_date,
    )
