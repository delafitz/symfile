"""Confirmed blocks table.

Stores human-reviewed block trades with optional
override fields. Keyed on the same composite key as
the raw trades table for trivial joins.

The interactive review/modification workflow lives
in app.trades.review.

    load_blocks()    — read raw table as DataFrame
    upsert_blocks()  — merge confirmed/rejected rows
    load_confirmed() — confirmed blocks joined with
                       raw trades (for API)
"""

import polars as pl

from app.trades.table import (
    KEY_COLS,
    TABLE_DIR,
    load_trades,
)
from app.util.log import log

TABLE_PATH = TABLE_DIR / 'blocks.parquet'

SCHEMA = {
    # Link key (same as trades table)
    'symbol': pl.Utf8,
    'date_filed': pl.Utf8,
    'filing_type': pl.Utf8,
    'seller': pl.Utf8,
    'shares': pl.Int64,
    # Override fields (null = use raw value)
    'notional': pl.Float64,
    'tx_price': pl.Float64,
    'offer_price': pl.Float64,
    'pricing_date': pl.Utf8,
    'trade_date': pl.Utf8,
    'seller_name': pl.Utf8,
    'banks': pl.List(pl.Utf8),
    # Block-specific fields
    'is_primary': pl.Boolean,
    # Metadata
    'status': pl.Utf8,
    'reviewed_at': pl.Utf8,
    'source': pl.Utf8,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_blocks() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    df = pl.read_parquet(TABLE_PATH)
    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(dtype).alias(col)
            )
    return df.select(list(SCHEMA.keys()))


def _save(df: pl.DataFrame) -> None:
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(TABLE_PATH)


def upsert_blocks(rows: list[dict]) -> int:
    """Merge confirmed/rejected block rows.

    Returns count of net-new rows added.
    """
    if not rows:
        return 0

    new_df = pl.DataFrame(rows, schema=SCHEMA)
    existing = load_blocks()

    keys = new_df.select(KEY_COLS).unique()
    kept = existing.join(
        keys, on=KEY_COLS, how='anti',
    )

    merged = pl.concat([kept, new_df])
    _save(merged)

    added = merged.height - existing.height
    log.info(
        'upsert blocks',
        new=len(rows),
        added=added,
        total=merged.height,
    )
    return added


def load_confirmed() -> pl.DataFrame:
    """Load confirmed blocks joined with raw trades.

    Override fields from blocks take precedence;
    falls back to raw trade values.
    """
    blocks = load_blocks().filter(
        pl.col('status') == 'confirmed'
    )
    if blocks.height == 0:
        return blocks

    trades = load_trades()

    joined = blocks.join(
        trades, on=KEY_COLS, how='left',
    )

    return joined.select(
        'symbol',
        'date_filed',
        'filing_type',
        pl.coalesce(
            'seller_name', 'seller',
        ).alias('seller'),
        'shares',
        pl.coalesce(
            'notional', 'implied_value',
        ).alias('notional'),
        pl.col('tx_price'),
        pl.coalesce(
            'offer_price', 'price',
        ).alias('offer_price'),
        pl.coalesce(
            'pricing_date', 'date_filed',
        ).alias('pricing_date'),
        pl.coalesce(
            'trade_date', 'trade_date_right',
        ).alias('trade_date'),
        pl.col('price_source'),
        pl.col('relationship'),
        'banks',
        pl.col('is_primary')
        .fill_null(False)
        .alias('is_primary'),
        pl.col('is_ipo'),
        pl.col('lockup'),
        pl.col('lockup_days'),
        'reviewed_at',
        'source',
    )
