"""Synthesize Trade records from Form 4 sales.

Some block trades (sponsor exits, strategic sales) are
reported only via Form 4, not 144 or registered offerings.
This module promotes qualifying Form 4 sales into the
trades table with filing_type='4' and flagged_block=True.

Heuristic mirrors _flag_144_block but without the broker
gate (Form 4 doesn't carry an underwriter):

  - txn_code == 'S'  (open-market sale)
  - notional >= MIN_144_VALUE ($25M)
  - shares/outstanding >= 1%  OR  shares/ADV >= 50%

The size-relative gate filters megacap insider noise
(e.g., a $30M sale by an INTU officer is < 0.01% of
float; a $30M Warburg exit can be 5%+).
"""

from datetime import datetime, timedelta

import polars as pl

from app.holdings.form4 import load_form4
from app.mds.massive.refs import RefRow
from app.trades.hist import (
    BLOCK_PCT_ADV,
    BLOCK_PCT_OUTSTANDING,
    MIN_144_VALUE,
    Trade,
)
from app.trades.table import load_trades
from app.util.log import log

# Same-deal match window vs existing 144/reg
DEDUP_TOL_DAYS = 5
DEDUP_TOL_PCT = 0.05


def _flag_form4_block(
    shares: int,
    price: float,
    ref: RefRow,
) -> bool:
    notional = shares * price
    if notional < MIN_144_VALUE:
        return False
    outstanding = (
        ref.mkt_cap / ref.price
        if ref.price > 0 else 0
    )
    if outstanding > 0 and (
        shares / outstanding >= BLOCK_PCT_OUTSTANDING
    ):
        return True
    if ref.adv > 0 and (
        shares / ref.adv >= BLOCK_PCT_ADV
    ):
        return True
    return False


def _has_matching_trade(
    non_f4: pl.DataFrame,
    symbol: str,
    shares: int,
    filing_date: str,
    txn_date: str,
) -> bool:
    """Is there a 144/reg in trades.parquet that
    plausibly represents the same deal?"""
    if shares <= 0:
        return False
    ref_dates = []
    for s in (filing_date, txn_date):
        try:
            ref_dates.append(
                datetime.fromisoformat(s).date()
            )
        except (ValueError, TypeError):
            continue
    if not ref_dates:
        return False
    cands = non_f4.filter(
        pl.col('symbol') == symbol
    ).to_dicts()
    for r in cands:
        try:
            d = datetime.fromisoformat(
                r['date_filed']
            ).date()
        except (ValueError, TypeError):
            continue
        if not any(
            abs((d - rd).days) <= DEDUP_TOL_DAYS
            for rd in ref_dates
        ):
            continue
        if (
            abs(r['shares'] - shares) / shares
            <= DEDUP_TOL_PCT
        ):
            return True
    return False


def build_form4_trades(
    syms: dict[str, RefRow],
) -> list[Trade]:
    """Scan form4.parquet for qualifying block sales.

    Dedupes joint filings (same symbol/date/shares/price
    by multiple reporters) into one Trade. Skips any
    candidate that already has a matching 144/reg row
    in trades.parquet (the 144/reg version is richer —
    has underwriter and offer price).
    """
    df = load_form4()
    df = df.filter(
        (pl.col('txn_code') == 'S')
        & (pl.col('shares_txn') > 0)
        & (pl.col('txn_price') > 0)
    )
    if df.height == 0:
        return []

    grouped = df.group_by(
        'symbol', 'txn_date',
        'shares_txn', 'txn_price',
    ).agg(
        pl.col('reporter').sort().alias('reporters'),
        pl.col('filing_date').max(),
    )

    existing = load_trades()
    non_f4 = existing.filter(
        pl.col('filing_type') != '4'
    )

    trades: list[Trade] = []
    skipped_no_ref = 0
    skipped_dupe = 0
    for r in grouped.to_dicts():
        ref = syms.get(r['symbol'])
        if not ref:
            skipped_no_ref += 1
            continue
        shares = r['shares_txn']
        price = r['txn_price']
        if not _flag_form4_block(shares, price, ref):
            continue
        if _has_matching_trade(
            non_f4, r['symbol'], shares,
            r['filing_date'], r['txn_date'],
        ):
            skipped_dupe += 1
            continue
        seller = ' / '.join(r['reporters'])
        notional = shares * price
        outstanding = (
            ref.mkt_cap / ref.price
            if ref.price > 0 else 0
        )
        pct = (
            shares / outstanding
            if outstanding > 0 else 0.0
        )
        trades.append(Trade(
            symbol=r['symbol'],
            date_filed=r['filing_date'],
            shares=shares,
            implied_value=notional,
            price=price,
            price_source='form4',
            filing_type='4',
            seller=seller,
            relationship='',
            underwriter='',
            mkt_cap=ref.mkt_cap,
            flagged_block=True,
            trade_date=r['txn_date'],
            pct_outstanding=pct,
        ))
    log.info(
        'form4 trade scan',
        candidates=grouped.height,
        flagged=len(trades),
        skipped_dupe=skipped_dupe,
        no_ref=skipped_no_ref,
    )
    return trades
