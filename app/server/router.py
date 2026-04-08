"""API routes."""

import polars as pl
from fastapi import APIRouter, Request

from app.server.schemas import (
    HolderRow,
    HoldersResponse,
    HoldersSummary,
    SymbolMeta,
)
from app.util.names import short_name

router = APIRouter()

QUARTERS = [(2025, 3), (2025, 4)]


def _tag(is_13d: bool, is_new: bool, is_exit: bool) -> str:
    if is_13d:
        return '13D'
    if is_new:
        return 'NEW'
    if is_exit:
        return 'EXIT'
    return ''


def _agg_sym(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    return (
        df.filter(pl.col('symbol') == symbol)
        .group_by('holder')
        .agg(
            pl.col('shares').sum(),
            pl.col('filing_date').max(),
            pl.col('base_shares').first(),
            pl.col('form_type').last(),
        )
    )


def _build_holders(
    cache, symbol: str, n: int,
) -> HoldersResponse:
    ref = cache.syms[symbol]
    so = int(ref.mkt_cap / ref.price) if ref.price > 0 else 0

    _, cq = QUARTERS[1]
    _, pq = QUARTERS[0]
    cy, _ = QUARTERS[1]
    qtr_label = f'Q{cq} {cy}'

    c = _agg_sym(cache.curr, symbol)
    p = _agg_sym(cache.prev, symbol)

    merged = c.join(
        p.select(
            'holder',
            pl.col('shares').alias('prev_shares'),
        ),
        on='holder',
        how='left',
    ).with_columns(
        pl.col('prev_shares').fill_null(0),
        pl.when(pl.col('base_shares').is_not_null())
        .then(
            pl.col('shares') - pl.col('base_shares')
        )
        .otherwise(
            pl.col('shares')
            - pl.col('prev_shares').fill_null(0)
        )
        .alias('chg'),
        pl.col('base_shares')
        .is_not_null()
        .alias('is_13d'),
    )

    def to_rows(
        df: pl.DataFrame,
    ) -> list[HolderRow]:
        rows = []
        for r in df.to_dicts():
            is_13d = r['is_13d']
            is_new = (
                r['prev_shares'] == 0 and not is_13d
            )
            is_exit = r['shares'] == 0
            rows.append(
                HolderRow(
                    name=short_name(r['holder']),
                    form_type=r['form_type'],
                    date=(
                        r['filing_date']
                        if is_13d
                        else qtr_label
                    ),
                    shares_mm=round(
                        r['shares'] / 1e6, 2
                    ),
                    pct_out=round(
                        r['shares'] / so * 100, 1
                    )
                    if so
                    else 0,
                    chg_mm=round(r['chg'] / 1e6, 2),
                    tag=_tag(is_13d, is_new, is_exit),
                )
            )
        return rows

    top = merged.sort(
        'shares', descending=True
    ).head(n)

    adds = (
        merged.filter(pl.col('chg') > 0)
        .sort('chg', descending=True)
        .head(n)
    )
    subs = (
        merged.filter(pl.col('chg') < 0)
        .sort('chg')
        .head(n)
    )

    total_mm = c['shares'].sum() / 1e6
    prev_mm = p['shares'].sum() / 1e6

    return HoldersResponse(
        meta=SymbolMeta(
            symbol=symbol,
            name=ref.name,
            mkt_cap_b=round(ref.mkt_cap / 1e9, 1),
            price=ref.price,
            quarter=qtr_label,
        ),
        holders=to_rows(top),
        adds=to_rows(adds),
        subs=to_rows(subs),
        summary=HoldersSummary(
            total_holders=c.height,
            total_mm=round(total_mm, 2),
            total_pct=round(
                total_mm * 1e6 / so * 100, 1
            )
            if so
            else 0,
            total_chg_mm=round(
                total_mm - prev_mm, 2
            ),
        ),
    )


@router.get('/health', tags=['ops'])
def health():
    return {'status': 'ok'}


@router.get(
    '/holders/{symbol}',
    response_model=HoldersResponse,
    tags=['holdings'],
)
async def get_holders(
    symbol: str, request: Request, n: int = 20,
):
    cache = request.state.cache
    symbol = symbol.upper()
    if cache.syms is None or symbol not in cache.syms:
        return {'error': f'{symbol} not in universe'}
    return _build_holders(cache, symbol, n)
