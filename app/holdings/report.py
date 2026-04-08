"""Holdings reports for individual symbols."""

import polars as pl

from app.holdings.build import (
    load_effective,
)
from app.mds.syms import load_syms
from app.util.names import short_name


def _shares_out(ref) -> int:
    if ref.price > 0:
        return int(ref.mkt_cap / ref.price)
    return 0


def _fmt_chg(chg, is_13d, is_new):
    if is_new:
        return 'NEW'
    if is_13d:
        return f'{chg:+.2f}*'
    return f'{chg:+.2f}'


def _fmt_tag(is_13d, is_new, is_exit):
    if is_13d:
        return '13D'
    if is_new:
        return 'NEW'
    if is_exit:
        return 'EXIT'
    return ''


def top_holders(
    symbol: str,
    quarters: list[tuple[int, int]]
    | None = None,
    n: int = 20,
) -> None:
    """Top N holders with QoQ change."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]

    syms = load_syms()
    ref = syms.get(symbol)
    if not ref:
        print(f'{symbol} not in universe')
        return

    prev_y, prev_q = quarters[0]
    curr_y, curr_q = quarters[1]
    qtr_label = f'Q{curr_q} {curr_y}'

    prev = load_effective(prev_y, prev_q)
    curr = load_effective(curr_y, curr_q)

    so = _shares_out(ref)

    def agg_sym(df, sym):
        return (
            df.filter(pl.col('symbol') == sym)
            .group_by('holder')
            .agg(
                pl.col('shares').sum(),
                pl.col('filing_date').max(),
                pl.col('base_shares').first(),
            )
        )

    c = agg_sym(curr, symbol)
    p = agg_sym(prev, symbol)

    merged = c.join(
        p.select(
            'holder',
            pl.col('shares').alias(
                'prev_shares'
            ),
        ),
        on='holder',
        how='left',
    ).with_columns(
        pl.col('prev_shares').fill_null(0),
        pl.when(
            pl.col('base_shares').is_not_null()
        )
        .then(
            pl.col('shares')
            - pl.col('base_shares')
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

    top = merged.sort(
        'shares', descending=True
    ).head(n)

    cap_b = ref.mkt_cap / 1e9
    print(
        f'\n{symbol} — {ref.name}'
        f'  |  mkt_cap=${cap_b:.0f}B'
        f'  |  price=${ref.price:.2f}'
    )
    print(
        f'Top {n} holders Q{curr_q} {curr_y}'
        f' (vs Q{prev_q})'
    )

    tbl = (
        top.with_columns(
            pl.col('holder')
            .map_elements(
                lambda h: short_name(h)[:30],
                return_dtype=pl.Utf8,
            )
            .alias('name'),
            pl.when(pl.col('is_13d'))
            .then(pl.col('filing_date'))
            .otherwise(pl.lit(qtr_label))
            .alias('date'),
            (
                pl.col('shares').cast(pl.Float64)
                / 1e6
            ).alias('pos'),
            (
                pl.col('shares').cast(pl.Float64)
                / so
                * 100
            ).alias('pct'),
            (
                pl.col('chg').cast(pl.Float64)
                / 1e6
            ).alias('chg_mm'),
        )
        .select(
            pl.col('name'),
            pl.col('date'),
            pl.col('pos').round(2),
            pl.col('pct').round(1),
            pl.struct(
                'chg_mm', 'is_13d', 'prev_shares'
            )
            .map_elements(
                lambda s: _fmt_chg(
                    s['chg_mm'],
                    s['is_13d'],
                    s['prev_shares'] == 0
                    and not s['is_13d'],
                ),
                return_dtype=pl.Utf8,
            )
            .alias('chg'),
        )
    )

    with pl.Config(
        tbl_formatting='UTF8_FULL_CONDENSED',
        tbl_hide_dataframe_shape=True,
        tbl_hide_column_data_types=True,
        tbl_hide_column_names=False,
        tbl_rows=n + 5,
        fmt_str_lengths=35,
        fmt_float='full',
        set_tbl_width_chars=80,
    ):
        print(tbl)

    total = c['shares'].sum() / 1e6
    prev_total = p['shares'].sum() / 1e6
    chg = total - prev_total
    pct = total * 1e6 / so * 100 if so else 0
    print(
        f'ALL ({c.height} holders)'
        f'  pos={total:.2f}MM'
        f'  %out={pct:.1f}%'
        f'  chg={chg:+.2f}MM'
    )

    _print_movers(merged, n, qtr_label)


def _print_movers(
    merged: pl.DataFrame,
    n: int,
    qtr_label: str,
) -> None:
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

    def _build_mover_tbl(df):
        return (
            df.with_columns(
                pl.col('holder')
                .map_elements(
                    lambda h: short_name(h)[:30],
                    return_dtype=pl.Utf8,
                )
                .alias('name'),
                pl.when(pl.col('is_13d'))
                .then(pl.col('filing_date'))
                .otherwise(pl.lit(qtr_label))
                .alias('date'),
                (
                    pl.col('chg').cast(pl.Float64)
                    / 1e6
                )
                .round(2)
                .alias('chg_mm'),
                pl.struct(
                    'is_13d', 'prev_shares',
                    'shares',
                )
                .map_elements(
                    lambda s: _fmt_tag(
                        s['is_13d'],
                        s['prev_shares'] == 0
                        and not s['is_13d'],
                        s['shares'] == 0,
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias('tag'),
            )
            .select('name', 'date', 'chg_mm', 'tag')
        )

    with pl.Config(
        tbl_formatting='UTF8_FULL_CONDENSED',
        tbl_hide_dataframe_shape=True,
        tbl_hide_column_data_types=True,
        tbl_hide_column_names=False,
        tbl_rows=n + 5,
        fmt_str_lengths=35,
        fmt_float='full',
        set_tbl_width_chars=80,
    ):
        if adds.height > 0:
            print('\nTop adds:')
            print(_build_mover_tbl(adds))
        if subs.height > 0:
            print('\nTop subtracts:')
            print(_build_mover_tbl(subs))
