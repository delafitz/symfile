"""Holdings reports for individual symbols."""

import polars as pl

from symfile.holdings.build import (
    load_quarter_parquet,
)
from symfile.mds.syms import load_syms


def top_holders(
    symbol: str,
    quarters: list[tuple[int, int]]
    | None = None,
    n: int = 20,
) -> None:
    """Print top N holders with QoQ change."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]

    syms = load_syms()
    ref = syms.get(symbol)
    if not ref:
        print(f'{symbol} not in universe')
        return

    prev_y, prev_q = quarters[0]
    curr_y, curr_q = quarters[1]

    prev = load_quarter_parquet(prev_y, prev_q)
    curr = load_quarter_parquet(curr_y, curr_q)

    def agg_sym(df, sym):
        return (
            df.filter(pl.col('symbol') == sym)
            .group_by('holder')
            .agg(pl.col('shares').sum())
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
        (
            pl.col('shares')
            - pl.col('prev_shares').fill_null(0)
        ).alias('chg'),
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
    print(
        f'{"HOLDER":<40s} '
        f'{"POS(MM)":>10s} '
        f'{"CHG(MM)":>10s}'
    )
    print('-' * 64)

    for row in top.iter_rows(named=True):
        holder = row['holder'][:39]
        pos = row['shares'] / 1e6
        chg = row['chg'] / 1e6
        new = row['prev_shares'] == 0

        chg_str = (
            'NEW'
            if new
            else f'{chg:>+10.1f}'
        )
        print(
            f'{holder:<40s} '
            f'{pos:>10.1f} '
            f'{chg_str:>10s}'
        )

    total = c['shares'].sum() / 1e6
    prev_total = p['shares'].sum() / 1e6
    chg = total - prev_total
    print('-' * 64)
    print(
        f'{"ALL (" + str(c.height) + " holders)":<40s} '
        f'{total:>10.1f} '
        f'{chg:>+10.1f}'
    )
