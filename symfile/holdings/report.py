"""Holdings reports for individual symbols."""

import polars as pl

from symfile.holdings.build import (
    load_effective,
)
from symfile.mds.syms import load_syms


def _shares_out(ref) -> int:
    if ref.price > 0:
        return int(ref.mkt_cap / ref.price)
    return 0


def top_holders(
    symbol: str,
    quarters: list[tuple[int, int]]
    | None = None,
    n: int = 20,
) -> None:
    """Top N holders with QoQ change + pct
    outstanding. Shows filing date per holder."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]

    syms = load_syms()
    ref = syms.get(symbol)
    if not ref:
        print(f'{symbol} not in universe')
        return

    prev_y, prev_q = quarters[0]
    curr_y, curr_q = quarters[1]

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
        pl.when(pl.col('base_shares').is_not_null())
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
    print(
        f'{"HOLDER":<35s} '
        f'{"DATE":>10s} '
        f'{"POS(MM)":>10s} '
        f'{"%OUT":>6s} '
        f'{"CHG(MM)":>10s}'
    )
    print('-' * 75)

    for row in top.iter_rows(named=True):
        holder = row['holder'][:34]
        pos = row['shares'] / 1e6
        chg = row['chg'] / 1e6
        pct = (
            row['shares'] / so * 100
            if so > 0
            else 0
        )
        new = (
            row['prev_shares'] == 0
            and not row['is_13d']
        )
        if new:
            chg_str = 'NEW'
        elif row['is_13d']:
            chg_str = f'{chg:>+8.1f}*'
        else:
            chg_str = f'{chg:>+10.1f}'
        print(
            f'{holder:<35s} '
            f'{row["filing_date"]:>10s} '
            f'{pos:>10.1f} '
            f'{pct:>5.1f}%'
            f'{chg_str:>10s}'
        )

    total = c['shares'].sum()
    prev_total = p['shares'].sum()
    chg = (total - prev_total) / 1e6
    pct = total / so * 100 if so > 0 else 0
    print('-' * 75)
    print(
        f'{"ALL (" + str(c.height) + " holders)":<35s} '
        f'{"":>10s} '
        f'{total / 1e6:>10.1f} '
        f'{pct:>5.1f}%'
        f'{chg:>+10.1f}'
    )

    _print_movers(merged, n)


def _print_movers(
    merged: pl.DataFrame, n: int
) -> None:
    """Print top adds and subtracts."""
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

    if adds.height > 0:
        print(f'\nTop adds:')
        print(
            f'{"HOLDER":<35s} '
            f'{"DATE":>10s} '
            f'{"CHG(MM)":>10s}'
        )
        print('-' * 58)
        for row in adds.iter_rows(named=True):
            chg = row['chg'] / 1e6
            new = row['prev_shares'] == 0
            tag = ' NEW' if new else ''
            print(
                f'{row["holder"][:34]:<35s} '
                f'{row["filing_date"]:>10s} '
                f'{chg:>+10.1f}'
                f'{tag}'
            )

    if subs.height > 0:
        print(f'\nTop subtracts:')
        print(
            f'{"HOLDER":<35s} '
            f'{"DATE":>10s} '
            f'{"CHG(MM)":>10s}'
        )
        print('-' * 58)
        for row in subs.iter_rows(named=True):
            chg = row['chg'] / 1e6
            exit = row['shares'] == 0
            tag = ' EXIT' if exit else ''
            print(
                f'{row["holder"][:34]:<35s} '
                f'{row["filing_date"]:>10s} '
                f'{chg:>+10.1f}'
                f'{tag}'
            )
