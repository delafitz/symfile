"""Holdings reports for individual symbols."""

import polars as pl

from symfile.holdings.load import load_with_syms
from symfile.mds.syms import load_cusips, load_syms


def top_holders(
    symbol: str,
    quarters: list[tuple[int, int]] | None = None,
    n: int = 20,
) -> None:
    """Print top N holders of a symbol with
    quarter-over-quarter change."""
    if quarters is None:
        quarters = [(2025, 3), (2025, 4)]

    syms = load_syms()
    cusip_map = load_cusips()
    ref = syms.get(symbol)
    if not ref:
        print(f'{symbol} not in universe')
        return

    prev_y, prev_q = quarters[0]
    curr_y, curr_q = quarters[1]

    prev = load_with_syms(
        prev_y, prev_q, cusip_map
    )
    curr = load_with_syms(
        curr_y, curr_q, cusip_map
    )

    def agg_sym(df, sym):
        return (
            df.filter(
                (pl.col('symbol') == sym)
                & (pl.col('sh_type') == 'SH')
                & (pl.col('put_call') == '')
            )
            .group_by('filer')
            .agg(
                pl.col('shares').sum(),
                pl.col('value').sum(),
                pl.col('sole').sum(),
                pl.col('shared').sum(),
                pl.col('none_auth').sum(),
            )
        )

    c = agg_sym(curr, symbol)
    p = agg_sym(prev, symbol)

    merged = c.join(
        p.select(
            'filer',
            pl.col('shares').alias('prev_shares'),
            pl.col('value').alias('prev_value'),
        ),
        on='filer',
        how='left',
    ).with_columns(
        pl.col('prev_shares').fill_null(0),
        pl.col('prev_value').fill_null(0),
        (
            pl.col('value').cast(pl.Float64)
            * 1000
        ).alias('value_usd'),
        (
            pl.col('prev_value').cast(pl.Float64)
            * 1000
        ).alias('prev_value_usd'),
        (
            pl.col('shares')
            - pl.col('prev_shares').fill_null(0)
        ).alias('shares_chg'),
    )

    top = merged.sort('value', descending=True).head(
        n
    )

    cap_b = ref.mkt_cap / 1e9
    price = ref.price
    print(
        f'\n{symbol} — {ref.name}'
        f'  |  mkt_cap=${cap_b:.0f}B'
        f'  |  price=${price:.2f}'
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
        filer = row['filer'][:39]
        sh_mm = row['shares'] / 1e6
        chg_mm = row['shares_chg'] / 1e6
        new = row['prev_shares'] == 0

        chg_str = (
            'NEW'
            if new
            else f'{chg_mm:>+10.1f}'
        )
        print(
            f'{filer:<40s} '
            f'{sh_mm:>10.1f} '
            f'{chg_str:>10s}'
        )

    total_mm = c['shares'].sum() / 1e6
    prev_total_mm = p['shares'].sum() / 1e6
    chg_mm = total_mm - prev_total_mm
    print('-' * 64)
    print(
        f'{"ALL (" + str(c.height) + " holders)":<40s} '
        f'{total_mm:>10.1f} '
        f'{chg_mm:>+10.1f}'
    )
