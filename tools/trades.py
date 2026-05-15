"""CLI tool: view recent block trades.

Usage:
    uv run python tools/trades.py
    uv run python tools/trades.py AAPL
    uv run python tools/trades.py --blocks
    uv run python tools/trades.py AAPL --count 50 --from 2025-01-01
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent)
)

import httpx
from rich.console import Console
from rich.table import Table

from app.server.schemas import (
    BlocksResponse,
    TradesResponse,
)

BASE = 'http://localhost:8000'
console = Console()


def fetch_trades(
    symbol: str | None,
    count: int,
    from_date: str | None,
) -> TradesResponse:
    params: dict = {'count': count}
    if symbol:
        params['symbol'] = symbol
    if from_date:
        params['from_date'] = from_date
    r = httpx.get(f'{BASE}/trades', params=params)
    r.raise_for_status()
    return TradesResponse.model_validate(r.json())


def fetch_blocks(
    symbol: str | None,
    count: int,
    from_date: str | None,
) -> BlocksResponse:
    params: dict = {'count': count}
    if symbol:
        params['symbol'] = symbol
    if from_date:
        params['from_date'] = from_date
    r = httpx.get(f'{BASE}/blocks', params=params)
    r.raise_for_status()
    return BlocksResponse.model_validate(r.json())


def _flags(
    is_block: bool, is_ipo: bool, lockup: bool,
    lockup_days: int,
) -> str:
    parts = []
    if is_block:
        parts.append('[yellow]BLK[/yellow]')
    if is_ipo:
        parts.append('[cyan]IPO[/cyan]')
    if lockup:
        parts.append(
            f'[magenta]LU{lockup_days}[/magenta]'
            if lockup_days
            else '[magenta]LU[/magenta]'
        )
    return ' '.join(parts)


def display_trades(
    resp: TradesResponse,
    symbol: str | None,
) -> None:
    title = (
        f'Recent trades — {symbol}'
        if symbol else 'Recent trades'
    )
    console.print(
        f'\n[bold]{title}[/bold]  '
        f'[dim](showing {len(resp.trades)} '
        f'of {resp.total})[/dim]\n'
    )

    tbl = Table(show_lines=False, pad_edge=False)
    tbl.add_column('date')
    if not symbol:
        tbl.add_column('sym')
    tbl.add_column('type')
    tbl.add_column('shares', justify='right')
    tbl.add_column('value', justify='right')
    tbl.add_column('price', justify='right')
    tbl.add_column('mkt cap', justify='right')
    tbl.add_column('seller', max_width=28)
    tbl.add_column('rel', max_width=10)
    tbl.add_column('flags')

    for t in resp.trades:
        row = [t.date_filed]
        if not symbol:
            row.append(t.symbol)
        row.extend([
            t.filing_type,
            f'{t.shares / 1e6:.2f}M',
            f'${t.implied_value_mm:,.1f}M',
            f'${t.price:,.2f}',
            f'${t.mkt_cap_b:,.1f}B',
            t.seller,
            t.relationship,
            _flags(
                t.flagged_block, t.is_ipo,
                t.lockup, t.lockup_days,
            ),
        ])
        tbl.add_row(*row)

    console.print(tbl)


def display_blocks(
    resp: BlocksResponse,
    symbol: str | None,
) -> None:
    title = (
        f'Confirmed blocks — {symbol}'
        if symbol else 'Confirmed blocks'
    )
    console.print(
        f'\n[bold]{title}[/bold]  '
        f'[dim](showing {len(resp.blocks)} '
        f'of {resp.total})[/dim]\n'
    )

    tbl = Table(show_lines=False, pad_edge=False)
    tbl.add_column('filed')
    tbl.add_column('priced')
    tbl.add_column('trade')
    if not symbol:
        tbl.add_column('sym')
    tbl.add_column('type')
    tbl.add_column('shares', justify='right')
    tbl.add_column('notional', justify='right')
    tbl.add_column('offer', justify='right')
    tbl.add_column('seller', max_width=24)
    tbl.add_column('banks', max_width=28)
    tbl.add_column('flags')

    for b in resp.blocks:
        row = [
            b.date_filed,
            b.pricing_date or '—',
            b.trade_date or '—',
        ]
        if not symbol:
            row.append(b.symbol)
        flag_parts = []
        if b.is_reg:
            flag_parts.append('[blue]REG[/blue]')
        if b.is_primary:
            flag_parts.append('[green]PRIM[/green]')
        if b.is_ipo:
            flag_parts.append('[cyan]IPO[/cyan]')
        if b.lockup:
            flag_parts.append(
                f'[magenta]LU{b.lockup_days}[/magenta]'
                if b.lockup_days
                else '[magenta]LU[/magenta]'
            )
        row.extend([
            b.filing_type,
            f'{b.shares / 1e6:.2f}M',
            f'${b.notional_mm:,.1f}M',
            f'${b.offer_price:,.2f}'
            if b.offer_price else '—',
            b.seller,
            ', '.join(b.banks),
            ' '.join(flag_parts),
        ])
        tbl.add_row(*row)

    console.print(tbl)


def lookup(
    symbol: str | None,
    count: int,
    from_date: str | None,
    blocks: bool,
) -> None:
    try:
        if blocks:
            resp = fetch_blocks(
                symbol, count, from_date,
            )
            display_blocks(resp, symbol)
        else:
            resp = fetch_trades(
                symbol, count, from_date,
            )
            display_trades(resp, symbol)
    except httpx.ConnectError:
        console.print(
            '[red]server not running[/red] '
            '— start with: uv run python -m app serve'
        )
        raise SystemExit(1)
    except httpx.HTTPStatusError as e:
        console.print(f'[red]{e}[/red]')


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='trades',
        description='View recent block trades.',
    )
    p.add_argument(
        'symbol', nargs='?', default=None,
        help='Filter to a ticker symbol.',
    )
    p.add_argument(
        '--count', '-n', type=int, default=25,
        help='Rows to show (default 25).',
    )
    p.add_argument(
        '--from', dest='from_date', default=None,
        help='Earliest filing date (YYYY-MM-DD).',
    )
    p.add_argument(
        '--blocks', action='store_true',
        help='Show confirmed blocks instead of trades.',
    )
    return p


def _parse(argv: list[str]) -> argparse.Namespace:
    ns = _build_parser().parse_args(argv)
    if ns.symbol:
        ns.symbol = ns.symbol.upper()
    return ns


def main() -> None:
    parser = _build_parser()

    # one-shot mode from argv
    if len(sys.argv) > 1:
        ns = parser.parse_args(sys.argv[1:])
        if ns.symbol:
            ns.symbol = ns.symbol.upper()
        lookup(
            ns.symbol, ns.count,
            ns.from_date, ns.blocks,
        )
        return

    # interactive loop — enter args same as one-shot,
    # blank line shows latest trades across universe
    console.print(
        '[dim]enter: [SYMBOL] [--blocks] '
        '[--count N] [--from YYYY-MM-DD]  '
        '(q to quit)[/dim]'
    )
    while True:
        try:
            raw = console.input(
                '[bold]trades>[/bold] '
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if raw.upper() in ('Q', 'QUIT', 'EXIT'):
            break
        try:
            ns = _parse(raw.split())
        except SystemExit:
            continue
        lookup(
            ns.symbol, ns.count,
            ns.from_date, ns.blocks,
        )


if __name__ == '__main__':
    main()
