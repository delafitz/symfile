"""CLI tool: look up top holders for a symbol.

Usage:
    uv run python tools/holders.py
    uv run python tools/holders.py AAPL
"""

import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent)
)

import httpx
from rich.console import Console
from rich.table import Table

from app.server.schemas import HoldersResponse

BASE = 'http://localhost:8000'
console = Console()


def fetch_holders(
    symbol: str, n: int = 20,
) -> HoldersResponse:
    r = httpx.get(
        f'{BASE}/holders/{symbol}', params={'n': n}
    )
    r.raise_for_status()
    return HoldersResponse.model_validate(r.json())


def display(resp: HoldersResponse) -> None:
    m = resp.meta
    console.print(
        f'\n[bold]{m.symbol}[/bold] — {m.name}'
        f'  |  mkt_cap=${m.mkt_cap_b:.0f}B'
        f'  |  price=${m.price:.2f}'
    )
    console.print(
        f'Top holders {m.quarter}\n',
        style='dim',
    )

    tbl = Table(show_lines=False, pad_edge=False)
    tbl.add_column('holder', max_width=30)
    tbl.add_column('type', justify='center')
    tbl.add_column('date', justify='center')
    tbl.add_column('pos (MM)', justify='right')
    tbl.add_column('%out', justify='right')
    tbl.add_column('chg (MM)', justify='right')
    tbl.add_column('', justify='center')

    for h in resp.holders:
        if h.tag == 'NEW':
            chg_cell = '[green]NEW[/green]'
        elif h.chg_mm > 0:
            chg_cell = f'[green]{h.chg_mm:+.2f}[/green]'
        elif h.chg_mm < 0:
            chg_cell = f'[red]{h.chg_mm:+.2f}[/red]'
        else:
            chg_cell = f'{h.chg_mm:+.2f}'
        tag_cell = (
            f'[yellow]{h.tag}[/yellow]'
            if h.tag else ''
        )
        tbl.add_row(
            h.name,
            h.form_type,
            h.date,
            f'{h.shares_mm:.2f}',
            f'{h.pct_out:.1f}%',
            chg_cell,
            tag_cell,
        )

    console.print(tbl)

    s = resp.summary
    console.print(
        f'\nALL ({s.total_holders} holders)'
        f'  pos={s.total_mm:.2f}MM'
        f'  %out={s.total_pct:.1f}%'
        f'  chg={s.total_chg_mm:+.2f}MM',
        style='dim',
    )

    if resp.adds:
        console.print('\n[green]Top adds:[/]')
        _movers_table(resp.adds)
    if resp.subs:
        console.print('\n[red]Top subtracts:[/]')
        _movers_table(resp.subs)


def _movers_table(rows: list) -> None:
    tbl = Table(show_lines=False, pad_edge=False)
    tbl.add_column('holder', max_width=30)
    tbl.add_column('date', justify='center')
    tbl.add_column('chg (MM)', justify='right')
    tbl.add_column('', justify='center')

    for h in rows:
        style = 'green' if h.chg_mm > 0 else 'red'
        tag_cell = (
            f'[yellow]{h.tag}[/yellow]'
            if h.tag else ''
        )
        tbl.add_row(
            h.name,
            h.date,
            f'[{style}]{h.chg_mm:+.2f}[/{style}]',
            tag_cell,
        )
    console.print(tbl)


def lookup(symbol: str, n: int = 20) -> None:
    try:
        resp = fetch_holders(symbol, n=n)
    except httpx.ConnectError:
        console.print(
            '[red]server not running[/red] '
            '— start with: uv run python -m app serve'
        )
        raise SystemExit(1)
    except httpx.HTTPStatusError as e:
        console.print(f'[red]{e}[/red]')
        return
    display(resp)


def _parse_input(raw: str) -> tuple[str, int]:
    """Parse 'AAPL 5' into (symbol, n)."""
    parts = raw.strip().split()
    symbol = parts[0].upper()
    n = int(parts[1]) if len(parts) > 1 else 20
    return symbol, n


def main() -> None:
    # one-shot mode from argv
    if len(sys.argv) > 1:
        symbol, n = _parse_input(
            ' '.join(sys.argv[1:])
        )
        lookup(symbol, n)
        return

    # interactive loop
    while True:
        try:
            raw = console.input(
                '[bold]symbol:[/bold] '
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not raw:
            continue
        if raw.upper() in ('Q', 'QUIT', 'EXIT'):
            break
        symbol, n = _parse_input(raw)
        lookup(symbol, n)


if __name__ == '__main__':
    main()
