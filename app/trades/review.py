"""Block-trade review workflow.

Interactive CLI for confirming / rejecting / editing
flagged trades into the blocks table.

    review_trades()  — interactive CLI review loop
"""

from datetime import datetime

import polars as pl

from app.holdings.form4 import load_form4
from app.trades.banks import parse_banks
from app.trades.blocks import load_blocks, upsert_blocks
from app.trades.table import KEY_COLS, load_trades

# Max |filing_date - txn_date| for Form 4 match
FORM4_MATCH_DAYS = 30


# --- Review CLI ---


def _fmt_value(v: float) -> str:
    if v >= 1e9:
        return f'${v / 1e9:,.1f}B'
    return f'${v / 1e6:,.1f}M'


def _fmt_shares(s: int) -> str:
    return f'{s:,}'


def _match_form4_price(
    form4: pl.DataFrame,
    symbol: str,
    ref_date: str,
) -> float | None:
    """Return nearest Form 4 sale txn_price for
    symbol within FORM4_MATCH_DAYS of ref_date.

    Picks positive-price S/F-code txns (open-market
    sales); returns the price from the txn whose
    txn_date is closest to ref_date.
    """
    if form4.height == 0 or not ref_date:
        return None
    try:
        ref = datetime.fromisoformat(ref_date).date()
    except ValueError:
        return None

    candidates = form4.filter(
        (pl.col('symbol') == symbol)
        & (pl.col('txn_code').is_in(['S', 'F']))
        & (pl.col('txn_price') > 0)
    )
    if candidates.height == 0:
        return None

    best_price: float | None = None
    best_delta = FORM4_MATCH_DAYS + 1
    for row in candidates.to_dicts():
        try:
            td = datetime.fromisoformat(
                row['txn_date']
            ).date()
        except (ValueError, TypeError):
            continue
        delta = abs((td - ref).days)
        if (
            delta <= FORM4_MATCH_DAYS
            and delta < best_delta
        ):
            best_delta = delta
            best_price = row['txn_price']
    return best_price


def review_trades() -> None:
    """Interactive review of unreviewed flagged trades."""
    trades = load_trades()
    blocks = load_blocks()
    form4 = load_form4()

    # Flagged trades not yet in blocks table
    flagged = trades.filter(
        pl.col('flagged_block') == True  # noqa: E712
    )
    if blocks.height > 0:
        pending = flagged.join(
            blocks.select(KEY_COLS),
            on=KEY_COLS,
            how='anti',
        )
    else:
        pending = flagged

    pending = pending.sort('date_filed')

    if pending.height == 0:
        print('no trades to review')
        return

    print(f'{pending.height} trades to review\n')
    reviewed = 0

    for i, row in enumerate(pending.to_dicts()):
        sym = row['symbol']
        dt = row['date_filed']
        ft = row['filing_type']
        shr = row['shares']
        val = row['implied_value']
        px = row['price']
        px_src = row['price_source']
        seller = row['seller']
        rel = row['relationship']
        uw = row['underwriter'] or ''
        cap = row['mkt_cap']
        tdate = row.get('trade_date', '') or ''

        banks = parse_banks(uw)
        banks_str = ', '.join(banks) if banks else uw

        # Look up matching Form 4 execution price
        match_tx_price = _match_form4_price(
            form4, sym, tdate or dt,
        )

        print(
            f'[{i + 1}/{pending.height}] '
            f'{sym}  {dt}  {ft}  '
            f'{_fmt_shares(shr)} shr  '
            f'{_fmt_value(val)}'
        )
        print(
            f'  Seller: {seller} ({rel})  '
            f'Banks: {banks_str}'
        )
        px_line = (
            f'  Price: ${px:,.2f} ({px_src})  '
            f'Mkt Cap: {_fmt_value(cap)}'
        )
        if match_tx_price is not None:
            px_line += (
                f'  Form4 tx: ${match_tx_price:,.2f}'
            )
        if tdate:
            px_line += f'  Trade: {tdate}'
        print(px_line)

        while True:
            choice = input(
                '  [c]onfirm  [r]eject  '
                '[e]dit  [s]kip  [q]uit > '
            ).strip().lower()

            if choice == 'q':
                print(f'\n{reviewed} reviewed')
                return

            if choice == 's':
                print()
                break

            if choice == 'r':
                upsert_blocks([{
                    'symbol': sym,
                    'date_filed': dt,
                    'filing_type': ft,
                    'seller': seller,
                    'shares': shr,
                    'notional': None,
                    'tx_price': None,
                    'offer_price': None,
                    'pricing_date': None,
                    'trade_date': None,
                    'seller_name': None,
                    'banks': banks,
                    'is_primary': None,
                    'status': 'rejected',
                    'reviewed_at': datetime.now()
                    .isoformat(timespec='seconds'),
                    'source': 'review',
                }])
                reviewed += 1
                print()
                break

            if choice == 'c':
                upsert_blocks([{
                    'symbol': sym,
                    'date_filed': dt,
                    'filing_type': ft,
                    'seller': seller,
                    'shares': shr,
                    'notional': None,
                    'tx_price': match_tx_price,
                    'offer_price': None,
                    'pricing_date': None,
                    'trade_date': None,
                    'seller_name': None,
                    'banks': banks,
                    'is_primary': (
                        False if ft == '144' else None
                    ),
                    'status': 'confirmed',
                    'reviewed_at': datetime.now()
                    .isoformat(timespec='seconds'),
                    'source': 'review',
                }])
                reviewed += 1
                print()
                break

            if choice == 'e':
                overrides = _edit_trade(
                    shr, val, px, seller, banks,
                    tdate, match_tx_price, ft, dt,
                )
                upsert_blocks([{
                    'symbol': sym,
                    'date_filed': dt,
                    'filing_type': ft,
                    'seller': seller,
                    'shares': shr,
                    **overrides,
                    'status': 'confirmed',
                    'reviewed_at': datetime.now()
                    .isoformat(timespec='seconds'),
                    'source': 'review',
                }])
                reviewed += 1
                print()
                break

    print(f'\n{reviewed} reviewed')


def _edit_trade(
    shares: int,
    notional: float,
    price: float,
    seller: str,
    banks: list[str],
    trade_date: str = '',
    tx_price: float | None = None,
    filing_type: str = '',
    date_filed: str = '',
) -> dict:
    """Sub-menu for editing trade fields."""
    is_reg = filing_type and filing_type != '144'
    out_notional = None
    out_offer_price = None
    out_tx_price = tx_price
    out_pricing_date = None
    out_trade_date = None
    out_seller = None
    out_banks = banks
    out_is_primary = False if not is_reg else None
    offer_price = price
    pricing_date = date_filed

    while True:
        tx_str = (
            f'${out_tx_price:,.2f}'
            if out_tx_price is not None
            else '—'
        )
        print(
            f'    shares={_fmt_shares(shares)}  '
            f'notional={_fmt_value(notional)}  '
            f'offer=${offer_price:,.2f}  '
            f'tx={tx_str}'
        )
        line2 = f'    seller={seller}  banks={", ".join(out_banks)}'
        if pricing_date:
            line2 += f'  pricing={pricing_date}'
        if trade_date:
            line2 += f'  trade={trade_date}'
        if is_reg:
            prim_str = (
                '?' if out_is_primary is None
                else ('yes' if out_is_primary else 'no')
            )
            line2 += f'  primary={prim_str}'
        print(line2)

        prompt = (
            '    [n]otional  [o]ffer  [x]tx  '
            '[g]pricing_date  [t]rade_date  '
            '[s]eller  [b]anks'
        )
        if is_reg:
            prompt += '  [m]primary'
        prompt += '  [d]one > '

        c = input(prompt).strip().lower()

        if c == 'd':
            break

        if c == 'n':
            v = input('    notional ($M): ').strip()
            if v:
                try:
                    out_notional = float(v) * 1e6
                    notional = out_notional
                except ValueError:
                    print('    invalid number')

        elif c == 'o' or c == 'p':
            v = input('    offer price: $').strip()
            if v:
                try:
                    out_offer_price = float(v)
                    offer_price = out_offer_price
                except ValueError:
                    print('    invalid number')

        elif c == 'x':
            v = input('    tx price: $').strip()
            if v:
                try:
                    out_tx_price = float(v)
                except ValueError:
                    print('    invalid number')

        elif c == 'g':
            v = input(
                '    pricing date (YYYY-MM-DD): '
            ).strip()
            if v:
                out_pricing_date = v
                pricing_date = v

        elif c == 't':
            v = input(
                '    trade date (YYYY-MM-DD): '
            ).strip()
            if v:
                out_trade_date = v
                trade_date = v

        elif c == 's':
            v = input('    seller name: ').strip()
            if v:
                out_seller = v
                seller = v

        elif c == 'b':
            v = input(
                '    banks (comma-sep): '
            ).strip()
            if v:
                out_banks = [
                    b.strip() for b in v.split(',')
                    if b.strip()
                ]

        elif c == 'm' and is_reg:
            v = input(
                '    primary offering? [y/n]: '
            ).strip().lower()
            if v in ('y', 'yes'):
                out_is_primary = True
            elif v in ('n', 'no'):
                out_is_primary = False

    return {
        'notional': out_notional,
        'tx_price': out_tx_price,
        'offer_price': out_offer_price,
        'pricing_date': out_pricing_date,
        'trade_date': out_trade_date,
        'seller_name': out_seller,
        'banks': out_banks,
        'is_primary': out_is_primary,
    }
