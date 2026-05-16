"""One-off: compare blocks.parquet to a curated golden
block-trade list and analyze coverage/precision.

    uv run python tools/compare_blocks_golden.py

Golden source: data/bootstrap/block_trades.20260321.json
"""

import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent)
)

import polars as pl
from rich.console import Console
from rich.table import Table

from app.trades.blocks import load_confirmed
from app.trades.table import load_trades

GOLDEN_PATH = (
    Path(__file__).resolve().parent.parent
    / 'data' / 'bootstrap'
    / 'block_trades.20260321.json'
)

# Golden uses short codes ("BAML", "MS", "GS"). Map to
# our canonical keys for like-for-like comparison.
GOLDEN_BANK_TO_CANON: dict[str, str] = {
    'BAML': 'BAC',
    'BOFA': 'BAC',
    'BOA': 'BAC',
    'JPM': 'JPM',
    'GS': 'GS',
    'MS': 'MS',
    'C': 'C',
    'CITI': 'C',
    'BCS': 'BCS',
    'BARC': 'BCS',
    'UBS': 'UBS',
    'JEF': 'JEF',
    'WFC': 'WFC',
    'WF': 'WFC',
    'RBC': 'RY',
    'DB': 'DB',
    'BMO': 'BMO',
    'BNP': 'BNP',
    'EVR': 'EVR',
    'TD': 'TD',
    'CWN': 'TD',
    'COWEN': 'TD',
    'NMR': 'NMR',
    'MIZ': 'MFG',
    'TFC': 'TFC',
    'PJ': 'PIPR',
    'PIPR': 'PIPR',
    'RJ': 'RJF',
    'RJF': 'RJF',
    'KBW': 'SF',
    'STIF': 'SF',
    'OPCO': 'OPY',
}

console = Console()


def _parse_date(s: str | None) -> str | None:
    """Parse '12-Mar-2026' -> '2026-03-12' ISO."""
    if not s:
        return None
    try:
        return datetime.strptime(
            s, '%d-%b-%Y',
        ).date().isoformat()
    except ValueError:
        return None


def _norm_bank(code: str | None) -> str | None:
    if not code:
        return None
    key = code.strip().upper()
    return GOLDEN_BANK_TO_CANON.get(key, key)


def load_golden() -> pl.DataFrame:
    raw = json.loads(GOLDEN_PATH.read_text())
    rows = []
    for r in raw:
        rows.append({
            'symbol': (r['Ticker'] or '').upper(),
            'trade_date': _parse_date(r.get('TradeDt')),
            'pricing_date': _parse_date(
                r.get('PxDt') or r.get('PriceDt')
            ),
            'shares': r.get('Shares') or 0,
            'offer_price': r.get('OfferPx'),
            'net_price': r.get('NetPx'),
            'discount': r.get('Disc'),
            't1': r.get('T+1'),
            'left_bank': _norm_bank(
                r.get('LeftBank')
            ),
            'type': r.get('Type') or '144',
        })
    return pl.DataFrame(rows)


def _type_matches(
    golden_type: str, blocks_filing_type: str,
) -> bool:
    """Golden 'Reg' ↔ 424B*; '144'/None ↔ '144' or '4'
    (some sponsor blocks are reported only via Form 4
    with no parallel 144 filing)."""
    if golden_type == 'Reg':
        return blocks_filing_type.startswith('424B')
    return blocks_filing_type in ('144', '4')


def find_match(
    blocks: pl.DataFrame,
    g: dict,
    tol_days: int = 1,
) -> dict | None:
    """Match on (symbol, type) with date within
    tol_days of trade_date or pricing_date. Among
    candidates, return the one closest in shares."""
    g_dt = None
    for s in (g['trade_date'], g['pricing_date']):
        if s:
            try:
                g_dt = datetime.fromisoformat(s).date()
                break
            except ValueError:
                continue
    if g_dt is None:
        return None

    candidates = blocks.filter(
        pl.col('symbol') == g['symbol']
    ).to_dicts()
    if not candidates:
        return None

    best = None
    best_share_delta = None
    for r in candidates:
        if not _type_matches(
            g['type'], r['filing_type'],
        ):
            continue
        # Pick closest of trade_date / pricing_date
        # / date_filed within tolerance
        in_window = False
        for k in ('trade_date', 'pricing_date', 'date_filed'):
            v = r.get(k) or ''
            try:
                b_dt = datetime.fromisoformat(v).date()
            except (ValueError, TypeError):
                continue
            if abs((b_dt - g_dt).days) <= tol_days:
                in_window = True
                break
        if not in_window:
            continue

        d = abs(r['shares'] - g['shares'])
        if best is None or d < best_share_delta:
            best = r
            best_share_delta = d
    return best


MIN_NOTIONAL = 50_000_000


def analyze() -> None:
    golden = load_golden()
    blocks = load_confirmed()

    # Pull notional from underlying trade (blocks join
    # uses coalesce(notional, implied_value)).
    blocks = blocks.with_columns(
        (
            pl.col('shares').cast(pl.Float64)
            * pl.col('offer_price')
        ).alias('_notional'),
    )

    g_min = golden['trade_date'].drop_nulls().min()
    g_max = golden['trade_date'].drop_nulls().max()
    console.print(
        f'[bold]Golden:[/bold] {golden.height} rows  '
        f'span {g_min} → {g_max}'
    )
    console.print(
        f'[bold]Blocks:[/bold] {blocks.height} rows '
        f'(confirmed only)'
    )

    # Apply golden's floor: only count blocks >= $50M.
    blocks = blocks.filter(
        pl.col('_notional') >= MIN_NOTIONAL
    )
    console.print(
        f'  >= ${MIN_NOTIONAL/1e6:.0f}M: {blocks.height}'
    )

    # Restrict blocks to golden's date range for fair
    # precision/recall.
    in_range = blocks.filter(
        (pl.col('date_filed') >= g_min)
        & (pl.col('date_filed') <= g_max)
    )
    console.print(
        f'  in golden range: {in_range.height}\n'
    )

    matched: list[tuple[dict, dict]] = []
    missed: list[dict] = []
    matched_block_keys: set[tuple] = set()

    for g in golden.to_dicts():
        if not g['symbol'] or not g['shares']:
            continue
        m = find_match(blocks, g)
        if m:
            matched.append((g, m))
            matched_block_keys.add((
                m['symbol'], m['date_filed'],
                m['filing_type'], m['seller'],
                m['shares'],
            ))
        else:
            missed.append(g)

    # Blocks in range that didn't match anything in golden
    extras = []
    for r in in_range.to_dicts():
        k = (
            r['symbol'], r['date_filed'],
            r['filing_type'], r['seller'],
            r['shares'],
        )
        if k not in matched_block_keys:
            extras.append(r)

    n_g = golden.height
    n_b = in_range.height
    n_m = len(matched)
    recall = n_m / n_g * 100 if n_g else 0
    precision = n_m / n_b * 100 if n_b else 0

    summary = Table(
        title='Coverage', show_header=False,
        pad_edge=False,
    )
    summary.add_column('', style='dim')
    summary.add_column('', justify='right')
    summary.add_row(
        'Matched (golden ∩ blocks)', str(n_m),
    )
    summary.add_row(
        'Missed (in golden, not in blocks)',
        str(len(missed)),
    )
    summary.add_row(
        'Extra (in blocks, not in golden)',
        str(len(extras)),
    )
    summary.add_row(
        'Recall  = matched / golden',
        f'{recall:.1f}%',
    )
    summary.add_row(
        'Precision = matched / blocks_in_range',
        f'{precision:.1f}%',
    )
    console.print(summary)

    # Field-level agreement on matched pairs
    if matched:
        ofr_diffs = []
        bank_match = 0
        share_off = []  # (g, b, pct) for >1% mismatch
        for g, b in matched:
            if g['offer_price'] and b['offer_price']:
                d = abs(
                    g['offer_price'] - b['offer_price']
                )
                ofr_diffs.append(d)
            if g['left_bank'] and b['banks']:
                if g['left_bank'] in b['banks']:
                    bank_match += 1
            if g['shares'] and b['shares']:
                pct = abs(
                    b['shares'] - g['shares']
                ) / g['shares']
                if pct > 0.01:
                    share_off.append((g, b, pct))

        agree = Table(
            title='Field agreement on matched',
            show_header=False, pad_edge=False,
        )
        agree.add_column('', style='dim')
        agree.add_column('', justify='right')
        agree.add_row(
            'shares within 1%',
            f'{n_m - len(share_off)}/{n_m}',
        )
        if ofr_diffs:
            avg = sum(ofr_diffs) / len(ofr_diffs)
            exact = sum(
                1 for d in ofr_diffs if d < 0.01
            )
            agree.add_row(
                'offer_price comparable',
                f'{len(ofr_diffs)}/{n_m}',
            )
            agree.add_row(
                'offer_price exact (Δ<$0.01)',
                f'{exact}/{len(ofr_diffs)}',
            )
            agree.add_row(
                'offer_price avg |Δ|',
                f'${avg:.3f}',
            )
        agree.add_row(
            'LeftBank in blocks.banks',
            f'{bank_match}/{n_m}',
        )
        console.print(agree)

        if share_off:
            so = Table(
                title=(
                    f'Matched but share count off >1% '
                    f'({len(share_off)} rows)'
                ),
                pad_edge=False,
            )
            for c in (
                'symbol', 'trade_date',
                'golden_shr', 'block_shr', 'Δ%',
                'type',
            ):
                so.add_column(c)
            share_off.sort(
                key=lambda x: -x[2],
            )
            for g, b, pct in share_off[:15]:
                so.add_row(
                    g['symbol'],
                    str(g['trade_date']),
                    f"{g['shares']:,}",
                    f"{b['shares']:,}",
                    f'{pct * 100:.1f}%',
                    g['type'],
                )
            console.print(so)

    # Drill into misses: in-trades vs not-in-trades
    if missed:
        trades = load_trades()
        in_trades_unflagged = []
        in_trades_flagged = []  # flagged but not in blocks?!
        not_in_trades = []

        for g in missed:
            cands = trades.filter(
                pl.col('symbol') == g['symbol']
            ).to_dicts()
            hit = None
            for r in cands:
                if not _type_matches(
                    g['type'], r['filing_type'],
                ):
                    continue
                # date ± 1 day on date_filed or trade_date
                try:
                    g_dt = datetime.fromisoformat(
                        g['trade_date']
                    ).date()
                except (ValueError, TypeError):
                    continue
                in_window = False
                for k in ('trade_date', 'date_filed'):
                    v = r.get(k) or ''
                    try:
                        b_dt = datetime.fromisoformat(
                            v
                        ).date()
                    except (ValueError, TypeError):
                        continue
                    if abs((b_dt - g_dt).days) <= 1:
                        in_window = True
                        break
                if in_window:
                    hit = r
                    break

            if hit is None:
                not_in_trades.append(g)
            elif hit['flagged_block']:
                in_trades_flagged.append((g, hit))
            else:
                in_trades_unflagged.append((g, hit))

        miss = Table(
            title='Misses by category',
            show_header=False, pad_edge=False,
        )
        miss.add_column('', style='dim')
        miss.add_column('', justify='right')
        miss.add_row(
            'In trades.parquet, '
            'flagged_block=False (heuristic gap)',
            str(len(in_trades_unflagged)),
        )
        miss.add_row(
            'In trades.parquet, '
            'flagged_block=True (review/upsert gap)',
            str(len(in_trades_flagged)),
        )
        miss.add_row(
            'Not in trades.parquet at all '
            '(parse/filter gap)',
            str(len(not_in_trades)),
        )
        console.print(miss)

        if in_trades_unflagged:
            t = Table(
                title='Heuristic gap (sample 10) — '
                'these are in trades but not flagged',
                pad_edge=False,
            )
            for c in (
                'symbol', 'g_date', 'g_shares',
                't_shares', 't_value_M',
                't_pct_out', 'type', 'underwriter',
            ):
                t.add_column(c)
            for g, r in in_trades_unflagged[:10]:
                t.add_row(
                    g['symbol'],
                    g['trade_date'],
                    f"{g['shares']:,}",
                    f"{r['shares']:,}",
                    f"{r['implied_value']/1e6:.1f}",
                    f"{r['pct_outstanding']*100:.2f}%"
                    if r['pct_outstanding'] else '—',
                    r['filing_type'],
                    (r['underwriter'] or '')[:30],
                )
            console.print(t)

        if not_in_trades:
            t = Table(
                title='Parse/filter gap (sample 10) — '
                'not in trades.parquet',
                pad_edge=False,
            )
            for c in (
                'symbol', 'trade_date', 'shares',
                'offer_price', 'left_bank', 'type',
            ):
                t.add_column(c)
            for g in not_in_trades[:10]:
                t.add_row(
                    g['symbol'],
                    str(g['trade_date']),
                    f"{g['shares']:,}",
                    f"{g['offer_price']}"
                    if g['offer_price'] else '—',
                    g['left_bank'] or '—',
                    g['type'],
                )
            console.print(t)

    # Sample 10 extras (potential false positives)
    if extras:
        ex_types = Counter(
            r['filing_type'] for r in extras
        )
        console.print(
            '\n[yellow]Extras by filing_type:[/yellow]',
            dict(ex_types),
        )
        et = Table(
            title='Sample extras (blocks-only)',
            pad_edge=False,
        )
        for c in (
            'symbol', 'date_filed', 'shares',
            'offer_price', 'banks', 'filing_type',
        ):
            et.add_column(c)
        for r in extras[:10]:
            et.add_row(
                r['symbol'],
                r['date_filed'],
                f"{r['shares']:,}",
                f"${r['offer_price']:.2f}"
                if r['offer_price'] else '—',
                ', '.join(r['banks'] or []),
                r['filing_type'],
            )
        console.print(et)


if __name__ == '__main__':
    analyze()
