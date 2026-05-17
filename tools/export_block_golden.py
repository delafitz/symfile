"""Export blocks.parquet as block_golden_bootstrap.json
for downstream backtest frameworks (e.g. symtools).

The JSON is self-describing — header carries field
documentation and provenance so consumers don't need
to re-derive the schema.

    uv run python tools/export_block_golden.py
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.trades.blocks import load_blocks  # noqa: E402

OUT_DIR = Path('data/bootstrap')

DOC = (
    'Vetted historical equity block trades. Each row is one '
    'priced deal, uniquely keyed by (price_date, ticker, '
    'offer_price). Sources: SEC 424B/144/Form 4 filings parsed '
    'and reconciled against bootstrap curation, plus a '
    'small legacy-only set of foreign-issuer deals with no '
    'SEC filings.'
)

FIELDS = {
    'ticker':        'Symbol at time of deal (may be a delisted/renamed ticker)',
    'cik':           'Issuer CIK (unpadded). Same CIK can map to multiple historical tickers.',
    'type':          '"Reg" (registered offering, prospectus supplement filed) or '
                     '"Unreg" (block sale by an affiliate, no prospectus).',
    'price_date':    'ISO date the deal was priced / publicly announced.',
    'trade_date':    'ISO date the trade executed. Equal to price_date when intraday=true; '
                     'otherwise the next weekday (after-close announcement, T+1 execution).',
    'intraday':      'true if announced during market hours (same-day execution).',
    'offer_price':   'As-filed public offering price per share (the gross/reoffer price). '
                     'For bought-deal blocks where the filing only states the net to seller, '
                     'this value comes from a manual gross override (block_deals_for_offerpx.csv).',
    'shares':        'As-filed share count (the offering size).',
    'notional':      'shares * offer_price.',
    'split_factor':  'Cumulative split factor between price_date and today. '
                     '>1 = forward split since; <1 = reverse split; =1 = no change.',
    'adj_price':     'Split-adjusted price (today\'s basis): offer_price / split_factor. '
                     'Backtests should use this so cross-deal comparisons stay apples-to-apples.',
    'adj_shares':    'Split-adjusted share count: shares * split_factor.',
    'seller':        'Selling entity name (e.g. issuer name for reg primaries, '
                     'top sponsor/holder for unreg).',
    'relationship':  '"selling stockholder" | "company" | "affiliate" | "insider".',
    'banks':         'Canonical bank codes for bookrunners / lead-left underwriter '
                     '(e.g. "GS", "MS", "JPM", "BAC", "RBC", "BMO", "BCS").',
    'evidence':      'How the row was assembled: '
                     '"golden+parser" (reg cover-extracted), '
                     '"golden+parser+override" (reg with manual gross OfferPx), '
                     '"both" (unreg w/ both 144 and Form 4), '
                     '"144" (unreg, 144 only), '
                     '"form4" (unreg, Form 4 only), '
                     '"none" (unreg golden anchor, no SEC filing in window), '
                     '"legacy_bootstrap" (foreign issuer w/ no SEC filing — '
                     'seeded direct from external dataset).',
    'source':        'Source bootstrap file the row came from.',
}


def _to_jsonable(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def main() -> None:
    blocks = load_blocks()
    print(f'loading {blocks.height} blocks')

    deals = []
    skipped_zero_shares = 0
    for r in blocks.sort(['price_date', 'symbol']).to_dicts():
        # Skip rows with no size — they're golden
        # anchors waiting on an external share count.
        # They stay in blocks.parquet but aren't useful
        # for backtests.
        if not r['shares']:
            skipped_zero_shares += 1
            continue
        deal = {
            'ticker':       r['symbol'],
            'cik':          r['cik'],
            'type':         r['type'],
            'price_date':   _to_jsonable(r['price_date']),
            'trade_date':   _to_jsonable(r['trade_date']),
            'intraday':     bool(r['intraday']),
            'offer_price':  r['offer_price'],
            'shares':       r['shares'],
            'notional':     r['notional'],
            'split_factor': r['split_factor'],
            'adj_price':    r['adj_price'],
            'adj_shares':   r['adj_shares'],
            'seller':       r['seller'] or '',
            'relationship': r['relationship'] or '',
            'banks':        list(r['banks']) if r['banks'] else [],
            'evidence':     r['evidence'],
            'source':       r['source'],
        }
        deals.append(deal)

    out = {
        'doc': DOC,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'count': len(deals),
        'fields': FIELDS,
        'deals': deals,
    }

    stamp = date.today().strftime('%Y%m%d')
    out_path = OUT_DIR / f'block_golden_bootstrap.{stamp}.json'
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f'wrote {len(deals)} deals -> {out_path}')
    if skipped_zero_shares:
        print(
            f'  (skipped {skipped_zero_shares} rows with '
            'shares=0 — populate manual_shares.csv to '
            'include them)'
        )

    # Summary print
    from collections import Counter
    by_type = Counter(d['type'] for d in deals)
    by_evidence = Counter(d['evidence'] for d in deals)
    print(f'\n  by type: {dict(by_type)}')
    print(f'  by evidence: {dict(by_evidence)}')
    print(f'  date range: {deals[0]["price_date"]} -> {deals[-1]["price_date"]}')


if __name__ == '__main__':
    main()
