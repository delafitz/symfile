"""Deal-level evaluation of the new reg parsers.

Where eval_reg_parsers.py reports per-filing fill
rates, this groups labeled filings by golden_idx
(treating each golden row as one deal), resolves
the cluster via app.parsers.reg_deal.resolve_deal,
and reports deal-level completeness.

A "complete" deal has offer_price, total, and
shares_offered all populated — these are what
downstream block-trade detection needs.

    uv run python tools/eval_reg_deals.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.edgar.fetch import get_cached  # noqa: E402
from app.parsers.reg_deal import (  # noqa: E402
    parse_member,
    resolve_deal,
)

LABELS = Path('data/corpus/reg_labels.parquet')
CORPUS = Path('data/corpus/reg_corpus.parquet')
GOLDEN = Path('data/bootstrap/regs_golden.20260516.json')


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def main() -> None:
    labels = pl.read_parquet(LABELS)
    corpus = pl.read_parquet(CORPUS)
    golden = json.loads(GOLDEN.read_text())

    # corpus -> filename:cik lookup
    cik_for = {
        r['filename']: r['cik']
        for r in corpus.to_dicts()
    }

    # Group labels by golden_idx
    by_idx: dict[int, list[dict]] = defaultdict(list)
    for r in labels.to_dicts():
        by_idx[r['golden_idx']].append(r)

    print(f'evaluating {len(by_idx)} deals '
          f'({labels.height} candidate filings)')

    c = Counter()
    sample_complete = []
    sample_partial = []
    sample_failed = []

    for gi, rows in by_idx.items():
        g = golden[gi]
        symbol = (g.get('Ticker') or '').upper()
        members = []
        for r in rows:
            raw = get_cached(r['candidate_filename'])
            if raw is None:
                continue
            members.append(
                parse_member(
                    filename=r['candidate_filename'],
                    filing_date=_parse_date(r['candidate_date']),
                    form_type=r['form_type'],
                    cik=cik_for.get(r['candidate_filename'], ''),
                    raw=raw,
                )
            )

        deal = resolve_deal(members, symbol)
        c['total'] += 1
        if deal is None:
            c['no_parse'] += 1
            sample_failed.append((gi, symbol, g.get('PriceDt')))
            continue

        c['parsed'] += 1
        if deal.offer_price > 0:
            c['has_offer_price'] += 1
        if deal.total > 0:
            c['has_total'] += 1
        if deal.shares_offered > 0:
            c['has_shares'] += 1
        if deal.last_price > 0:
            c['has_last_price'] += 1

        complete = (
            deal.offer_price > 0
            and deal.total > 0
            and deal.shares_offered > 0
        )
        if complete:
            c['complete'] += 1
            if len(sample_complete) < 5:
                sample_complete.append((gi, deal))
        else:
            if len(sample_partial) < 8:
                sample_partial.append((gi, deal))

    n = c['total']
    parsed = c['parsed']
    print(f'\n=== deal-level coverage ===')
    print(f'  deals:           {n}')
    print(f'  resolved:        {parsed} ({parsed / n * 100:.1f}%)')
    print(f'  complete (price+total+shares):'
          f' {c["complete"]:3d} ({c["complete"] / n * 100:.1f}%)')
    print()
    print('  field fill (of resolved):')
    for k, lbl in [
        ('has_offer_price', 'offer_price'),
        ('has_total', 'total'),
        ('has_shares', 'shares'),
        ('has_last_price', 'last_price'),
    ]:
        v = c[k]
        print(f'    {lbl:12s} {v:3d}/{parsed} '
              f'({v / parsed * 100:.1f}%)')

    if sample_complete:
        print('\n--- sample complete deals ---')
        for gi, d in sample_complete:
            n_files = len(d.filenames)
            print(f'  gi={gi:3d}  {d.symbol:6s}  '
                  f'{d.announce_date} -> {d.price_date}  '
                  f'shares={d.shares_offered:>12,}  '
                  f'offer=${d.offer_price:>8.2f}  '
                  f'total=${d.total:>15,.0f}  '
                  f'({n_files} filings)')

    if sample_partial:
        print('\n--- sample partial deals (resolved but '
              'missing pricing) ---')
        for gi, d in sample_partial:
            print(f'  gi={gi:3d}  {d.symbol:6s}  '
                  f'forms={d.forms}  '
                  f'offer={d.offer_price}  '
                  f'total={d.total}  '
                  f'last={d.last_price}')


if __name__ == '__main__':
    main()
