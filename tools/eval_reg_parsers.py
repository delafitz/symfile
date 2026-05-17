"""Evaluation harness for app/parsers/reg_424b5 + reg_424b7.

Runs each parser against its slice of the labeled corpus
and reports parse rate, field fill rates, and surfaces
filings with missing fields (grouped by golden_idx so
you can see whether a given block is fully covered).

    uv run python tools/eval_reg_parsers.py [--form 424B5]
                                            [--show 20]
                                            [--field shares_offered]
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.edgar.fetch import get_cached  # noqa: E402
from app.parsers.reg_deal import PARSERS  # noqa: E402

LABELS = Path('data/corpus/reg_labels.parquet')

TRACKED_FIELDS = [
    'shares_offered',
    'offer_price',
    'total',
    'last_price',
    'last_price_date',
    'ticker',
    'exchange',
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--form', default=None,
                    help='restrict to one form type')
    ap.add_argument('--show', type=int, default=10,
                    help='example failures to print')
    ap.add_argument('--field', default=None,
                    help='only show filings missing this field')
    args = ap.parse_args()

    labels = pl.read_parquet(LABELS)
    if args.form:
        labels = labels.filter(
            pl.col('form_type') == args.form
        )

    # Deduplicate by candidate_filename — same file
    # may be linked to multiple golden_idx.
    files = (
        labels.group_by('candidate_filename')
        .agg(
            pl.col('form_type').first().alias('form_type'),
            pl.col('symbol').first().alias('symbol'),
            pl.col('golden_idx').alias('golden_idxs'),
        )
    )

    print(f'evaluating {files.height} unique filings')

    by_form: dict[str, Counter] = defaultdict(Counter)
    failures: dict[str, list] = defaultdict(list)

    for r in files.to_dicts():
        form = r['form_type']
        parser = PARSERS.get(form)
        if not parser:
            by_form[form]['skipped'] += 1
            continue

        raw = get_cached(r['candidate_filename'])
        if raw is None:
            by_form[form]['cache_miss'] += 1
            continue

        by_form[form]['total'] += 1

        try:
            f = parser(raw)
        except Exception as e:
            by_form[form]['exception'] += 1
            failures[form].append({
                'filename': r['candidate_filename'],
                'symbol': r['symbol'],
                'reason': f'exception: {e}',
            })
            continue

        if f is None:
            by_form[form]['rejected'] += 1
            failures[form].append({
                'filename': r['candidate_filename'],
                'symbol': r['symbol'],
                'reason': 'parser returned None',
            })
            continue

        by_form[form]['parsed'] += 1
        for fld in TRACKED_FIELDS:
            v = getattr(f, fld, None)
            if v not in (0, 0.0, '', None):
                by_form[form][f'has_{fld}'] += 1

        if args.field and args.field in f.missing:
            failures[form].append({
                'filename': r['candidate_filename'],
                'symbol': r['symbol'],
                'reason': f'missing {args.field}',
                'parsed': f,
            })

    # Report
    for form in sorted(by_form):
        c = by_form[form]
        total = c['total']
        if not total:
            continue
        print(f'\n=== {form} ({total} filings) ===')
        print(f'  parsed:      {c["parsed"]:4d} '
              f'({c["parsed"] / total * 100:5.1f}%)')
        if c.get('rejected'):
            print(f'  rejected:    {c["rejected"]:4d}')
        if c.get('exception'):
            print(f'  exceptions:  {c["exception"]:4d}')
        if c.get('cache_miss'):
            print(f'  cache miss:  {c["cache_miss"]:4d}')
        print()
        print('  field fill (of parsed):')
        parsed_n = c['parsed'] or 1
        for fld in TRACKED_FIELDS:
            n = c[f'has_{fld}']
            print(f'    {fld:20s} {n:4d} '
                  f'({n / parsed_n * 100:5.1f}%)')

    # Print sample failures
    if args.show and failures:
        for form, items in failures.items():
            if not items:
                continue
            print(f'\n--- sample {form} failures '
                  f'({len(items)} total, showing '
                  f'{min(args.show, len(items))}) ---')
            for it in items[:args.show]:
                print(f'  {it["symbol"]:8s} '
                      f'{it["filename"]}  -> {it["reason"]}')


if __name__ == '__main__':
    main()
