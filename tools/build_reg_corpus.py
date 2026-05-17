"""Build the labeled 424B* corpus for the new reg parser.

Two artifacts:

  data/corpus/reg_corpus.parquet
    Every cached 424B* filing on disk with its index metadata.
    Columns: filename, cache_path, form_type, cik, company,
             date_filed, symbol (resolved via cik_map)

  data/corpus/reg_labels.parquet
    Golden positives: each regs_golden row paired with candidate
    cached filings within ±5 days of PriceDt for the same symbol.
    Columns: golden_idx, symbol, price_date, candidate_filename,
             candidate_date, form_type

The corpus is for offline parser iteration — no network. Rebuild
when new filings are synced.

    uv run python tools/build_reg_corpus.py
"""

import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent)
)

from app.edgar.fetch import get_cached  # noqa: E402
from app.mds.massive.refs import (  # noqa: E402
    build_cik_map,
)
from app.mds.syms import load_syms  # noqa: E402

CORPUS_DIR = Path('data/corpus')
GOLDEN_PATH = Path(
    'data/bootstrap/regs_golden.20260516.json'
)

# Forms we care about
REG_PREFIX = '424B'

_IDX_DATE_FORMATS = ('%Y%m%d', '%Y-%m-%d')


def _parse_idx_date(s: str):
    s = s.strip()
    for fmt in _IDX_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_golden_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(
            s, '%d-%b-%Y'
        ).date()
    except ValueError:
        return None


def build_corpus(
    cik_to_sym: dict[str, str],
) -> pl.DataFrame:
    """Walk every .idx, keep 424B* entries that are cached."""
    rows = []
    seen: set[str] = set()
    for f in sorted(
        Path('data/indices').glob('*.idx')
    ):
        for line in f.read_text().splitlines():
            parts = line.split('|')
            if len(parts) < 5:
                continue
            form = parts[2]
            if not form.startswith(REG_PREFIX):
                continue
            fn = parts[4]
            if fn in seen:
                continue
            seen.add(fn)
            if get_cached(fn) is None:
                continue
            d = _parse_idx_date(parts[3])
            if not d:
                continue
            cik = parts[0].lstrip('0') or '0'
            rows.append({
                'filename': fn,
                'form_type': form,
                'cik': cik,
                'company': parts[1],
                'date_filed': d.isoformat(),
                'symbol': cik_to_sym.get(cik, ''),
            })
    return pl.DataFrame(rows)


def build_labels(
    corpus: pl.DataFrame,
    syms: dict,
) -> pl.DataFrame:
    """For each regs_golden row, find corpus candidates
    within ±5 days for the same symbol."""
    golden = json.loads(GOLDEN_PATH.read_text())
    rows = []
    TOL = timedelta(days=5)
    for i, g in enumerate(golden):
        sym = (g.get('Ticker') or '').upper()
        pd = _parse_golden_date(g.get('PriceDt'))
        if not sym or not pd:
            continue
        if sym not in syms:
            continue
        sub = corpus.filter(pl.col('symbol') == sym)
        for r in sub.to_dicts():
            try:
                cd = datetime.fromisoformat(
                    r['date_filed']
                ).date()
            except ValueError:
                continue
            if abs((cd - pd).days) > TOL.days:
                continue
            rows.append({
                'golden_idx': i,
                'symbol': sym,
                'price_date': pd.isoformat(),
                'candidate_filename': r['filename'],
                'candidate_date': r['date_filed'],
                'form_type': r['form_type'],
            })
    return pl.DataFrame(rows)


def main() -> None:
    print('loading syms...')
    syms = load_syms()
    cik_to_sym = {
        ref.cik.lstrip('0') or '0': sym
        for sym, ref in syms.items()
    }

    print('building corpus...')
    corpus = build_corpus(cik_to_sym)
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    corpus_path = CORPUS_DIR / 'reg_corpus.parquet'
    corpus.write_parquet(corpus_path)
    print(
        f'  {corpus.height} cached 424B* '
        f'-> {corpus_path}'
    )
    by_form = (
        corpus.group_by('form_type')
        .len()
        .sort('len', descending=True)
    )
    print('  by form:')
    for r in by_form.to_dicts():
        print(f'    {r["form_type"]:8s} {r["len"]:6d}')

    print('\nbuilding labels...')
    labels = build_labels(corpus, syms)
    labels_path = CORPUS_DIR / 'reg_labels.parquet'
    labels.write_parquet(labels_path)
    print(
        f'  {labels.height} candidate pairs '
        f'-> {labels_path}'
    )

    # Coverage: how many golden rows got >=1 candidate
    n_golden = len(
        json.loads(GOLDEN_PATH.read_text())
    )
    matched_golden = labels.n_unique('golden_idx')
    print(
        f'  golden rows with ≥1 candidate: '
        f'{matched_golden}/{n_golden} '
        f'({matched_golden / n_golden * 100:.1f}%)'
    )


if __name__ == '__main__':
    main()
