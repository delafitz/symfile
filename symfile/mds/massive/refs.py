"""Reference data: symbol, CIK, market cap, price.

Phase 1: Polygon snapshot -> prices (one call)
Phase 2: Polygon ticker details -> market cap
         (async, concurrency pool)
Filter:  mkt_cap >= $1B

Caches to data/mds/refs.YYYYMMDD.csv.
"""

import asyncio
import csv
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from symfile.mds import DATA_DIR
from symfile.mds.massive.session import get_client

MAX_AGE_DAYS = 7
MIN_MKT_CAP = 1_000_000_000
CONCURRENCY = 20


@dataclass
class RefRow:
    symbol: str
    cik: str
    name: str
    mkt_cap: float
    price: float


def _find_cached() -> tuple[Path, date] | None:
    pattern = re.compile(r'refs\.(\d{8})\.csv$')
    best: tuple[Path, date] | None = None
    if not DATA_DIR.exists():
        return None
    for f in DATA_DIR.iterdir():
        m = pattern.match(f.name)
        if m:
            d = date.fromisoformat(
                f'{m.group(1)[:4]}-{m.group(1)[4:6]}'
                f'-{m.group(1)[6:]}'
            )
            if best is None or d > best[1]:
                best = (f, d)
    return best


async def _fetch_refs_async(
    tickers: dict[str, dict],
) -> list[RefRow]:
    """Fetch prices + market cap for all CS tickers."""
    client = get_client()

    # Phase 1: bulk snapshot for prices (one call)
    print('phase 1: snapshots...')
    snaps = await asyncio.to_thread(
        client.get_snapshot_all, 'stocks'
    )
    prices: dict[str, float] = {}
    for s in snaps:
        p = None
        if s.day and s.day.close:
            p = s.day.close
        elif s.prev_day and s.prev_day.close:
            p = s.prev_day.close
        if p:
            prices[s.ticker] = p
    print(f'  {len(prices)} prices')

    # Build candidate list: CS + CIK + has price
    candidates = [
        sym
        for sym, info in tickers.items()
        if info.get('type') == 'CS'
        and info.get('cik')
        and sym in prices
    ]
    print(
        f'phase 2: details for '
        f'{len(candidates)} symbols '
        f'(concurrency={CONCURRENCY})...'
    )

    # Phase 2: async ticker details with semaphore
    sem = asyncio.Semaphore(CONCURRENCY)
    rows: list[RefRow] = []
    lock = asyncio.Lock()
    done = 0

    async def fetch_one(sym: str) -> None:
        nonlocal done
        async with sem:
            try:
                d = await asyncio.to_thread(
                    client.get_ticker_details, sym
                )
            except Exception:
                return
            mc = getattr(d, 'market_cap', None)
            if not mc or mc < MIN_MKT_CAP:
                return
            cik = tickers[sym]['cik']
            ref = RefRow(
                symbol=sym,
                cik=cik,
                name=tickers[sym].get(
                    'name', ''
                ),
                mkt_cap=mc,
                price=prices[sym],
            )
            async with lock:
                rows.append(ref)
                done += 1
                if done % 100 == 0:
                    print(
                        f'  {done} qualifying...'
                    )

    await asyncio.gather(
        *(fetch_one(s) for s in candidates)
    )
    print(
        f'  {len(rows)} symbols with '
        f'mkt_cap >= ${MIN_MKT_CAP / 1e9:.0f}B'
    )
    return rows


def _save(rows: list[RefRow]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = date.today().strftime('%Y%m%d')
    path = DATA_DIR / f'refs.{stamp}.csv'
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                'symbol', 'cik', 'name',
                'mkt_cap', 'price',
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    'symbol': r.symbol,
                    'cik': r.cik,
                    'name': r.name,
                    'mkt_cap': r.mkt_cap,
                    'price': r.price,
                }
            )
    print(f'saved {len(rows)} refs to {path}')
    return path


def _load_csv(path: Path) -> dict[str, RefRow]:
    result: dict[str, RefRow] = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            result[row['symbol']] = RefRow(
                symbol=row['symbol'],
                cik=row['cik'],
                name=row['name'],
                mkt_cap=float(row['mkt_cap']),
                price=float(row['price']),
            )
    return result


def load_refs(
    tickers: dict[str, dict] | None = None,
    max_age_days: int = MAX_AGE_DAYS,
) -> dict[str, RefRow]:
    """Load refs from cache or fetch fresh."""
    cached = _find_cached()
    cutoff = date.today() - timedelta(
        days=max_age_days
    )

    if cached and cached[1] >= cutoff:
        path = cached[0]
        print(
            f'using cached refs from {path.name}'
        )
        result = _load_csv(path)
        print(f'  {len(result)} symbols')
        return result

    if tickers is None:
        from symfile.mds.massive.tickers import (
            load_tickers,
        )

        tickers = load_tickers()

    print('building refs...')
    rows = asyncio.run(_fetch_refs_async(tickers))
    _save(rows)
    return {r.symbol: r for r in rows}


def build_cik_map(
    refs: dict[str, RefRow],
) -> dict[str, RefRow]:
    """Build CIK->RefRow mapping.

    CIK keys are unpadded to match EDGAR index format.
    """
    cik_to_ref: dict[str, RefRow] = {}
    for ref in refs.values():
        cik = ref.cik.lstrip('0') or '0'
        cik_to_ref[cik] = ref
    return cik_to_ref
