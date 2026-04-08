"""Historical block trade scanner.

Scans EDGAR indexes for 144 and registered offering
filings within a date range. Fetches, parses, dedupes,
and returns unified trade records.

Usage:
    trades = get_trades(
        syms, start=date(2025,1,1), end=date(2025,12,31)
    )
    # or filtered:
    trades = get_trades(syms, symbol='NVDA')
"""

import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from app.edgar.fetch import SEC_RPS
from app.edgar.index import (
    Filing,
    fetch_filings_async,
    fetch_full_index,
    filter_forms,
)
from app.util.dates import quarters

from app.edgar.parse.form144 import (
    Filing144,
    parse_144,
)
from app.edgar.parse.reg import (
    BANK_SYMS,
    REG_FORMS,
    parse_reg,
)
from app.mds.massive.refs import RefRow
from app.util.log import log

MIN_144_VALUE = 25_000_000
MIN_REG_VALUE = 50_000_000
MAX_MCAP_PCT = 0.20  # reject if > 20% of mkt_cap
MIN_BLOCK_PCT = 0.01  # >= 1% of outstanding

# Comp-sale nature keywords (case-insensitive)
_COMP_RE = re.compile(
    r'restricted|RSU|PSU|option|vest|compensation'
    r'|comp|award|grant|stock plan|bonus|SAR|LTIP'
    r'|PRSU|MIP|employee stock|incentive|director'
    r'|board|exercis|RPUS|cashless|ESPP',
    re.IGNORECASE,
)

# Institutional broker patterns
_INST_BROKER_RE = re.compile(
    r'Goldman Sachs & Co'
    r'|Morgan Stanley & Co'
    r'|J\.?P\.? ?Morgan Securities'
    r'|BofA Securities'
    r'|Barclays Capital'
    r'|Citigroup Global'
    r'|RBC Capital'
    r'|Jefferies LLC'
    r'|UBS Securities'
    r'|Deutsche Bank Securities'
    r'|BMO Capital',
    re.IGNORECASE,
)

# Retail/wealth broker markers (disqualify)
_RETAIL_RE = re.compile(
    r'Smith Barney|Executive Financial'
    r'|Fidelity|Schwab|Pershing'
    r'|Merrill Lynch|E\*TRADE|Vanguard'
    r'|TD Ameritrade|Interactive Brokers',
    re.IGNORECASE,
)


def _flag_144_block(
    d: Filing144,
) -> bool:
    """Heuristic: is this 144 filing a block trade?

    Signals (any two = flagged):
      - institutional broker (not retail/wealth)
      - nature is NOT comp (RSU/option/vest/etc.)
      - shares >= 1% of outstanding
    """
    score = 0

    # Broker signal
    if d.broker:
        if _RETAIL_RE.search(d.broker):
            pass  # retail = 0
        elif _INST_BROKER_RE.search(d.broker):
            score += 1

    # Nature signal
    if d.nature and not _COMP_RE.search(d.nature):
        score += 1

    # Size signal (% of outstanding)
    if (
        d.outstanding > 0
        and d.shares / d.outstanding >= MIN_BLOCK_PCT
    ):
        score += 1

    return score >= 2


@dataclass
class Trade:
    symbol: str
    date_filed: str
    shares: int
    implied_value: float
    price: float
    price_source: str  # 'offer' | 'last' | 'ref'
    filing_type: str  # '144' | '424B*'
    seller: str
    relationship: str  # 144: rel, reg: SEC|PRI
    underwriter: str
    mkt_cap: float
    flagged_block: bool = False
    is_ipo: bool = False
    # 144 detail (preserved)
    nature: str = ''
    pct_outstanding: float = 0.0



def _build_bank_ciks(
    syms: dict[str, RefRow],
) -> set[str]:
    ciks = set()
    for s in BANK_SYMS:
        if s in syms:
            ciks.add(syms[s].cik.lstrip('0'))
    return ciks


def _build_cik_map(
    syms: dict[str, RefRow],
) -> dict[str, RefRow]:
    m: dict[str, RefRow] = {}
    for ref in syms.values():
        cik = ref.cik.lstrip('0') or '0'
        m[cik] = ref
    return m


def _scan_144(
    filings: list[Filing],
    cik_map: dict[str, RefRow],
) -> list[Trade]:
    """Scan 144 filings, return qualifying trades."""
    f144 = filter_forms(filings, ('144',))
    matched = [
        f for f in f144 if f.cik in cik_map
    ]
    if not matched:
        return []

    trades: list[Trade] = []

    def on_filing(f: Filing, raw: bytes) -> None:
        d = parse_144(raw)
        if not d or d.shares <= 0:
            return
        ref = cik_map[f.cik]
        implied = d.shares * ref.price
        if implied < MIN_144_VALUE:
            return
        pct = (
            d.shares / d.outstanding
            if d.outstanding > 0
            else 0.0
        )
        trades.append(
            Trade(
                symbol=ref.symbol,
                date_filed=f.date_filed,
                shares=d.shares,
                implied_value=implied,
                price=ref.price,
                price_source='ref',
                filing_type='144',
                seller=d.seller,
                relationship=d.relationship,
                underwriter=d.broker,
                mkt_cap=ref.mkt_cap,
                flagged_block=_flag_144_block(d),
                nature=d.nature,
                pct_outstanding=pct,
            )
        )

    asyncio.run(
        fetch_filings_async(matched, on_filing)
    )
    return trades


def _scan_reg(
    filings: list[Filing],
    cik_map: dict[str, RefRow],
    bank_ciks: set[str],
) -> list[Trade]:
    """Scan registered offering filings."""
    reg = [
        f
        for f in filings
        if f.cik in cik_map
        and (
            f.form_type in REG_FORMS
            and not (
                f.form_type == '424B2'
                and f.cik in bank_ciks
            )
        )
    ]
    if not reg:
        return []

    trades: list[Trade] = []

    def on_filing(f: Filing, raw: bytes) -> None:
        d = parse_reg(raw)
        if not d or d.shares <= 0:
            return
        ref = cik_map[f.cik]
        px = (
            d.offer_price
            or d.last_price
            or ref.price
        )
        px_src = (
            'offer'
            if d.offer_price
            else ('last' if d.last_price else 'ref')
        )
        implied = (
            d.total
            if d.total
            else d.shares * px
        )
        if implied < MIN_REG_VALUE:
            return
        if implied > ref.mkt_cap * MAX_MCAP_PCT:
            return
        trades.append(
            Trade(
                symbol=ref.symbol,
                date_filed=f.date_filed,
                shares=d.shares,
                implied_value=implied,
                price=px,
                price_source=px_src,
                filing_type=f.form_type,
                seller=(
                    'SEC'
                    if d.is_seller
                    else 'PRI'
                ),
                relationship=(
                    'selling stockholder'
                    if d.is_seller
                    else 'company'
                ),
                underwriter=d.underwriter,
                mkt_cap=ref.mkt_cap,
                flagged_block=d.is_bought,
                is_ipo=d.is_ipo,
            )
        )

    asyncio.run(
        fetch_filings_async(reg, on_filing)
    )
    return trades


def _dedupe_144(
    trades: list[Trade],
) -> list[Trade]:
    """Collapse same (symbol, seller, shares) on
    same or consecutive days."""
    groups: dict[
        tuple[str, str, int], list[Trade]
    ] = defaultdict(list)
    for t in trades:
        key = (t.symbol, t.seller, t.shares)
        groups[key].append(t)
    deduped = []
    for entries in groups.values():
        entries.sort(key=lambda t: t.date_filed)
        deduped.append(entries[0])
    return deduped


def _dedupe_reg(
    trades: list[Trade],
) -> list[Trade]:
    """Per (symbol, shares), keep latest filing
    (final over preliminary)."""
    groups: dict[
        tuple[str, int], list[Trade]
    ] = defaultdict(list)
    for t in trades:
        key = (t.symbol, t.shares)
        groups[key].append(t)
    deduped = []
    for entries in groups.values():
        entries.sort(key=lambda t: t.date_filed)
        deduped.append(entries[-1])
    return deduped


def get_trades(
    syms: dict[str, RefRow],
    start: date | None = None,
    end: date | None = None,
    symbol: str | None = None,
    types: str = 'both',
    rps: int = 9,
) -> list[Trade]:
    """Scan EDGAR for block trades in a date range.

    Args:
        syms: symbol reference table from load_syms()
        start: start date (default: 1 year ago)
        end: end date (default: today)
        symbol: optional symbol filter
        types: '144', 'reg', or 'both'
        rps: SEC requests per second (default 9)

    Returns list of Trade records sorted by date.
    """
    import app.edgar.fetch as fetch_mod

    fetch_mod.SEC_RPS = rps

    if end is None:
        end = date.today()
    if start is None:
        start = date(end.year - 1, end.month, end.day)

    cik_map = _build_cik_map(syms)
    bank_ciks = _build_bank_ciks(syms)

    # If symbol filter, narrow cik_map
    if symbol:
        sym_upper = symbol.upper()
        if sym_upper not in syms:
            return []
        ref = syms[sym_upper]
        cik = ref.cik.lstrip('0') or '0'
        cik_map = {cik: ref}

    all_144: list[Trade] = []
    all_reg: list[Trade] = []

    for year, qtr in quarters(start, end):
        log.info('scanning quarter', year=year, qtr=qtr)
        filings = fetch_full_index(year, qtr)

        if types in ('144', 'both'):
            t144 = _scan_144(filings, cik_map)
            all_144.extend(t144)

        if types in ('reg', 'both'):
            treg = _scan_reg(
                filings, cik_map, bank_ciks
            )
            all_reg.extend(treg)

        log.info('quarter complete', year=year, qtr=qtr, trades=len(all_144) + len(all_reg))

    # Dedupe
    if types in ('144', 'both'):
        all_144 = _dedupe_144(all_144)
    if types in ('reg', 'both'):
        all_reg = _dedupe_reg(all_reg)
        # Filter MSTR treasury conversions
        all_reg = [
            t for t in all_reg
            if t.symbol != 'MSTR'
        ]

    trades = all_144 + all_reg
    trades.sort(key=lambda t: t.date_filed)
    return trades
