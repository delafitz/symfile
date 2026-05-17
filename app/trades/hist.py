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
from app.parsers.reg import BANK_SYMS, REG_FORMS
from app.parsers.reg_deal import PARSERS
from app.mds.massive.refs import RefRow
from app.trades.banks import OTHER, parse_banks
from app.util.log import log

MIN_144_VALUE = 25_000_000
MIN_REG_VALUE = 50_000_000
MAX_MCAP_PCT = 0.20  # reject if > 20% of mkt_cap

# Flag thresholds — OR'd together
BLOCK_NOTIONAL = 50_000_000
BLOCK_PCT_OUTSTANDING = 0.01
BLOCK_PCT_ADV = 0.5

# Comp-sale nature keywords (case-insensitive)
_COMP_RE = re.compile(
    r'restricted|RSU|PSU|option|vest|compensation'
    r'|comp|award|grant|stock plan|bonus|SAR|LTIP'
    r'|PRSU|MIP|employee stock|incentive|director'
    r'|board|exercis|RPUS|cashless|ESPP',
    re.IGNORECASE,
)

# Major-bank broker whitelist — matches any brand
# variant associated with a top-tier institution,
# including their wealth-management arms (we no
# longer reject retail-branded channels; sales
# through a major bank's wealth desk are still
# block-adjacent when size/liquidity say so).
_INST_BROKER_RE = re.compile(
    r'Goldman Sachs'
    r'|Morgan Stanley'
    r'|Smith Barney'
    r'|Merrill Lynch'
    r'|BofA'
    r'|Bank of America'
    r'|Citi(group)?'
    r'|J\.?P\.? ?Morgan'
    r'|JPMorgan'
    r'|Barclays'
    r'|RBC Capital'
    r'|Jefferies'
    r'|UBS'
    r'|Deutsche Bank'
    r'|BMO',
    re.IGNORECASE,
)


def _flag_144_block(
    d: Filing144,
    ref: RefRow,
) -> bool:
    """Heuristic: is this 144 filing a block trade?

    Requires a known-bank broker (anything in our
    BANKS dict), notional >= $50M, AND a size-relative
    signal (>=0.5% of float OR >=50% of ADV). The
    size-relative gate filters megacap insider noise:
    a $78M sale by a Google officer is 0.002% of float
    and not a block in any meaningful sense.
    """
    if not d.broker:
        return False
    parsed = parse_banks(d.broker)
    if not parsed or parsed == [OTHER]:
        return False

    pct_out = (
        d.shares / d.outstanding
        if d.outstanding > 0 else 0.0
    )
    pct_adv = (
        d.shares / ref.adv
        if ref.adv > 0 else 0.0
    )
    notional = d.shares * ref.price

    # 1) Significant stake — sponsor / 5%+ holder exit
    if pct_out >= BLOCK_PCT_OUTSTANDING:
        return True
    # 2) Single-day liquidity event
    if pct_adv >= BLOCK_PCT_ADV:
        return True
    # 3) Big notional, but require a non-trivial stake
    #    (>=0.5%) to filter megacap insider noise
    if (
        notional >= BLOCK_NOTIONAL
        and pct_out >= BLOCK_PCT_OUTSTANDING / 2
    ):
        return True
    return False


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
    trade_date: str = ''  # approx sale/pricing date
    # 144 detail (preserved)
    nature: str = ''
    pct_outstanding: float = 0.0
    # reg lock-up
    lockup: bool = False
    lockup_days: int = 0



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


def build_144_trade(
    filing: Filing,
    raw: bytes,
    cik_map: dict[str, RefRow],
) -> Trade | None:
    """Parse a 144 filing and build a Trade.

    144s are filed by the SELLER (often an insider or
    sponsor outside our universe), so we resolve the
    issuer via the parsed XML's issuerCik rather than
    the filer's index CIK.
    """
    d = parse_144(raw)
    if not d or d.shares <= 0:
        return None
    # Drop comp sales (RSU/option/vest/etc.)
    if d.nature and _COMP_RE.search(d.nature):
        return None
    issuer_key = (
        d.issuer_cik.lstrip('0') or '0'
    ) if d.issuer_cik else filing.cik
    ref = cik_map.get(issuer_key)
    if not ref:
        return None
    implied = d.shares * ref.price
    if implied < MIN_144_VALUE:
        return None
    pct = (
        d.shares / d.outstanding
        if d.outstanding > 0
        else 0.0
    )
    return Trade(
        symbol=ref.symbol,
        date_filed=filing.date_filed,
        shares=d.shares,
        implied_value=implied,
        price=ref.price,
        price_source='ref',
        filing_type='144',
        seller=d.seller,
        relationship=d.relationship,
        underwriter=d.broker,
        mkt_cap=ref.mkt_cap,
        flagged_block=_flag_144_block(d, ref),
        trade_date=d.sale_date,
        nature=d.nature,
        pct_outstanding=pct,
    )


def build_reg_trade(
    filing: Filing,
    raw: bytes,
    cik_map: dict[str, RefRow],
) -> Trade | None:
    """Parse a reg offering and build a Trade."""
    parser = PARSERS.get(filing.form_type)
    if parser is None:
        return None
    d = parser(raw)
    if not d or d.shares_offered <= 0:
        return None
    ref = cik_map.get(filing.cik)
    if not ref:
        return None
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
        else d.shares_offered * px
    )
    if implied < MIN_REG_VALUE:
        return None
    if implied > ref.mkt_cap * MAX_MCAP_PCT:
        return None
    is_seller = d.has_selling_stockholder
    return Trade(
        symbol=ref.symbol,
        date_filed=filing.date_filed,
        shares=d.shares_offered,
        implied_value=implied,
        price=px,
        price_source=px_src,
        filing_type=filing.form_type,
        seller='SEC' if is_seller else 'PRI',
        relationship=(
            'selling stockholder'
            if is_seller
            else 'company'
        ),
        underwriter=d.underwriter,
        mkt_cap=ref.mkt_cap,
        flagged_block=d.is_bought,
        is_ipo=d.is_ipo,
        trade_date=filing.date_filed,
        lockup=d.lockup,
        lockup_days=d.lockup_days,
    )


def _scan_144(
    filings: list[Filing],
    cik_map: dict[str, RefRow],
) -> list[Trade]:
    """Scan 144 filings, return qualifying trades.

    Can't pre-filter on f.cik (the filer is usually
    the seller, not the issuer). Fetch all 144s and
    let build_144_trade resolve issuer at parse time.
    """
    f144 = filter_forms(filings, ('144',))
    if not f144:
        return []

    trades: list[Trade] = []

    def on_filing(f: Filing, raw: bytes) -> None:
        t = build_144_trade(f, raw, cik_map)
        if t:
            trades.append(t)

    asyncio.run(
        fetch_filings_async(f144, on_filing)
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
        t = build_reg_trade(f, raw, cik_map)
        if t:
            trades.append(t)

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
