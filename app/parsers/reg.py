"""Shared dataclass + cover-page utilities for 424B*
filings.

Per-form extractors live in reg_424b5.py / reg_424b7.py
and populate a RegFiling.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field

# --- Dataclass ---


@dataclass
class RegFiling:
    """Common extraction shape for any 424B* filing.

    Fields are filled best-effort; consumers should
    treat them as nullable. The set of fields is
    the *union* across 424B5/B7 — not every filing
    populates every field.
    """

    form_type: str  # '424B5', '424B7', ...

    # Cover-page identity
    issuer_name: str = ''
    ticker: str = ''
    exchange: str = ''  # NYSE / Nasdaq / NYSE Texas / ...

    # Offering size
    shares_offered: int = 0      # total in title
    issuer_shares: int = 0       # 'We are offering N shares'
    ssh_shares: int = 0          # selling-stockholder portion

    # Pricing
    offer_price: float = 0.0     # pricing-table $/share
    total: float = 0.0           # gross proceeds
    last_price: float = 0.0      # last reported sale price
    last_price_date: str = ''    # ISO if extracted

    # Classification flags
    is_preliminary: bool = False  # 'Subject to completion'
    is_ipo: bool = False
    is_bought: bool = False       # firm-commitment language
    has_selling_stockholder: bool = False

    # Aux (kept for downstream block-trade scoring)
    underwriter: str = ''
    lockup: bool = False
    lockup_days: int = 0

    # Diagnostics: which fields failed to extract.
    # Populated by extractors as they go.
    missing: list[str] = field(default_factory=list)


# --- Common HTML utilities ---


def strip_html(text: str) -> str:
    """Strip tags, decode entities, collapse ws.

    Returns clean text suitable for regex matching.
    The result preserves the relative ordering of
    cover-page elements.
    """
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = html.unescape(clean)
    clean = clean.replace(' ', ' ').replace(' ', ' ')
    return re.sub(r'\s+', ' ', clean)


def decode_raw(raw: bytes) -> str:
    """Decode SEC filing bytes (mixed encodings)."""
    return raw.decode('latin-1')


# --- Cover-page field extractors ---


# Pricing patterns — three flavors:
#  (1) "Public offering price $X.XX ... $TOTAL" cover-row
#  (2) block-deal: "agreed to purchase ... at a price
#      of/equal to $X.XX per share"
#  (3) standard 3-column table:
#      "Per Share $ 16.85 $ 0.42 $ 16.43"
# All allow 2 or 3 decimal places.
_OFFER_PX_RE = re.compile(
    r'public offering price[\s\S]{0,200}?'
    r'\$\s*([\d]+\.[\d]{2,5})',
    re.IGNORECASE,
)
_BLOCK_PX_RE = re.compile(
    r'(?:agreed|committed)\s+to\s+purchase[\s\S]{0,400}?'
    r'at\s+(?:(?:a\s+|the\s+)?price\s+(?:of\s+|equal\s+to\s+)?)?'
    r'\$\s*([\d]+\.[\d]{2,5})\s*'
    r'per\s+(?:share|unit|ordinary|common|ADS|ADR)',
    re.IGNORECASE,
)
_PER_SHARE_TABLE_RE = re.compile(
    r'Per\s+Share\b[\s\S]{0,60}?'
    r'\$\s*([\d]+\.[\d]{2,5})'
)

# Total dollar amount — either the second $ on a
# pricing-table row, or the "Total" row of the
# 3-column pricing table.
_TOTAL_RE = re.compile(
    r'public offering price[\s\S]{0,200}?'
    r'\$\s*[\d]+\.[\d]{2,5}[\s\S]{0,80}?'
    r'\$\s*([\d,]+(?:\.[\d]{2})?)',
    re.IGNORECASE,
)
_TOTAL_TABLE_RE = re.compile(
    r'\bTotal\b[\s\S]{0,60}?'
    r'\$\s*([\d,]{7,})(?:\.[\d]{2})?'
)
# Block-deal proceeds. Two flavors:
#  (a) full number: "result in $344,374,110"
#  (b) M/B suffix: "result in approximately $96.0 million"
# Some filings have a stray duplicate $ ("$ $344").
_BLOCK_TOTAL_RE = re.compile(
    r'(?:will\s+result\s+in|aggregate\s+proceeds\s+of)'
    r'\s+(?:approximately\s+)?'
    r'\$\s*\$?\s*([\d,]{7,})(?:\.[\d]{2})?',
    re.IGNORECASE,
)
_BLOCK_TOTAL_M_RE = re.compile(
    r'(?:will\s+result\s+in|aggregate\s+proceeds\s+of)'
    r'\s+(?:approximately\s+)?'
    r'\$\s*([\d]+(?:\.[\d]+)?)\s+(million|billion)\b',
    re.IGNORECASE,
)

# "last reported sale price ... was $X.XX" — $ is
# normally present but some filers omit it, so the
# dollar sign is optional. We constrain to a plausible
# stock-price range to avoid latching onto stray decimals.
_LAST_PX_RE = re.compile(
    r'(?:last reported|closing)\s+'
    r'(?:sale[s]?\s+)?price[\s\S]{0,200}?'
    r'\$?\s*([\d]{1,5}\.[\d]{2})\s+per\s+share',
    re.IGNORECASE,
)
_LAST_PX_RE_FALLBACK = re.compile(
    r'(?:last reported|closing)\s+'
    r'(?:sale[s]?\s+)?price[\s\S]{0,200}?'
    r'\$\s*([\d]{1,5}\.[\d]{2})',
    re.IGNORECASE,
)

# Date inside the last-price clause. Cover wording often
# inserts "on the NYSE/Nasdaq" before the actual date,
# so the date phrase must lead with a month name.
_LAST_PX_DATE_RE = re.compile(
    r'(?:last reported|closing)[\s\S]{0,300}?'
    r'on\s+(January|February|March|April|May|June|July'
    r'|August|September|October|November|December)'
    r'\s+(\d{1,2}),?\s+(\d{4})',
    re.IGNORECASE,
)

# "X,XXX,XXX shares/units/ADSs" near the title (first
# 3000 chars). MLPs trade in "Common Units"; ADRs in
# "American Depositary Shares" (ADSs).
_TITLE_SHARES_RE = re.compile(
    r'(?<!\$)\b([\d,]{4,})\s+'
    r'(?:[\w-]+\s+){0,3}'
    r'(?:[Ss]hares|[Uu]nits|ADSs?|ADRs?)\b'
)

# "We are offering N shares" — primary issuer count
_ISSUER_SHARES_RE = re.compile(
    r'[Ww]e\s+are\s+offering\s+([\d,]{4,})\s+shares',
)

# "selling stockholder ... offering N shares" /
# "selling stockholder ... N shares"
_SSH_RE = re.compile(
    r'selling\s+(?:stockholder|shareholder)s?'
    r'[\s\S]{0,300}?(?:offering|is\s+selling)?\s*'
    r'(?:up\s+to\s+)?([\d,]{4,})\s+shares',
    re.IGNORECASE,
)
_SSH_PRESENT_RE = re.compile(
    r'selling\s+(?:stockholder|shareholder)s?',
    re.IGNORECASE,
)

# "under the symbol 'XXX'" or "under the symbol “XXX”"
_TICKER_RE = re.compile(
    r'under\s+the\s+symbol[s]?\s+'
    r'[“"\']\s*([A-Z][A-Z0-9.-]{0,5})\s*[”"\']',
    re.IGNORECASE,
)

# Exchange identification
_EXCHANGE_RE = re.compile(
    r'\b(New York Stock Exchange|NYSE\s*American'
    r'|NYSE\s*Texas|NYSE|Nasdaq[^\s]*\s*'
    r'(?:Global Select|Global|Capital)?\s*'
    r'(?:Market)?|NASDAQ)\b'
)


def find_offer_price(clean: str) -> float:
    """Per-share offer price.

    Tries "Public offering price" first, then block
    "agreed to purchase ... at $X" phrasing, then the
    traditional 3-column "Per Share" table row.
    Returns 0.0 if not found (e.g. preliminary).
    """
    for pat in (_OFFER_PX_RE, _BLOCK_PX_RE, _PER_SHARE_TABLE_RE):
        m = pat.search(clean[:15000])
        if m:
            return float(m.group(1))
    return 0.0


def find_total(clean: str) -> float:
    """Gross proceeds.

    Tries the public-offering-price row, then the
    "Total" row of a 3-column table, then block-deal
    "will result in $X" / "aggregate proceeds of $X"
    phrasing — including "$96.0 million" suffix form.
    """
    for pat in (_TOTAL_RE, _TOTAL_TABLE_RE, _BLOCK_TOTAL_RE):
        m = pat.search(clean[:15000])
        if not m:
            continue
        val = float(m.group(1).replace(',', ''))
        if val > 1_000_000:
            return val
    # M/B suffix fallback
    m = _BLOCK_TOTAL_M_RE.search(clean[:15000])
    if m:
        base = float(m.group(1))
        scale = 1_000_000 if m.group(2).lower() == 'million' else 1_000_000_000
        return base * scale
    return 0.0


def find_last_price(clean: str) -> tuple[float, str]:
    """Last-reported-sale-price + date (best-effort).

    Date is normalized to ISO if parseable, else ''.
    """
    px_m = (
        _LAST_PX_RE.search(clean[:15000])
        or _LAST_PX_RE_FALLBACK.search(clean[:15000])
    )
    if not px_m:
        return 0.0, ''
    price = float(px_m.group(1))
    # Sanity: per-share equity prices are between
    # $0.50 and $10,000.
    if price < 0.50 or price > 10_000:
        return 0.0, ''
    dt_m = _LAST_PX_DATE_RE.search(clean[:15000])
    if not dt_m:
        return price, ''
    month, day, year = dt_m.group(1), dt_m.group(2), dt_m.group(3)
    import datetime as dt
    for fmt in ('%B %d %Y', '%b %d %Y'):
        try:
            d = dt.datetime.strptime(
                f'{month} {day} {year}', fmt
            ).date()
            return price, d.isoformat()
        except ValueError:
            continue
    return price, ''


def find_title_shares(clean: str) -> int:
    """Largest comma-separated 'N shares' in first
    3000 chars (the title region)."""
    best = 0
    for m in _TITLE_SHARES_RE.finditer(clean[:3000]):
        n = int(m.group(1).replace(',', ''))
        if n > best:
            best = n
    return best


def find_issuer_shares(clean: str) -> int:
    m = _ISSUER_SHARES_RE.search(clean[:8000])
    return int(m.group(1).replace(',', '')) if m else 0


def find_ssh(clean: str) -> tuple[bool, int]:
    """(present, share_count). Count may be 0 even when
    present (filings don't always disclose the split
    in the cover-page sentence)."""
    if not _SSH_PRESENT_RE.search(clean[:8000]):
        return False, 0
    m = _SSH_RE.search(clean[:8000])
    if m:
        return True, int(m.group(1).replace(',', ''))
    return True, 0


def find_ticker(clean: str) -> str:
    m = _TICKER_RE.search(clean[:8000])
    return m.group(1).upper() if m else ''


def find_exchange(clean: str) -> str:
    m = _EXCHANGE_RE.search(clean[:8000])
    if not m:
        return ''
    raw = m.group(1)
    # Normalize: collapse whitespace
    return re.sub(r'\s+', ' ', raw).strip()


def is_preliminary(clean: str) -> bool:
    """Preliminary 424B5/B7 lead with 'Subject to
    completion'. The final pricing supplement omits
    that phrase."""
    return 'subject to completion' in clean[:5000].lower()


def is_bought_deal(clean: str) -> bool:
    """Firm-commitment underwriting language anywhere
    in the body (the Underwriting section can be
    deep in the doc for resale prospectuses)."""
    low = clean.lower()
    return (
        'agreed to purchase' in low
        or 'committed to purchase' in low
        or 'underwriter is committed' in low
        or 'underwriter purchases the shares' in low
        or 'underwriters purchase the shares' in low
    )


def is_ipo(clean: str) -> bool:
    return 'initial public offering' in clean[:15000].lower()


# --- Shared dispatcher ---


def _is_non_equity(low: str) -> bool:
    """Reject filings that aren't common-stock blocks:
    debt notes, ATM, shelf base, preferred (excluding
    American Depositary Shares)."""
    if 'notes' in low[:2000] and 'shares' not in low[:2000]:
        return True
    if 'at the market' in low or 'at-the-market' in low:
        return True
    if 'may from time to time' in low[:3000]:
        return True
    title = low[:3000]
    if 'trust preferred' in title or 'series of preferred' in title:
        return True
    if 'depositary shares' in title and 'american depositary' not in title:
        return True
    return False


def parse_supplement(
    raw: bytes, form_type: str,
) -> RegFiling | None:
    """Generic 424B* cover-page extraction.

    Used by all per-form parsers (424B2/3/4/5/7).
    Returns None for non-equity filings.
    """
    text = decode_raw(raw)
    clean = strip_html(text)
    low = clean[:10000].lower()

    if _is_non_equity(low):
        return None

    f = RegFiling(form_type=form_type)
    f.shares_offered = find_title_shares(clean)
    if f.shares_offered == 0:
        f.missing.append('shares_offered')

    f.offer_price = find_offer_price(clean)
    if f.offer_price == 0.0:
        f.missing.append('offer_price')

    f.total = find_total(clean)
    if f.total == 0.0:
        f.missing.append('total')

    f.last_price, f.last_price_date = find_last_price(clean)
    if f.last_price == 0.0:
        f.missing.append('last_price')

    f.issuer_shares = find_issuer_shares(clean)
    has_ssh, ssh_n = find_ssh(clean)
    f.has_selling_stockholder = has_ssh
    f.ssh_shares = ssh_n

    f.ticker = find_ticker(clean)
    if not f.ticker:
        f.missing.append('ticker')
    f.exchange = find_exchange(clean)
    if not f.exchange:
        f.missing.append('exchange')

    f.is_preliminary = is_preliminary(clean)
    f.is_ipo = is_ipo(clean)
    f.is_bought = is_bought_deal(clean)

    return f
