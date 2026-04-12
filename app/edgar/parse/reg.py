"""Parse registered offering filings (424B2/B3/B5/B7,
424B4) from EDGAR.

These are HTML prospectus supplements. We extract
shares, price, total, seller type, and underwriter
from the cover page.
"""

import html
import re
from dataclasses import dataclass

# Form types to scan
REG_FORMS = ('424B2', '424B3', '424B4', '424B5', '424B7')

# Bank CIK symbols to skip for 424B2
# (structured note issuers that flood the index)
BANK_SYMS = frozenset([
    'RY', 'BNS', 'TD', 'CM', 'BMO',
    'GS', 'MS', 'JPM', 'BAC', 'C',
    'WFC', 'USB', 'PNC', 'TFC', 'SCHW',
    'HSBC', 'BCS', 'DB', 'NMR', 'UBS',
    'CS', 'MUFG',
])

KNOWN_BANKS = [
    'J.P. Morgan', 'JPMorgan',
    'Goldman Sachs & Co. LLC',
    'Goldman Sachs & Co.',
    'Morgan Stanley', 'BofA Securities',
    'Barclays', 'Citigroup',
    'Deutsche Bank', 'Jefferies',
    'RBC Capital Markets',
    'Wells Fargo Securities',
    'Piper Sandler', 'Raymond James',
    'Stephens Inc.', 'Oppenheimer & Co.',
    'UBS', 'TD Securities', 'BTIG',
    'Evercore ISI',
    'Cantor Fitzgerald', 'Cantor',
    'TD Cowen', 'Truist Securities',
    'KeyBanc', 'William Blair',
    'Needham', 'Canaccord Genuity',
    'Leerink Partners',
    'Guggenheim Securities',
    'Nomura', 'Mizuho', 'Stifel',
    'BMO Capital', 'Baird', 'Moelis',
]

# Shares regex: number followed by up to 3 words
# then [Ss]hares. Handles "Ordinary Shares",
# "Class A Common Shares", etc.
_SHARES_RE = re.compile(
    r'(?<!\$)\b([\d,]{4,})\s+'
    r'(?:[\w]+\s+){0,3}[Ss]hares'
)

# Price in pricing table (unicode-whitespace aware)
_OFFER_PX_RE = re.compile(
    r'[Pp]ublic offering price[\s\S]{0,200}?'
    r'\$[\s\u200b\u00a0]*([\d]+\.[\d]{2})'
)

# Last reported / closing price
_LAST_PX_RE = re.compile(
    r'(?:last reported|closing) '
    r'(?:sales? )?price[\s\S]{0,60}?'
    r'\$[\s\u200b\u00a0]*([\d]+\.[\d]{2})'
)

# Total from pricing table (second $ amount after
# per-share price, must be > $1M)
_TOTAL_RE = re.compile(
    r'[Pp]ublic offering price[\s\S]{0,200}?'
    r'\$[\s\u200b\u00a0]*[\d]+\.[\d]{2}'
    r'[\s\S]{0,80}?'
    r'\$[\s\u200b\u00a0]*([\d,]+\.[\d]{2})'
)

_SELLER_RE = re.compile(
    r'selling (?:stockholder|shareholder)'
)

# Lock-up: "X days after/following the date of this prospectus"
_LOCKUP_DAYS_RE = re.compile(
    r'(\d{2,3})[\s-]?days?\s+'
    r'(?:after|following|from)\s+'
    r'(?:the\s+)?(?:date\s+of\s+)?'
    r'th(?:is|e)\s+prospectus',
    re.IGNORECASE,
)
_LOCKUP_PERIOD_RE = re.compile(
    r'period of\s+(\d{2,3})[\s-]?days?',
    re.IGNORECASE,
)
_LOCKUP_RESTRICT_RE = re.compile(
    r'(\d{2,3})[\s-]?day\s+'
    r'(?:restricted|lock[\s-]?up)',
    re.IGNORECASE,
)
_LOCKUP_HEADER_RE = re.compile(
    r'\b[Ll]ock[\s-]?[Uu]p\s+[Aa]greements?\b'
)


@dataclass
class FilingReg:
    shares: int
    offer_price: float
    last_price: float
    total: float
    is_seller: bool
    is_bought: bool  # underwriter bought the shares
    is_ipo: bool
    underwriter: str
    lockup: bool = False
    lockup_days: int = 0


def _strip_html(text: str) -> str:
    """Strip tags, decode entities, collapse ws."""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = html.unescape(clean)
    return re.sub(r'\s+', ' ', clean)


def _find_underwriters(clean: str) -> str:
    """Extract underwriter names from filing."""
    # Strategy 1: labeled section
    for pat in [
        r'(?:Joint\s+)?(?:Lead\s+)?'
        r'Book[\s-]*[Rr]unning\s+'
        r'[Mm]anagers?\s+(.*?)'
        r'(?:The date|Co-Manager)',
        r'(?:Joint\s+)?(?:Lead\s+)?'
        r'Bookrunners?\s+(.*?)'
        r'(?:The date|Co-Manager)',
        r'Sole\s+Book[\s-]*[Rr]unning\s+'
        r'[Mm]anager\s+(.*?)'
        r'(?:The date|Co-Manager)',
    ]:
        m = re.search(
            pat, clean[:15000], re.IGNORECASE
        )
        if m:
            raw = re.sub(
                r'\s+', ' ', m.group(1).strip()
            )
            for stop in [
                'Table of Contents',
                'Prospectus supplement',
                'Page ',
            ]:
                si = raw.find(stop)
                if si > 0:
                    raw = raw[:si].strip()
            return raw[:200]

    # Strategy 2: known bank names clustered
    # before "The date of this prospectus
    # supplement is"
    di = clean.lower().find(
        'the date of this prospectus '
        'supplement is'
    )
    if di > 0:
        chunk = clean[max(0, di - 400) : di]
        found = [
            (b, chunk.index(b))
            for b in KNOWN_BANKS
            if b in chunk
        ]
        if found:
            found.sort(key=lambda x: x[1])
            return ', '.join(b for b, _ in found)

    return ''


def parse_reg(
    raw: bytes,
) -> FilingReg | None:
    """Parse a registered offering filing.

    Returns None if the filing is not an equity
    offering (debt, ATM, shelf base prospectus).
    """
    text = raw.decode('latin-1')
    clean = _strip_html(text)
    low = clean[:10000].lower()

    # Reject debt/notes without shares
    if (
        'notes' in low[:2000]
        and 'shares' not in low[:2000]
    ):
        return None

    # Reject ATM programs
    if (
        'at the market' in low
        or 'at-the-market' in low
    ):
        return None

    # Reject shelf base prospectuses
    if 'may from time to time' in low[:3000]:
        return None

    # Extract share count
    m = _SHARES_RE.search(clean[:5000])
    if not m:
        return None
    shares = int(m.group(1).replace(',', ''))
    if shares == 0:
        return None

    is_seller = bool(
        _SELLER_RE.search(low[:5000])
    )

    # Bought deal: underwriter purchased shares
    low_full = clean[:20000].lower()
    is_bought = 'agreed to purchase' in low_full
    is_ipo = (
        'initial public offering' in low_full
    )

    # Offer price
    price = 0.0
    pm = _OFFER_PX_RE.search(clean[:8000])
    if pm:
        price = float(pm.group(1))

    # Last reported price (fallback)
    last_price = 0.0
    lm = _LAST_PX_RE.search(low[:5000])
    if lm:
        last_price = float(lm.group(1))

    # Total from pricing table
    total = 0.0
    tm = _TOTAL_RE.search(clean[:8000])
    if tm:
        t = float(tm.group(1).replace(',', ''))
        if t > 1_000_000:
            total = t

    underwriter = _find_underwriters(clean)

    # Lock-up extraction
    lockup = False
    lockup_days = 0
    lm = _LOCKUP_HEADER_RE.search(clean)
    if lm:
        lockup = True
        win = clean[
            max(0, lm.start() - 500) :
            lm.start() + 3000
        ]
        dm = (
            _LOCKUP_DAYS_RE.search(win)
            or _LOCKUP_PERIOD_RE.search(win)
            or _LOCKUP_RESTRICT_RE.search(win)
        )
        if dm:
            d = int(dm.group(1))
            if 30 <= d <= 365:
                lockup_days = d

    return FilingReg(
        shares=shares,
        offer_price=price,
        last_price=last_price,
        total=total,
        is_seller=is_seller,
        is_bought=is_bought,
        is_ipo=is_ipo,
        underwriter=underwriter,
        lockup=lockup,
        lockup_days=lockup_days,
    )
