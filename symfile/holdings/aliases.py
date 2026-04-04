"""Holder name aliases for 13D -> 13F matching.

Maps 13D reporting person names to their 13F
filing manager names. Used to overlay 13D
ownership data onto 13F holdings.
"""

import re

ALIASES: dict[str, str] = {
    'KKR Group Partnership L.P.':
        'KKR & CO INC',
    'GAMCO Asset Management Inc.':
        'GAMCO INVESTORS, INC. ET AL',
    'ICAHN ENTERPRISES HOLDINGS L.P.':
        'ICAHN CARL C',
    'Icahn Enterprises L.P.':
        'ICAHN CARL C',
    'Icahn Capital LP':
        'ICAHN CARL C',
    'Carl C. Icahn':
        'ICAHN CARL C',
    'BVF PARTNERS L P/IL':
        'BVF INC/IL',
    'Glencore International AG':
        'Glencore plc',
    'Glencore AG':
        'Glencore plc',
    'Luther King Capital Management Corporation':
        'KING LUTHER CAPITAL MANAGEMENT CORP',
    'ValueAct Capital Management, L.P.':
        'ValueAct Holdings, L.P.',
    'General Electric Company':
        'General Electric Co',
    'Blackstone Holdings I/II GP L.L.C.':
        'Blackstone Inc.',
    'NIPPON LIFE INSURANCE COMPANY':
        'NIPPON LIFE INSURANCE CO',
    'Nippon Life Insurance Company':
        'NIPPON LIFE INSURANCE CO',
    'Deutsche Telekom AG':
        'DEUTSCHE TELEKOM AG',
    'Liberty Broadband Corporation':
        'LIBERTY BROADBAND CORP',
    'Liberty Live Holdings, Inc.':
        'LIBERTY MEDIA CORP /DE/',
    'Cascade Investment, L.L.C.':
        'CASCADE INVESTMENT LLC',
    'BlackRock Portfolio Management LLC':
        'BlackRock, Inc.',
    'Apollo Principal Holdings A GP, Ltd.':
        'Apollo Management Holdings, L.P.',
    'OEP AHCO Investment Holdings, LLC':
        'OEP CAPITAL ADVISORS, L.P.',
    'Occidental Petroleum Corporation':
        'OCCIDENTAL PETROLEUM CORP /DE/',
    'Diamondback Energy, Inc.':
        'DIAMONDBACK ENERGY INC',
    'Delek US Holdings, Inc.':
        'DELEK US HOLDINGS INC /DE/',
    'Nelson Peltz':
        'TRIAN FUND MANAGEMENT, L.P.',
    '3G Restaurant Brands Holdings '
    'General Partner LLC':
        '3G Capital Partners LP',
    'V. PREM WATSA':
        'FAIRFAX FINANCIAL HOLDINGS LTD/ CAN',
    'Ronald Baron':
        'BARON CAPITAL GROUP INC',
    'Felix J. Baker':
        'BAKER BROS. ADVISORS LP',
    'Julian C. Baker':
        'BAKER BROS. ADVISORS LP',
    'Warren E. Buffett':
        'Berkshire Hathaway Inc',
}


def normalize(s: str) -> str:
    s = s.upper()
    s = re.sub(r'[,./\-\'\"()]', ' ', s)
    for suffix in [
        ' LLC', ' LP', ' L P', ' INC',
        ' CORP', ' LTD', ' CO', ' PLC',
        ' SA', ' NV', ' AG', ' SE',
        ' L L C', ' L P ', ' THE',
    ]:
        s = s.replace(suffix, '')
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def build_matcher(
    holders_13f: list[str],
) -> dict[str, str]:
    """Build 13D name -> 13F name lookup.

    Combines exact, normalized, prefix, and
    manual alias matching.
    """
    norm_map: dict[str, str] = {}
    for h in holders_13f:
        norm_map[normalize(h)] = h

    def match(name_13d: str) -> str | None:
        if name_13d in ALIASES:
            target = ALIASES[name_13d]
            if target in set(holders_13f):
                return target
            n = normalize(target)
            if n in norm_map:
                return norm_map[n]

        n = normalize(name_13d)
        if n in norm_map:
            return norm_map[n]

        tokens = n.split()
        if len(tokens) >= 3:
            prefix = ' '.join(tokens[:3])
            cands = [
                v for k, v in norm_map.items()
                if k.startswith(prefix)
            ]
            if len(cands) == 1:
                return cands[0]

        return None

    return match
