"""Shorten holder/filer names for display."""

CORP_SUFFIXES = [
    'inc', 'corp', 'ltd', 'plc', 'na', 'nv',
    'compan', 'equities',
    'limited', 'public', 'holding',
]

CORP_EXACT = {'sa', 'se', 'ag', 'co'}

FUND_SUFFIXES = [
    'llc', 'lp', 'llp', 'advisors',
    'advisory', 'management', 'partners',
    'investments', 'securities',
    'asset', 'wealth', 'financial',
]

STOP_AFTER = [
    'group', 'capital', 'fund', 'trust',
    'international', 'investors',
]

ALL_SUFFIXES = (
    CORP_SUFFIXES + FUND_SUFFIXES
)


def _is_suffix(token: str) -> bool:
    t = token.lower().rstrip(',.')
    if t in CORP_EXACT:
        return True
    if t in ('l', 'de', '/de/',
             '/adv', '/ca/', '/md/', '/il/',
             '/ny/', '/ct/', '/nc/',
             'the', 'ii', 'iii', 'iv', 'vi',
             'i/ii', 'gp'):
        return True
    for s in ALL_SUFFIXES:
        if t.startswith(s):
            return True
    return False


def _is_connective(token: str) -> bool:
    return token.lower() in (
        '&', 'and', 'of', 'a', 'for', 'the',
    )


def short_name(raw: str) -> str:
    """Shorten a holder name for display.

    'VANGUARD GROUP INC' -> 'Vanguard'
    'BlackRock, Inc.' -> 'BlackRock'
    'BANK OF AMERICA CORP /DE/'
      -> 'Bank of America'
    'GOLDMAN SACHS GROUP INC'
      -> 'Goldman Sachs'
    """
    clean = raw.split(',')[0].strip()
    tokens = (
        clean.replace('.com', '')
        .replace('.', '')
        .split()
    )
    if not tokens:
        return raw

    if tokens[0].lower() == 'the':
        tokens.pop(0)
    if not tokens:
        return raw

    if (
        len(tokens) == 2
        and tokens[1].lower().startswith('corp')
    ):
        return f'{tokens[0]} Corp'

    name = [tokens.pop(0)]
    for i, token in enumerate(tokens[:6]):
        if _is_connective(token):
            rest = tokens[i + 1 : i + 3]
            if rest and not _is_suffix(rest[0]):
                name.append(token)
                continue
            break
        if _is_suffix(token):
            break
        t = token.lower().rstrip(',.')
        if any(t.startswith(s) for s in STOP_AFTER):
            name.append(token)
            break
        name.append(token)

    while (
        len(name) > 1
        and _is_connective(name[-1])
    ):
        name.pop()

    result = ' '.join(name)

    if result.isupper() and len(result) > 4:
        result = result.title()

    return result
