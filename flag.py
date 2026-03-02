"""Country and flag utilities with typo-tolerant matching and number-based detection."""
import difflib
import re
from country import COUNTRIES

# Build flag dictionary from country data
FLAG_MAP = {}
for country in COUNTRIES:
    name = country.get('name')
    flag = country.get('flag')
    if name and flag:
        FLAG_MAP[name] = flag


def _norm(text):
    """Normalize text for robust matching."""
    if text is None:
        return ""
    text = str(text).strip().lower()
    # Keep alnum and spaces only; collapse repeated spaces
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


COUNTRY_BY_NORM_NAME = {}
COUNTRY_BY_ISO2 = {}
COUNTRY_BY_ISO3 = {}
COUNTRIES_BY_CALLING = {}

for c in COUNTRIES:
    name = (c.get("name") or "").strip()
    iso2 = (c.get("iso2") or "").strip().upper()
    iso3 = (c.get("iso3") or "").strip().upper()
    calling = (c.get("calling_code") or "").strip()

    if name:
        COUNTRY_BY_NORM_NAME[_norm(name)] = c
    if iso2:
        COUNTRY_BY_ISO2[iso2] = c
    if iso3:
        COUNTRY_BY_ISO3[iso3] = c
    if calling:
        digits = re.sub(r"\D", "", calling)
        if digits:
            COUNTRIES_BY_CALLING.setdefault(digits, []).append(c)


def resolve_country(country_hint):
    """
    Resolve country using:
    1) exact name (case-insensitive)
    2) ISO2 / ISO3
    3) calling code (+880 / 880)
    4) fuzzy name match
    """
    if not country_hint:
        return None

    raw = str(country_hint).strip()
    norm = _norm(raw)

    # Exact normalized name
    c = COUNTRY_BY_NORM_NAME.get(norm)
    if c:
        return c

    # ISO2/ISO3
    upper = raw.upper()
    if upper in COUNTRY_BY_ISO2:
        return COUNTRY_BY_ISO2[upper]
    if upper in COUNTRY_BY_ISO3:
        return COUNTRY_BY_ISO3[upper]

    # Calling code
    digits = re.sub(r"\D", "", raw)
    if digits and digits in COUNTRIES_BY_CALLING:
        # When many countries share code, use first deterministic entry (sorted list in country.py)
        return COUNTRIES_BY_CALLING[digits][0]

    # Fuzzy by country name
    choices = list(COUNTRY_BY_NORM_NAME.keys())
    if norm and choices:
        match = difflib.get_close_matches(norm, choices, n=1, cutoff=0.78)
        if match:
            return COUNTRY_BY_NORM_NAME[match[0]]

    return None


def _candidates_from_number(number):
    """Return country candidates based on longest matching calling code prefix."""
    digits = re.sub(r"\D", "", str(number or ""))
    if len(digits) < 4:
        return []

    # Longest prefix wins (e.g. 1242 before 1)
    found = []
    max_len = 0
    for code_digits, countries in COUNTRIES_BY_CALLING.items():
        if digits.startswith(code_digits):
            l = len(code_digits)
            if l > max_len:
                max_len = l
                found = countries
            elif l == max_len:
                found = found + countries

    # Deduplicate by country name, keep order
    uniq = []
    seen = set()
    for c in found:
        name = c.get("name")
        if name and name not in seen:
            seen.add(name)
            uniq.append(c)

    # +7 ambiguity handling (RU vs KZ):
    # Kazakhstan mobile prefixes provided by admin:
    # 700, 701, 702, 705, 706, 707, 708, 709, 747, 771, 772
    # Otherwise default to Russia for +7.
    if digits.startswith("7") and len(uniq) > 1:
        by_norm = {_norm(c.get("name", "")): c for c in uniq}
        ru = by_norm.get("russia")
        kz = by_norm.get("kazakhstan")
        if ru and kz:
            kz_prefixes = (
                "700", "701", "702", "705", "706", "707", "708", "709",
                "747", "771", "772",
            )
            if digits.startswith(kz_prefixes):
                return [kz]
            return [ru]

    return uniq


def detect_country_from_numbers(numbers, country_hint=None):
    """
    Detect best country using phone number prefixes.
    If calling code maps to multiple countries, use hint as tiebreaker.
    Returns country dict or None.
    """
    if not numbers:
        return resolve_country(country_hint)

    hint_country = resolve_country(country_hint) if country_hint else None
    hint_name = hint_country.get("name") if hint_country else None
    hint_norm = _norm(hint_name or country_hint or "")

    votes = {}
    for num in numbers:
        candidates = _candidates_from_number(num)
        if not candidates:
            continue

        chosen = None
        if len(candidates) == 1:
            chosen = candidates[0]
        elif hint_norm:
            # Try exact/fuzzy matching hint against ambiguous candidates
            cand_norm_to_country = {_norm(c.get("name", "")): c for c in candidates}
            if hint_norm in cand_norm_to_country:
                chosen = cand_norm_to_country[hint_norm]
            else:
                close = difflib.get_close_matches(hint_norm, list(cand_norm_to_country.keys()), n=1, cutoff=0.72)
                if close:
                    chosen = cand_norm_to_country[close[0]]

        if not chosen:
            # Deterministic fallback for ambiguity
            chosen = sorted(candidates, key=lambda x: (x.get("name") or ""))[0]

        name = chosen.get("name")
        if name:
            votes[name] = votes.get(name, 0) + 1

    if votes:
        best_name = sorted(votes.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        return resolve_country(best_name)

    return resolve_country(country_hint)


def get_flag(country_name):
    """Get flag emoji for a country name
    
    Args:
        country_name (str): Country name (e.g., "Bangladesh", "Romania")
    
    Returns:
        str: Flag emoji or "🌍" if not found
    
    Example:
        >>> get_flag("Bangladesh")
        '🇧🇩'
        >>> get_flag("Unknown")
        '🌍'
    """
    if not country_name:
        return "🌍"

    resolved = resolve_country(country_name)
    if resolved and resolved.get("flag"):
        return resolved["flag"]

    # Fallback legacy behavior
    if country_name in FLAG_MAP:
        return FLAG_MAP[country_name]
    for country, flag in FLAG_MAP.items():
        if country.lower() == str(country_name).lower():
            return flag
    return "🌍"


def canonical_country_name(country_hint, numbers=None):
    """Return best canonical country name from hint + optional number list."""
    if numbers:
        detected = detect_country_from_numbers(numbers, country_hint=country_hint)
        if detected and detected.get("name"):
            return detected["name"]
    resolved = resolve_country(country_hint)
    if resolved and resolved.get("name"):
        return resolved["name"]
    return (country_hint or "").strip()


def get_all_flags():
    """Get all countries with their flags
    
    Returns:
        dict: Dictionary of {country_name: flag_emoji}
    
    Example:
        >>> flags = get_all_flags()
        >>> flags['Bangladesh']
        '🇧🇩'
    """
    return FLAG_MAP.copy()


def flag_by_iso2(iso2_code):
    """Get flag by ISO 3166-1 alpha-2 code
    
    Args:
        iso2_code (str): ISO 2-letter country code (e.g., "BD", "RO")
    
    Returns:
        str: Flag emoji or "🌍" if not found
    
    Example:
        >>> flag_by_iso2("BD")
        '🇧🇩'
    """
    for country in COUNTRIES:
        if country.get('iso2') == iso2_code.upper():
            return country.get('flag', "🌍")
    return "🌍"


# Quick test
if __name__ == '__main__':
    print(f"Loaded {len(FLAG_MAP)} country flags")
    print(f"Bangladesh flag: {get_flag('Bangladesh')}")
    print(f"Romania flag: {get_flag('Romania')}")
    print(f"Unknown flag: {get_flag('Unknown Country')}")
    print(f"\nISO2 lookup - BD: {flag_by_iso2('BD')}")
    print(f"ISO2 lookup - RO: {flag_by_iso2('RO')}")
