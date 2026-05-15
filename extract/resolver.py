"""US ticker universe + name resolver.

Builds a cache of all US-listed stocks (~8000 tickers) from Finnhub.
Provides:
- `extract_tickers(text)`: finds tickers in raw text via cashtags ($XYZ) and
  company-name mentions.
- `get_universe()`: dict of ticker -> {name, exchange, mic}
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import requests

from config.settings import require_key

UNIVERSE_CACHE = Path(__file__).resolve().parent / "us_universe.json"
CACHE_TTL_DAYS = 7

# Cashtag like "$NVDA" — 1-5 uppercase letters after $.
CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")

# Single-word company names that match too aggressively as plain English.
# These tickers are still findable via $cashtag, just not via bare-name mention.
GENERIC_NAME_BLOCKLIST = {
    "NEWS", "BLOCK", "DATA", "POWER", "ENERGY", "BANK", "TRUST", "CAPITAL",
    "GROWTH", "INCOME", "GLOBAL", "STATE", "FIRST", "UNITED", "NATIONAL",
    "GENERAL", "AMERICAN", "INTERNATIONAL", "COMMUNITY", "STANDARD", "DIRECT",
    "PRIME", "OPEN", "CORE", "FREE", "REAL", "GOOD", "PURE", "SIMPLE",
    "MARKET", "SQUARE", "SOLO", "ONE", "MEDIA", "VITAL", "ELITE", "TODAY",
    "TOMORROW", "FUTURE", "SAFE", "SMART", "BRIGHT", "STRONG",
    "HERE", "POST", "WORLD", "FACT", "POINT", "PATH", "WAVE", "EDGE",
    "DRIVE", "PEAK", "RISE", "GAIN", "LEAP", "BOLD", "PIVOT", "FOCUS",
}

# Words that look like tickers but are usually English. Excludes them from
# bare-symbol matches (rare path; we mainly match by name, not bare letters).
COMMON_FALSE_POSITIVES = {
    "A", "I", "AN", "AT", "BE", "BY", "DO", "GO", "HE", "IF", "IN", "IS", "IT",
    "ME", "MY", "NO", "OF", "ON", "OR", "SO", "TO", "UP", "US", "WE", "ALL",
    "AND", "ANY", "ARE", "BUT", "CAN", "FOR", "GET", "HAD", "HAS", "HER", "HIM",
    "HIS", "HOW", "ITS", "MAY", "NEW", "NOT", "NOW", "OLD", "ONE", "OUR", "OUT",
    "SEE", "SHE", "TWO", "USE", "WAS", "WAY", "WHO", "WHY", "YOU", "BIG", "CEO",
    "USA", "INC", "LLC", "LTD", "FDA", "SEC", "API", "AI", "IT", "PR", "TV",
}

# Company-name suffixes to strip before building the lookup index.
# Matched case-INsensitively against the trailing portion of the name.
NAME_SUFFIXES = [
    " inc.", " inc", " incorporated", " corporation", " corp.", " corp",
    " co.", " co", " company", " holdings", " holding", " group", " plc",
    " ltd.", " ltd", " limited", " llc", " n.v.", " s.a.", " s.a.b.", " ag",
    " adr", " adrs", " class a", " class b", " class c", " common stock",
    " ordinary shares", " ordinary share",
]


def _fetch_finnhub_universe() -> list[dict]:
    """One-time pull of all US-listed symbols."""
    token = require_key("finnhub")
    url = "https://finnhub.io/api/v1/stock/symbol"
    r = requests.get(url, params={"exchange": "US", "token": token}, timeout=30)
    r.raise_for_status()
    return r.json()


_CLASS_SHARE_RE = re.compile(
    r"\s*[-/]?\s*("
    r"class\s+[a-z]|"
    r"inc[-\s]+(class|cl)\s+[a-z](\s+shares?)?|"
    r"-\s*(class|cl)\s+[a-z]|"
    r"-\s*[a-z]$|"
    r"common\s+stock|"
    r"ordinary\s+shares?|"
    r"preferred\s+(stock|shares?)"
    r")\s*$",
    re.IGNORECASE,
)


def _clean_name(name: str) -> str:
    name = name.strip()
    # Strip class/share designators first.
    name = _CLASS_SHARE_RE.sub("", name).strip()
    # Strip suffixes case-insensitively, iteratively.
    changed = True
    while changed:
        changed = False
        lower = name.lower()
        for suf in NAME_SUFFIXES:
            if lower.endswith(suf):
                name = name[: -len(suf)].strip()
                changed = True
                break
        # Strip trailing punctuation like "&", ",", "."
        stripped = name.rstrip(" &,./-")
        if stripped != name:
            name = stripped
            changed = True
    return name.strip()


def _load_or_fetch_universe() -> list[dict]:
    if UNIVERSE_CACHE.exists():
        age = datetime.now() - datetime.fromtimestamp(UNIVERSE_CACHE.stat().st_mtime)
        if age < timedelta(days=CACHE_TTL_DAYS):
            with UNIVERSE_CACHE.open() as f:
                return json.load(f)
    raw = _fetch_finnhub_universe()
    UNIVERSE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with UNIVERSE_CACHE.open("w") as f:
        json.dump(raw, f)
    return raw


REAL_EXCHANGES = {"XNAS", "XNYS", "BATS", "XASE"}  # Nasdaq, NYSE, Cboe BZX, NYSE American
ALLOWED_TYPES = {"Common Stock", "ADR", "REIT"}


@lru_cache(maxsize=1)
def get_universe() -> dict[str, dict]:
    """Return {ticker: {name, exchange, type}} for *real-exchange* US listings.

    Filters out OTC, warrants, units, closed-end funds — keeps Common Stock,
    ADRs, and REITs on NYSE/Nasdaq/Cboe/NYSE-American.
    """
    raw = _load_or_fetch_universe()
    out: dict[str, dict] = {}
    for row in raw:
        sym = (row.get("symbol") or "").strip().upper()
        if not sym or "." in sym or "-" in sym:
            continue
        if row.get("mic", "") not in REAL_EXCHANGES:
            continue
        if row.get("type", "") not in ALLOWED_TYPES:
            continue
        out[sym] = {
            "name": row.get("description", "") or row.get("displaySymbol", ""),
            "description": row.get("description", ""),
            "exchange": row.get("mic", ""),
            "type": row.get("type", ""),
        }
    return out


@lru_cache(maxsize=1)
def _name_index() -> list[tuple[re.Pattern, str]]:
    """Build list of (compiled_regex, ticker) sorted by name length descending.

    Long names match first so 'Bank of America' isn't shadowed by a 'Bank' match.
    """
    universe = get_universe()
    entries: list[tuple[str, str]] = []
    for ticker, info in universe.items():
        name = _clean_name(info.get("name") or "")
        if not name or len(name) < 4:
            continue
        # Drop single-word names that are too generic (e.g. NWSA → "NEWS").
        if " " not in name and name.upper() in GENERIC_NAME_BLOCKLIST:
            continue
        entries.append((name, ticker))
    entries.sort(key=lambda x: len(x[0]), reverse=True)
    compiled = [
        (re.compile(rf"(?<![\w]){re.escape(name)}(?![\w])", re.IGNORECASE), tk)
        for name, tk in entries
    ]
    return compiled


def extract_cashtags(text: str) -> set[str]:
    """Find $XYZ patterns. Filter to tickers actually in the universe."""
    universe = get_universe()
    found = set(m.group(1).upper() for m in CASHTAG_RE.finditer(text or ""))
    return {t for t in found if t in universe and t not in COMMON_FALSE_POSITIVES}


def extract_by_name(text: str, max_matches: int = 5) -> set[str]:
    """Find company-name mentions and return their tickers.

    Index is sorted longest-name-first; when a longer name matches a span of
    text, that span is masked so a shorter sub-name (e.g. ticker MGHL clean
    name 'MORGAN') cannot also match inside the same span (e.g. inside MS
    clean name 'MORGAN STANLEY').
    """
    if not text:
        return set()
    matched: set[str] = set()
    # Track [start, end) ranges already covered by an accepted match.
    covered: list[tuple[int, int]] = []

    def overlaps_any(span: tuple[int, int]) -> bool:
        s, e = span
        for cs, ce in covered:
            if s < ce and cs < e:
                return True
        return False

    for pattern, ticker in _name_index():
        if len(matched) >= max_matches:
            break
        m = pattern.search(text)
        if not m:
            continue
        span = m.span()
        if overlaps_any(span):
            continue
        matched.add(ticker)
        covered.append(span)
    return matched


def extract_tickers(text: str) -> set[str]:
    """Cashtags ∪ name mentions. Universe-bounded."""
    return extract_cashtags(text) | extract_by_name(text)
