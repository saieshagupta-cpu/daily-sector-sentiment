"""Map Finnhub industry labels to our 6 internal sectors.

Finnhub returns a free-text industry per ticker. We normalise that into our
canonical sectors. Anything that doesn't fit returns None (filtered out).
"""
from __future__ import annotations

# Lowercase Finnhub industry -> our sector
_FINNHUB_TO_SECTOR: dict[str, str] = {
    # Energy
    "oil & gas": "energy",
    "energy": "energy",
    "utilities": "energy",
    # Healthcare
    "pharmaceutical": "healthcare",
    "pharmaceuticals": "healthcare",
    "biotechnology": "healthcare",
    "health care": "healthcare",
    "healthcare": "healthcare",
    "medical devices": "healthcare",
    "life sciences tools & services": "healthcare",
    # Minerals / metals
    "metals & mining": "minerals",
    "mining": "minerals",
    "steel": "minerals",
    "chemicals": "minerals",
    "materials": "minerals",
    # Tech
    "technology": "tech",
    "semiconductors": "tech",
    "software": "tech",
    "hardware": "tech",
    "internet": "tech",
    "communications": "tech",
    "media": "tech",
    # Real Estate
    "real estate": "real_estate",
    "reit": "real_estate",
    "real estate investment trusts": "real_estate",
    # Finance
    "banking": "finance",
    "banks": "finance",
    "financial services": "finance",
    "insurance": "finance",
    "capital markets": "finance",
    "asset management": "finance",
    "diversified financial services": "finance",
}


def map_industry(industry: str | None) -> str | None:
    if not industry:
        return None
    s = industry.strip().lower()
    # Exact match first
    if s in _FINNHUB_TO_SECTOR:
        return _FINNHUB_TO_SECTOR[s]
    # Contains-match fallback
    for key, sector in _FINNHUB_TO_SECTOR.items():
        if key in s:
            return sector
    return None
