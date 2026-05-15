"""GDELT 2.0 DOC API — free, no-auth global news firehose.

Used for the discovery side of the pipeline: pull recent US-sourced articles
matching sector keywords, then let the ticker extractor find symbols in them.

Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests

from ingest.types import Article

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
TIMEOUT = 30

# Sector keyword bundles — broad on purpose; the ticker extractor filters down.
SECTOR_QUERIES: dict[str, str] = {
    "energy": (
        '(oil OR "natural gas" OR LNG OR refinery OR shale OR OPEC OR pipeline '
        'OR "renewable energy" OR solar OR "wind power") sourcecountry:US'
    ),
    "healthcare": (
        '(pharma OR biotech OR FDA OR "drug approval" OR "clinical trial" '
        'OR Medicare OR Medicaid OR "health insurer" OR hospital) sourcecountry:US'
    ),
    "minerals": (
        '(copper OR "iron ore" OR steel OR aluminum OR gold OR "rare earth" '
        'OR mining OR lithium OR nickel) sourcecountry:US'
    ),
    "tech": (
        '("artificial intelligence" OR semiconductor OR cloud OR software '
        'OR chip OR "data center" OR cybersecurity OR SaaS) sourcecountry:US'
    ),
    "real_estate": (
        '(REIT OR "commercial real estate" OR mortgage OR housing '
        'OR "data center" OR "self storage" OR apartment) sourcecountry:US'
    ),
    "finance": (
        '(bank OR "interest rate" OR Fed OR "credit card" OR brokerage '
        'OR "asset management" OR "investment bank" OR fintech) sourcecountry:US'
    ),
}


def _parse_iso_or_yyyymmdd(s: str) -> datetime:
    """GDELT returns either '20260514T120000Z' or ISO. Handle both."""
    s = s.strip()
    if "T" in s and s.endswith("Z") and "-" not in s:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def search(query: str, max_records: int = 75, hours_back: int = 24,
           max_retries: int = 3) -> list[Article]:
    """Run a GDELT DOC query with simple backoff. Returns Articles."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours_back)
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": max_records,
        "startdatetime": start.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end.strftime("%Y%m%d%H%M%S"),
        "sort": "DateDesc",
    }
    for attempt in range(max_retries):
        r = requests.get(BASE, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            break
        if r.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"[gdelt] 429 rate-limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        print(f"[gdelt] HTTP {r.status_code} on query: {query[:60]}")
        return []
    else:
        return []
    try:
        data = r.json()
    except Exception:
        # GDELT sometimes returns HTML error pages
        return []
    out: list[Article] = []
    for item in data.get("articles", []):
        try:
            pub = _parse_iso_or_yyyymmdd(item.get("seendate", ""))
        except Exception:
            continue
        out.append(Article(
            source="gdelt",
            source_outlet=item.get("domain", "") or "",
            headline=item.get("title", "") or "",
            summary="",  # GDELT DOC doesn't include bodies; title is what we get
            url=item.get("url", "") or "",
            published_at=pub,
            tickers=[],  # filled by the extractor downstream
            language=item.get("language", "English"),
        ))
    return out


def fetch_by_sector(sectors: Iterable[str], hours_back: int = 24,
                    max_per_sector: int = 75,
                    sleep_between: float = 5.0) -> dict[str, list[Article]]:
    """Pull one query per sector with a small inter-call delay (rate-limit-friendly)."""
    out: dict[str, list[Article]] = {}
    sectors = list(sectors)
    for i, sec in enumerate(sectors):
        q = SECTOR_QUERIES.get(sec)
        if not q:
            out[sec] = []
            continue
        out[sec] = search(q, max_records=max_per_sector, hours_back=hours_back)
        print(f"[gdelt] {sec}: {len(out[sec])} articles")
        if i < len(sectors) - 1:
            time.sleep(sleep_between)
    return out
