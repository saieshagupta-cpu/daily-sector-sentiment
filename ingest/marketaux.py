"""Marketaux news client. Free tier: 100 req/day, 3 articles per request.

Two ways to use it:
- Search by ticker (high precision, taggable to specific names)
- Search by ticker-discovery mode (broad keyword + countries filter) — for
  finding *new* tickers we don't already track.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

import requests

from config.settings import require_key
from ingest.types import Article

BASE = "https://api.marketaux.com/v1"
TIMEOUT = 15


def _get(path: str, **params) -> dict:
    params["api_token"] = require_key("marketaux")
    r = requests.get(f"{BASE}{path}", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _parse_articles(raw: dict) -> list[Article]:
    out: list[Article] = []
    for item in raw.get("data", []):
        try:
            published = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        tickers = [e.get("symbol", "") for e in item.get("entities", []) if e.get("symbol")]
        out.append(Article(
            source="marketaux",
            source_outlet=item.get("source", "") or "",
            headline=item.get("title", "") or "",
            summary=item.get("description", "") or item.get("snippet", "") or "",
            url=item.get("url", "") or "",
            published_at=published,
            tickers=[t for t in tickers if t],
        ))
    return out


def news_for_tickers(tickers: Iterable[str], limit: int = 3) -> list[Article]:
    """Tickers must already be known. Limit caps per-request article count."""
    sym = ",".join(tickers)
    raw = _get("/news/all", symbols=sym, language="en", filter_entities="true", limit=limit)
    return _parse_articles(raw)


def discover_us_news(industries: str | None = None, limit: int = 3) -> list[Article]:
    """Broad pull — surfaces tickers *not* in our blue-chip list.

    `industries` is a comma-separated Marketaux industry string, e.g.
    'Technology,Energy'. Leave None for the full firehose.
    """
    params = dict(countries="us", language="en", filter_entities="true", limit=limit)
    if industries:
        params["industries"] = industries
    raw = _get("/news/all", **params)
    return _parse_articles(raw)
