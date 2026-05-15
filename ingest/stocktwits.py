"""StockTwits — free, no-auth read endpoints.

We use two endpoints:
- Trending: `/trending/symbols.json` -> tickers being talked about most NOW
- Symbol stream: `/streams/symbol/{TICKER}.json` -> recent posts on a ticker

Each post carries an optional `sentiment` field (Bullish/Bearish) set by the
poster — we use that as a label *and* run our own FinBERT on the body for a
numeric score.

Undocumented rate limit ~200/hr. Be polite: sleep between calls.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import requests

from ingest.types import Article

BASE = "https://api.stocktwits.com/api/2"
TIMEOUT = 15
HEADERS = {"User-Agent": "amaltash-sentiment/0.1"}


def _get(path: str, **params) -> dict:
    r = requests.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code == 429:
        time.sleep(10)
        r = requests.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def trending_tickers(limit: int = 30, us_only: bool = True) -> list[str]:
    """Tickers currently trending on StockTwits.

    StockTwits' trending list is global. With `us_only=True`, filter to symbols
    present in our US universe (no .NSE/.BSE/.L suffix etc.).
    """
    data = _get("/trending/symbols.json", limit=limit)
    syms = [s.get("symbol", "") for s in data.get("symbols", []) if s.get("symbol")]
    if not us_only:
        return syms
    # Filter to plain US tickers (no exchange suffix) and in our universe.
    from extract.resolver import get_universe
    uni = get_universe()
    return [s for s in syms if "." not in s and s in uni]


def symbol_stream(ticker: str, limit: int = 30) -> list[Article]:
    """Recent posts mentioning a ticker."""
    try:
        data = _get(f"/streams/symbol/{ticker}.json", limit=limit)
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise
    out: list[Article] = []
    for msg in data.get("messages", []):
        body = msg.get("body", "") or ""
        if not body:
            continue
        try:
            created = datetime.fromisoformat(
                msg.get("created_at", "").replace("Z", "+00:00")
            )
        except Exception:
            created = datetime.now(timezone.utc)
        user = msg.get("user", {}) or {}
        out.append(Article(
            source="stocktwits",
            source_outlet=user.get("username", "") or "stocktwits",
            headline=body[:140],
            summary=body[:500],
            url=f"https://stocktwits.com/{user.get('username', '')}/message/{msg.get('id', '')}",
            published_at=created,
            tickers=[ticker],
        ))
    return out


def stream_for_tickers(tickers: list[str], per_ticker: int = 20,
                       sleep_between: float = 0.5) -> list[Article]:
    out: list[Article] = []
    for t in tickers:
        try:
            arts = symbol_stream(t, limit=per_ticker)
            out.extend(arts)
        except Exception as e:
            print(f"[stocktwits] {t}: {type(e).__name__}: {e}")
        time.sleep(sleep_between)
    return out
