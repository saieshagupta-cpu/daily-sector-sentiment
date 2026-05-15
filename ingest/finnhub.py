"""Finnhub company news client. Free tier: 60 req/min, US tickers covered well."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests

from config.settings import require_key
from ingest.types import Article

BASE = "https://finnhub.io/api/v1"
TIMEOUT = 15


def _get(path: str, **params) -> list[dict] | dict:
    params["token"] = require_key("finnhub")
    r = requests.get(f"{BASE}{path}", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def company_news(ticker: str, days_back: int = 1) -> list[Article]:
    """Pull recent company-tagged news for a single ticker."""
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=days_back)).isoformat()
    end = today.isoformat()
    raw = _get("/company-news", symbol=ticker, **{"from": start, "to": end})
    if not isinstance(raw, list):
        return []
    out: list[Article] = []
    for item in raw:
        ts = item.get("datetime")
        if not ts:
            continue
        out.append(Article(
            source="finnhub",
            source_outlet=item.get("source", "") or "",
            headline=item.get("headline", "") or "",
            summary=item.get("summary", "") or "",
            url=item.get("url", "") or "",
            published_at=datetime.fromtimestamp(ts, tz=timezone.utc),
            tickers=[ticker],
        ))
    return out


def company_news_batch(tickers: Iterable[str], days_back: int = 1,
                       sleep_between: float = 1.1) -> list[Article]:
    """Sequential pull with throttling — Finnhub free tier is 60/min.

    We sleep ~1.1s between calls to stay safely under the limit. If we still
    hit a 429, sleep 30s and retry once.
    """
    all_articles: list[Article] = []
    tickers = list(tickers)
    for i, t in enumerate(tickers):
        try:
            all_articles.extend(company_news(t, days_back=days_back))
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                print(f"[finnhub] {t}: 429, sleeping 30s and retrying once")
                time.sleep(30)
                try:
                    all_articles.extend(company_news(t, days_back=days_back))
                except Exception as e2:
                    print(f"[finnhub] {t}: retry failed: {type(e2).__name__}")
            else:
                print(f"[finnhub] {t}: HTTP {e.response.status_code}")
        except Exception as e:
            print(f"[finnhub] {t}: {type(e).__name__}: {e}")
        if i < len(tickers) - 1:
            time.sleep(sleep_between)
    return all_articles


def company_profile(ticker: str) -> dict:
    """Sector, market cap, ADV — used by the discovery quality gate."""
    raw = _get("/stock/profile2", symbol=ticker)
    return raw if isinstance(raw, dict) else {}
