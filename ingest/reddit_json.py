"""Reddit public-JSON ingest — no auth, no app, no API keys.

Reddit exposes a read-only JSON variant of every subreddit URL. Append `.json`
to any sort path and you get the posts as JSON. No `client_id`/`client_secret`
required for *public, read-only* data.

The ToS still applies — same commercial-use clause as the regular Data API.
This is a registration workaround only, not a legal one.

Rate limit (unauthenticated): ~60 req/min. We stay well under by sleeping
~1.5s between subreddit calls.

We pull a mix of sorts (hot + top of week) so the daily refresh always has
material to score, regardless of whether anything was posted "today".
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Iterable

import requests

from ingest.types import Article

BASE = "https://www.reddit.com"
TIMEOUT = 15
HEADERS = {"User-Agent": "amaltash-sentiment/0.1 (sentiment research)"}

# Subreddit bundle. Tag a sector hint where obvious (informational only —
# ticker extractor downstream drives the actual sector assignment).
SUBREDDITS: list[tuple[str, str | None]] = [
    ("stocks",            None),
    ("investing",         None),
    ("SecurityAnalysis",  None),
    ("wallstreetbets",    None),
    ("options",           None),
    ("biotech",           "healthcare"),
    ("energystocks",      "energy"),
    ("uranium",           "energy"),
    ("REbubble",          "real_estate"),
    ("fintech",           "finance"),
]


def _fetch_listing(subreddit: str, sort: str = "hot",
                   timeframe: str | None = None, limit: int = 50) -> list[dict]:
    url = f"{BASE}/r/{subreddit}/{sort}.json"
    params = {"limit": limit}
    if timeframe and sort == "top":
        params["t"] = timeframe
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code == 429:
        time.sleep(10)
        r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"[reddit] {subreddit}/{sort}: HTTP {r.status_code}")
        return []
    try:
        data = r.json()
    except Exception:
        return []
    children = data.get("data", {}).get("children", [])
    return [c.get("data", {}) for c in children if c.get("data")]


def _post_to_article(p: dict, subreddit: str) -> Article | None:
    created = p.get("created_utc")
    if not created:
        return None
    title = p.get("title", "") or ""
    selftext = p.get("selftext", "") or ""
    if not title:
        return None
    permalink = p.get("permalink", "") or ""
    return Article(
        source="reddit",
        source_outlet=f"r/{subreddit}",
        headline=title,
        summary=selftext[:1500],         # cap to avoid mega-text false positives
        url=f"{BASE}{permalink}",
        published_at=datetime.fromtimestamp(float(created), tz=timezone.utc),
        tickers=[],                       # ticker extractor fills these
    )


def fetch_subreddit(subreddit: str, sorts: tuple[str, ...] = ("hot", "top"),
                    top_timeframe: str = "week", limit: int = 50) -> list[Article]:
    seen: set[str] = set()
    out: list[Article] = []
    for sort in sorts:
        tf = top_timeframe if sort == "top" else None
        posts = _fetch_listing(subreddit, sort=sort, timeframe=tf, limit=limit)
        for p in posts:
            pid = p.get("id") or p.get("name") or p.get("permalink")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            art = _post_to_article(p, subreddit)
            if art is not None:
                out.append(art)
    return out


def fetch_all(subreddits: Iterable[tuple[str, str | None]] | None = None,
              sleep_between: float = 1.5) -> list[Article]:
    subs = list(subreddits) if subreddits is not None else SUBREDDITS
    all_articles: list[Article] = []
    for i, (sub, _hint) in enumerate(subs):
        try:
            arts = fetch_subreddit(sub)
            all_articles.extend(arts)
            print(f"[reddit] r/{sub}: {len(arts)} posts")
        except Exception as e:
            print(f"[reddit] r/{sub}: {type(e).__name__}: {e}")
        if i < len(subs) - 1:
            time.sleep(sleep_between)
    return all_articles
