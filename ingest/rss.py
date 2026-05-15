"""RSS feed bundle — free, no-auth headline pull.

Each feed contributes a stream of headlines/snippets. We don't try to be
exhaustive; instead we pick a curated bundle that maximises sector coverage.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import feedparser

from ingest.types import Article

# Curated bundle. Each entry: (outlet_label, url, sector_hint_or_None)
# sector_hint_or_None is informational; tickers ultimately drive sector tagging.
FEEDS: list[tuple[str, str, str | None]] = [
    # General market
    ("Yahoo Finance",   "https://finance.yahoo.com/news/rssindex", None),
    ("MarketWatch",     "http://feeds.marketwatch.com/marketwatch/topstories/", None),
    ("CNBC Top News",   "https://www.cnbc.com/id/100003114/device/rss/rss.html", None),
    ("CNBC Markets",    "https://www.cnbc.com/id/15839135/device/rss/rss.html", None),
    ("SeekingAlpha",    "https://seekingalpha.com/feed.xml", None),

    # Sector-leaning
    ("OilPrice",        "https://oilprice.com/rss/main", "energy"),
    ("FierceBiotech",   "https://www.fiercebiotech.com/rss/xml", "healthcare"),
    ("FiercePharma",    "https://www.fiercepharma.com/rss/xml", "healthcare"),
    ("Mining.com",      "https://www.mining.com/feed/", "minerals"),
    ("The Verge",       "https://www.theverge.com/rss/index.xml", "tech"),
    ("TechCrunch",      "https://techcrunch.com/feed/", "tech"),
    ("GlobeSt",         "https://www.globest.com/rss/", "real_estate"),
    ("Banking Dive",    "https://www.bankingdive.com/feeds/news/", "finance"),

    # Primary source
    ("SEC EDGAR",       "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom", None),
]


def _parse_dt(entry) -> datetime:
    for key in ("published_parsed", "updated_parsed"):
        v = getattr(entry, key, None) or entry.get(key)
        if v:
            try:
                return datetime(*v[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def pull_feed(outlet: str, url: str) -> list[Article]:
    parsed = feedparser.parse(url)
    out: list[Article] = []
    for e in parsed.entries[:50]:
        url_e = e.get("link", "") or ""
        if not url_e:
            continue
        summary = e.get("summary", "") or e.get("description", "") or ""
        # Strip basic HTML
        if "<" in summary:
            import re
            summary = re.sub(r"<[^>]+>", "", summary)
        out.append(Article(
            source="sec_edgar" if "sec.gov" in url else "rss",
            source_outlet=outlet,
            headline=e.get("title", "") or "",
            summary=summary[:500],
            url=url_e,
            published_at=_parse_dt(e),
            tickers=[],
        ))
    return out


def pull_all(feeds: Iterable[tuple[str, str, str | None]] | None = None) -> list[Article]:
    feeds = list(feeds) if feeds is not None else FEEDS
    all_articles: list[Article] = []
    for outlet, url, _hint in feeds:
        try:
            arts = pull_feed(outlet, url)
            all_articles.extend(arts)
            print(f"[rss] {outlet}: {len(arts)} articles")
        except Exception as e:
            print(f"[rss] {outlet}: {type(e).__name__}: {e}")
    return all_articles
