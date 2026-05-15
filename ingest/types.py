"""Shared types for ingest layer — every source returns a list of Article."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Article:
    source: str            # "finnhub" | "marketaux" | "gdelt" | "stocktwits" | "rss" | "sec_edgar"
    source_outlet: str     # e.g. "Reuters", "CNBC" — the publisher, if known
    headline: str
    summary: str           # short body / description
    url: str
    published_at: datetime
    tickers: list[str] = field(default_factory=list)  # resolved tickers (empty for un-tagged sources)
    language: str = "en"

    def text_for_sentiment(self) -> str:
        return f"{self.headline}. {self.summary}".strip()
