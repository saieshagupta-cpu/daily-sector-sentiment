"""Discovery pipeline — the daily orchestrator.

Inputs:
- Blue chip universe (config/universe.yaml)
- News sources: Finnhub (per blue chip), Marketaux (discovery + per-ticker),
  GDELT (sector firehose), RSS bundle, StockTwits (per-ticker stream)

Output (written to SQLite):
- `articles`: every article ingested, with FinBERT sentiment
- `daily_scores`: per (date, ticker) aggregated score + breakdown

The dashboard reads `daily_scores` for the table and joins back to `articles`
via `article_tickers` to show the WHY (top headlines per ticker).
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from config.settings import (
    ALL_CANDIDATES, BLUE_CHIP_TICKERS, CANDIDATES, CANDIDATE_SECTORS, FILTERS,
    SECTORS, TICKER_TO_SECTOR, UNIVERSE,
)
from discovery.sector_map import map_industry
from discovery.strength import StrengthResult, compute_many
from extract.resolver import extract_tickers, get_universe
from ingest import finnhub as fh
from ingest import gdelt as gd
from ingest import marketaux as mx
from ingest import reddit_json as rj
from ingest import rss as rss
from ingest import stocktwits as st
from ingest.types import Article
from sentiment.scorer import SentimentResult, score_texts


@dataclass
class TickerScore:
    ticker: str
    sector: str | None
    sentiment_avg: float = 0.0
    sentiment_weighted: float = 0.0
    mention_count: int = 0
    distinct_sources: int = 0
    novelty: float = 1.0
    is_blue_chip: bool = False
    # Strength fields (filled for WATCH candidates)
    strength: StrengthResult | None = None
    final_score: float = 0.0     # composite of strength + sentiment bonus
    articles: list[Article] = field(default_factory=list)
    sentiments: list[SentimentResult] = field(default_factory=list)


SOURCE_WEIGHTS: dict[str, float] = FILTERS["source_weights"]


def _resolve_sector(ticker: str) -> str | None:
    """Use the blue-chip mapping if it's a blue chip, else look up via Finnhub."""
    if ticker in TICKER_TO_SECTOR:
        return TICKER_TO_SECTOR[ticker]
    try:
        profile = fh.company_profile(ticker)
        return map_industry(profile.get("finnhubIndustry"))
    except Exception:
        return None


def _attach_tickers(articles: Iterable[Article]) -> list[Article]:
    """For articles not already tagged, run the resolver on headline+summary."""
    out: list[Article] = []
    for a in articles:
        if not a.tickers:
            text = (a.headline or "") + " " + (a.summary or "")
            a.tickers = sorted(extract_tickers(text))
        out.append(a)
    return out


def collect_articles(hours_back: int = 168) -> list[Article]:
    """Pull from every source, return a flat list of Articles (some untagged).

    Default lookback is 7 days (168h) so NEW NAMES cards have headlines to
    show even on days when there's no fresh news on a given ticker.
    """
    pulled: list[Article] = []
    days_back = max(1, hours_back // 24)

    # 1a. Finnhub: blue-chip per-ticker news (high precision, ticker-tagged)
    bc_news = fh.company_news_batch(list(BLUE_CHIP_TICKERS), days_back=days_back)
    print(f"[pipeline] Finnhub blue-chip news: {len(bc_news)} articles")
    pulled.extend(bc_news)

    # 1b. Finnhub: per-candidate news for the WATCH pool. Without this, NEW
    #     NAMES would only get headlines if they happened to be mentioned in
    #     GDELT/RSS/Reddit. Per-candidate pulls guarantee each card has WHY.
    non_bc_candidates = sorted(ALL_CANDIDATES - BLUE_CHIP_TICKERS)
    cand_news = fh.company_news_batch(non_bc_candidates, days_back=days_back)
    print(f"[pipeline] Finnhub candidate news: {len(cand_news)} articles "
          f"across {len(non_bc_candidates)} candidates")
    pulled.extend(cand_news)

    # 2. Marketaux: discovery firehose (broad US news)
    try:
        mx_disc = mx.discover_us_news(limit=3)
        pulled.extend(mx_disc)
        print(f"[pipeline] Marketaux discovery: {len(mx_disc)} articles")
    except Exception as e:
        print(f"[pipeline] Marketaux discovery failed: {e}")

    # 3. GDELT: per-sector firehose
    try:
        gd_by_sector = gd.fetch_by_sector(SECTORS, hours_back=hours_back, max_per_sector=50)
        for sec, arts in gd_by_sector.items():
            pulled.extend(arts)
    except Exception as e:
        print(f"[pipeline] GDELT failed: {e}")

    # 4. RSS bundle
    try:
        rss_arts = rss.pull_all()
        pulled.extend(rss_arts)
    except Exception as e:
        print(f"[pipeline] RSS failed: {e}")

    # 5. Reddit (public JSON, no auth)
    try:
        rj_arts = rj.fetch_all()
        pulled.extend(rj_arts)
        print(f"[pipeline] Reddit JSON: {len(rj_arts)} posts")
    except Exception as e:
        print(f"[pipeline] Reddit failed: {e}")

    return pulled


def aggregate_scores(articles: list[Article],
                     sentiments: list[SentimentResult]) -> dict[str, TickerScore]:
    """Group articles by ticker, compute per-ticker aggregates."""
    bag: dict[str, TickerScore] = {}
    for art, sent in zip(articles, sentiments):
        if not art.tickers:
            continue
        for t in art.tickers:
            ts = bag.setdefault(t, TickerScore(
                ticker=t,
                sector=None,
                is_blue_chip=(t in BLUE_CHIP_TICKERS),
            ))
            ts.articles.append(art)
            ts.sentiments.append(sent)

    for ts in bag.values():
        if not ts.sentiments:
            continue
        scores = [s.score for s in ts.sentiments]
        weights = [SOURCE_WEIGHTS.get(a.source, 0.5) for a in ts.articles]
        ts.sentiment_avg = float(sum(scores) / len(scores))
        weighted_sum = sum(s * w for s, w in zip(scores, weights))
        weight_total = sum(weights) or 1.0
        ts.sentiment_weighted = float(weighted_sum / weight_total)
        ts.mention_count = len(ts.articles)
        ts.distinct_sources = len({a.source_outlet for a in ts.articles if a.source_outlet})
    return bag


def rank_per_sector(bag: dict[str, TickerScore],
                    top_n_watch: int = 10) -> dict[str, dict]:
    """Build the daily output: held (blue chips) + watch (top-N strongest candidates).

    WATCH selection is STRENGTH-driven, not news-driven:
    - candidate pool = config/candidates.yaml (~30 names/sector, non-blue-chip)
    - rank by strength composite (momentum + trend + RSI)
    - sentiment is added as a small bonus when present

    Returns:
        {
          'energy': {
            'held':  [TickerScore, ...],   # blue chips in this sector
            'watch': [TickerScore, ...],   # top N strongest candidates
          },
          ...
        }
    """
    # 1. Resolve sector for every ticker in bag (used by HELD news join)
    for t, ts in bag.items():
        ts.sector = _resolve_sector(t)

    out: dict[str, dict] = {sec: {"held": [], "watch": []} for sec in SECTORS}

    # 2. HELD = our blue chips, sorted within sector by sentiment_weighted desc
    by_sector_held: dict[str, list[TickerScore]] = defaultdict(list)
    for t in BLUE_CHIP_TICKERS:
        sec = TICKER_TO_SECTOR.get(t)
        if not sec:
            continue
        ts = bag.get(t) or TickerScore(ticker=t, sector=sec, is_blue_chip=True)
        ts.sector = sec
        ts.is_blue_chip = True
        by_sector_held[sec].append(ts)
    for sec, lst in by_sector_held.items():
        lst.sort(key=lambda x: x.sentiment_weighted, reverse=True)
        out[sec]["held"] = lst

    # 3. WATCH = strength-ranked candidates per sector. Tickers can appear in
    #    multiple sectors (e.g. VRT in both Tech and Energy) — we compute
    #    strength ONCE per ticker and create a fresh TickerScore per sector.
    unique_candidates = sorted(ALL_CANDIDATES - BLUE_CHIP_TICKERS)
    print(f"[pipeline] computing strength for {len(unique_candidates)} unique candidates "
          f"(some appear in >1 sector)")
    strengths = compute_many(unique_candidates)

    by_sector_watch: dict[str, list[TickerScore]] = defaultdict(list)
    for sec, sec_tickers in CANDIDATES.items():
        if sec not in SECTORS:
            continue
        for t in sec_tickers:
            if t in BLUE_CHIP_TICKERS:
                continue
            s = strengths.get(t)
            if s is None:
                continue
            existing = bag.get(t)
            sentiment_bonus = 0.0
            if existing and existing.mention_count > 0:
                sentiment_bonus = max(-0.20, min(0.20, existing.sentiment_weighted * 0.20))

            # Build a per-(sector, ticker) TickerScore (one ticker → potentially
            # multiple TickerScore objects across sectors)
            ts = TickerScore(
                ticker=t,
                sector=sec,
                is_blue_chip=False,
                sentiment_avg=existing.sentiment_avg if existing else 0.0,
                sentiment_weighted=existing.sentiment_weighted if existing else 0.0,
                mention_count=existing.mention_count if existing else 0,
                distinct_sources=existing.distinct_sources if existing else 0,
                articles=existing.articles if existing else [],
                sentiments=existing.sentiments if existing else [],
            )
            ts.strength = s
            ts.final_score = float(s.composite + sentiment_bonus)
            by_sector_watch[sec].append(ts)

    for sec, cands in by_sector_watch.items():
        cands.sort(key=lambda x: x.final_score, reverse=True)
        out[sec]["watch"] = cands[:top_n_watch]

    return out


def run_pipeline(hours_back: int = 24) -> dict[str, dict]:
    """Top-level entry: collect → tag → score → aggregate → rank.

    Returns the same shape as rank_per_sector. Persistence is handled by the
    daily-refresh job (jobs/daily_refresh.py) which calls this and writes
    results into SQLite for the dashboard.
    """
    print("[pipeline] collecting articles...")
    arts = collect_articles(hours_back=hours_back)
    print(f"[pipeline] {len(arts)} raw articles collected")

    arts = _attach_tickers(arts)
    tagged = [a for a in arts if a.tickers]
    print(f"[pipeline] {len(tagged)} articles tagged with at least one ticker")

    print("[pipeline] scoring sentiment...")
    texts = [a.text_for_sentiment() for a in tagged]
    sentiments = score_texts(texts, batch_size=16)

    bag = aggregate_scores(tagged, sentiments)
    print(f"[pipeline] {len(bag)} unique tickers in scoring bag")

    ranked = rank_per_sector(bag)
    return {
        "ranked": ranked,
        "articles": tagged,
        "sentiments": sentiments,
        "as_of": datetime.now(timezone.utc),
    }
