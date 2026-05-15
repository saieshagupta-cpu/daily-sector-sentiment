"""SQLite storage for articles, scored sentiment, and daily aggregates.

Schema is deliberately denormalised for query speed in the Streamlit dashboard.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ingest.types import Article
from sentiment.scorer import SentimentResult

DB_PATH = Path(__file__).resolve().parent / "amaltash.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    source_outlet TEXT,
    headline TEXT NOT NULL,
    summary TEXT,
    published_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    sentiment_label TEXT,
    sentiment_score REAL,
    sentiment_confidence REAL
);

CREATE TABLE IF NOT EXISTS article_tickers (
    article_id INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    PRIMARY KEY (article_id, ticker),
    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_tickers_ticker ON article_tickers(ticker);

CREATE TABLE IF NOT EXISTS daily_scores (
    snapshot_date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    sector TEXT NOT NULL,
    sentiment_avg REAL,
    sentiment_weighted REAL,
    mention_count INTEGER,
    distinct_sources INTEGER,
    novelty REAL,
    is_blue_chip INTEGER,
    -- strength fields (NULL for HELD; populated for WATCH)
    strength_composite REAL,
    ret_1m REAL,
    ret_3m REAL,
    ret_6m REAL,
    pct_above_dma200 REAL,
    pct_off_52w_high REAL,
    final_score REAL,
    -- PK includes sector so a ticker can appear in multiple sector lists
    PRIMARY KEY (snapshot_date, ticker, sector)
);

CREATE INDEX IF NOT EXISTS idx_daily_sector ON daily_scores(snapshot_date, sector);
"""


@contextmanager
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)


def upsert_article(conn: sqlite3.Connection, art: Article, sent: SentimentResult | None) -> int | None:
    """Insert article + tickers. Returns row id, or None if duplicate URL skipped."""
    now = datetime.utcnow().isoformat()
    try:
        cur = conn.execute(
            """INSERT INTO articles
               (url, source, source_outlet, headline, summary, published_at, fetched_at,
                sentiment_label, sentiment_score, sentiment_confidence)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (art.url, art.source, art.source_outlet, art.headline, art.summary,
             art.published_at.isoformat(), now,
             sent.label if sent else None,
             sent.score if sent else None,
             sent.confidence if sent else None),
        )
        article_id = cur.lastrowid
    except sqlite3.IntegrityError:
        # URL already stored — skip (we keep first-seen version)
        return None
    for t in set(art.tickers):
        conn.execute(
            "INSERT OR IGNORE INTO article_tickers (article_id, ticker) VALUES (?, ?)",
            (article_id, t),
        )
    return article_id


def store_articles(items: Iterable[tuple[Article, SentimentResult | None]]) -> int:
    """Bulk-insert. Returns count of newly inserted articles."""
    init_db()
    inserted = 0
    with connect() as conn:
        for art, sent in items:
            if upsert_article(conn, art, sent) is not None:
                inserted += 1
    return inserted


def recent_articles_for_ticker(ticker: str, limit: int = 20) -> list[dict]:
    init_db()
    with connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT a.* FROM articles a
               JOIN article_tickers t ON t.article_id = a.id
               WHERE t.ticker = ?
               ORDER BY a.published_at DESC
               LIMIT ?""",
            (ticker, limit),
        ).fetchall()
        return [dict(r) for r in rows]
