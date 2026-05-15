"""End-to-end smoke: Finnhub → FinBERT → SQLite → read-back."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingest.finnhub import company_news
from sentiment.scorer import score_texts
from store.db import store_articles, recent_articles_for_ticker, init_db


def run(tickers=("NVDA", "OXY", "MP")) -> None:
    init_db()

    pulled: list[tuple[str, list]] = []
    total = 0
    for t in tickers:
        news = company_news(t, days_back=1)
        pulled.append((t, news))
        total += len(news)
        print(f"[ingest] {t}: {len(news)} articles")

    if total == 0:
        print("no articles — nothing to score")
        return

    flat = [a for _, news in pulled for a in news]
    texts = [a.text_for_sentiment() for a in flat]
    print(f"[score]  scoring {len(texts)} headlines with FinBERT...")
    results = score_texts(texts, batch_size=16)

    inserted = store_articles(zip(flat, results))
    print(f"[store]  inserted {inserted} new articles (rest were duplicates)")

    for t in tickers:
        rows = recent_articles_for_ticker(t, limit=3)
        print(f"\n--- {t} latest 3 in DB ---")
        for r in rows:
            print(f"  {r['sentiment_label']:>9s}  {r['sentiment_score']:+.3f}  "
                  f"[{r['source_outlet']}]  {r['headline'][:80]}")


if __name__ == "__main__":
    run()
