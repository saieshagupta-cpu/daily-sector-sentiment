"""Daily refresh job.

Runs the full discovery pipeline, applies the TA gate to discovered tickers,
and writes everything to SQLite. This is the single command the cron will run.

Usage:
    python -m jobs.daily_refresh                  # 24-hour window
    python -m jobs.daily_refresh --hours 48       # widen window
    python -m jobs.daily_refresh --no-ta          # skip TA gate (faster, for testing)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow `python -m jobs.daily_refresh` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from discovery.pipeline import run_pipeline
from store.db import connect, init_db, store_articles
from technicals.ta import fetch_technicals


def _persist_daily_scores(ranked: dict, ta_results: dict) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    init_db()
    with connect() as conn:
        # Clear today's rows so re-runs are idempotent
        conn.execute("DELETE FROM daily_scores WHERE snapshot_date = ?", (today,))
        for sector, buckets in ranked.items():
            for kind in ("held", "watch"):
                for ts in buckets[kind]:
                    ta = ta_results.get(ts.ticker)
                    s = ts.strength
                    conn.execute(
                        """INSERT INTO daily_scores
                           (snapshot_date, ticker, sector, sentiment_avg,
                            sentiment_weighted, mention_count, distinct_sources,
                            novelty, is_blue_chip,
                            strength_composite, ret_1m, ret_3m, ret_6m,
                            pct_above_dma200, pct_off_52w_high, final_score)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (today, ts.ticker, ts.sector,
                         ts.sentiment_avg, ts.sentiment_weighted,
                         ts.mention_count, ts.distinct_sources, ts.novelty,
                         1 if ts.is_blue_chip else 0,
                         s.composite if s else None,
                         s.ret_1m if s else None,
                         s.ret_3m if s else None,
                         s.ret_6m if s else None,
                         s.pct_above_dma200 if s else None,
                         s.pct_off_52w_high if s else None,
                         ts.final_score),
                    )
    print(f"[refresh] persisted daily_scores for {today}")


def _persist_ta(ta_results: dict) -> None:
    """Store TA results in a side table so the dashboard can show gate status."""
    today = datetime.now(timezone.utc).date().isoformat()
    init_db()
    with connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_ta (
            snapshot_date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            price REAL,
            dma_200 REAL,
            rsi_14 REAL,
            avg_dollar_vol_20 REAL,
            market_cap REAL,
            passes_gate INTEGER,
            gate_reasons TEXT,
            PRIMARY KEY (snapshot_date, ticker)
        );
        """)
        conn.execute("DELETE FROM daily_ta WHERE snapshot_date = ?", (today,))
        for t, r in ta_results.items():
            conn.execute(
                """INSERT INTO daily_ta
                   (snapshot_date, ticker, price, dma_200, rsi_14,
                    avg_dollar_vol_20, market_cap, passes_gate, gate_reasons)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, t, r.price, r.dma_200, r.rsi_14,
                 r.avg_dollar_vol_20, r.market_cap,
                 1 if r.passes_gate else 0,
                 "; ".join(r.gate_reasons)),
            )
    print(f"[refresh] persisted daily_ta for {today}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--hours", type=int, default=168,
                   help="lookback window in hours (default 168 = 7 days)")
    p.add_argument("--no-ta", action="store_true", help="skip TA gate (faster)")
    args = p.parse_args(argv)

    result = run_pipeline(hours_back=args.hours)
    ranked = result["ranked"]
    articles = result["articles"]
    sentiments = result["sentiments"]

    print(f"[refresh] storing {len(articles)} articles...")
    inserted = store_articles(zip(articles, sentiments))
    print(f"[refresh] {inserted} new articles inserted (rest were duplicates)")

    # Apply TA gate to every ticker that surfaced — both held and watch.
    ta_results: dict = {}
    if not args.no_ta:
        all_tickers: set[str] = set()
        for sec, buckets in ranked.items():
            for kind in ("held", "watch"):
                for ts in buckets[kind]:
                    all_tickers.add(ts.ticker)
        print(f"[refresh] computing TA for {len(all_tickers)} tickers...")
        from technicals.ta import fetch_many
        ta_results = fetch_many(sorted(all_tickers))
        print(f"[refresh] TA computed for {len(ta_results)} tickers")
        _persist_ta(ta_results)

    _persist_daily_scores(ranked, ta_results)

    # Print summary
    print("\n=== SECTOR SUMMARY ===")
    print("  HELD shows sentiment_weighted | WATCH shows final_score (strength + sentiment bonus)")
    for sec, buckets in ranked.items():
        held = buckets["held"]
        watch = buckets["watch"]
        print(f"\n{sec.upper()}")
        print(f"  HELD ({len(held)}):  " + ", ".join(
            f"{ts.ticker}({ts.sentiment_weighted:+.2f})" for ts in held[:10]))
        print(f"  WATCH ({len(watch)}): " + ", ".join(
            f"{ts.ticker}({ts.final_score:+.2f})" for ts in watch[:10]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
