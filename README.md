# Daily Sector Sentiment

A 6-sector US-equity dashboard combining FinBERT news sentiment with a
strength-based momentum screen.

- **Big Names** — 10 fixed established leaders per sector, scored by news sentiment
- **New Names** — lesser-known mid/small caps surfaced by technical strength
  (1m + 3m + 6m return, % above 200-DMA, RSI) with a sentiment bonus layered on

Sources: Finnhub, Marketaux, GDELT 2.0, 14 RSS feeds, 10 Reddit subreddits, StockTwits.

## Quick start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Add keys (Finnhub + Marketaux — both free tiers)
cp api_keys.env.example api_keys.env
# edit api_keys.env

# 3. Run the daily refresh (~5–10 min)
python -m jobs.daily_refresh

# 4. Open the dashboard
streamlit run app/streamlit_app.py
```

## Daily auto-refresh (macOS)

```bash
bash jobs/install_launchd.sh install   # registers a launchd job at 06:30 daily
bash jobs/install_launchd.sh status
bash jobs/install_launchd.sh tail      # read latest log
```

## Project layout

```
config/        # universe.yaml (Big Names), candidates.yaml (New Names pool), filters.yaml
ingest/        # finnhub, marketaux, gdelt, rss, reddit_json, stocktwits
extract/       # ticker resolver — cashtags + company-name → US ticker
sentiment/     # FinBERT scorer
technicals/    # yfinance OHLCV + TA gate
discovery/     # pipeline orchestrator + strength scorer + sector mapping
store/         # SQLite schema + writes
app/           # Streamlit dashboard
jobs/          # daily_refresh + launchd plist
```

## Methodology

**Trend:** price vs 200-day moving average.
**Momentum:** 1m / 3m / 6m returns, weighted 20% / 30% / 20%.
**Confirmation:** RSI(14) in [40, 70], current vs 20-day volume.
**Quality floor:** market cap ≥ \$500M, 20-day average dollar volume ≥ \$10M.

The strength composite is then nudged by sentiment (±0.20 max) so a hot news
story can promote a moderately-strong name, but technical action dominates.

## Deploy to Streamlit Cloud

1. Push this repo to GitHub.
2. At [share.streamlit.io](https://share.streamlit.io), New app → point at this repo.
3. Main file path: `app/streamlit_app.py`.
4. Advanced settings → Secrets → paste:

   ```toml
   FINNHUB_API_KEY = "..."
   MARKETAUX_API_KEY = "..."
   ```

5. The deployed app reads the same SQLite DB that your local refresh produces.
   To keep the deployed data fresh, either:
   - commit `store/amaltash.db` after each refresh (small repo, simple); or
   - move data to a hosted DB (Supabase / Neon) and update both connections.

by Saiesha Gupta
