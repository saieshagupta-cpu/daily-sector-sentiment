"""Daily sector-sentiment dashboard.

Shows, per sector:
- BIG NAMES: 10 fixed established leaders with daily news sentiment
- NEW NAMES: top 10 lesser-known mid/small caps with strongest price momentum
  + sentiment overlay

Each card expands into the headlines that drove the score (with source links).

Run:
    streamlit run amaltash_sentiment/app/streamlit_app.py
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st

from config.settings import UNIVERSE
from extract.resolver import get_universe as get_us_universe
from store.db import DB_PATH


# ─────────────────────────────────────────────────────────────────────────────
# Company-name lookup
# ─────────────────────────────────────────────────────────────────────────────

_NAME_SUFFIXES_TO_STRIP = (
    " inc.", " inc", " corporation", " corp.", " corp", " co.", " co",
    " company", " holdings", " holding", " plc", " ltd.", " ltd",
    " limited", " llc", " n.v.", " s.a.", " ag",
)


def _prettify(raw: str) -> str:
    """Convert ALL-CAPS Finnhub name to Title Case, strip noisy suffixes."""
    if not raw:
        return ""
    s = raw.strip()
    # Strip trailing class-share designators e.g. " - CLASS A", " - A"
    import re
    s = re.sub(r"\s*[-/]?\s*(class|cl)\s+[a-z](\s+shares?)?$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*[a-z]$", "", s, flags=re.I)
    s = s.title()
    # Strip suffixes after title-casing for cleaner names
    lower = s.lower()
    for suf in _NAME_SUFFIXES_TO_STRIP:
        if lower.endswith(suf):
            s = s[: -len(suf)].rstrip(" ,.&-")
            break
    return s.strip()


@st.cache_data(ttl=3600)
def build_ticker_names() -> dict[str, str]:
    """Map every ticker we care about to a display-ready company name.

    Priority: universe.yaml (curated, nice case) → Finnhub universe (auto title-case).
    """
    names: dict[str, str] = {}
    # 1) Blue chips from universe.yaml — already nicely cased
    for rows in UNIVERSE.values():
        for row in rows:
            names[row["ticker"]] = row.get("name", row["ticker"])
    # 2) Everything else from the Finnhub US universe (~5,461 names)
    us_uni = get_us_universe()
    for ticker, info in us_uni.items():
        if ticker in names:
            continue
        raw = info.get("name") or ""
        if raw:
            names[ticker] = _prettify(raw)
    return names


TICKER_NAMES: dict[str, str] = {}  # filled inside main() after Streamlit boots


SECTOR_LABELS = {
    "energy":      "Energy",
    "healthcare":  "Healthcare",
    "minerals":    "Minerals",
    "tech":        "Tech",
    "real_estate": "Real Estate",
    "finance":     "Finance",
}
SECTOR_ORDER = ["energy", "healthcare", "minerals", "tech", "real_estate", "finance"]


# ─────────────────────────────────────────────────────────────────────────────
# Data access
# ─────────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


@st.cache_data(ttl=300)
def latest_snapshot_date() -> str | None:
    if not Path(DB_PATH).exists():
        return None
    with _conn() as c:
        row = c.execute("SELECT MAX(snapshot_date) AS d FROM daily_scores").fetchone()
        return row["d"] if row and row["d"] else None


@st.cache_data(ttl=300)
def load_scores(snapshot_date: str) -> pd.DataFrame:
    with _conn() as c:
        df = pd.read_sql_query(
            """SELECT s.*, t.price, t.dma_200, t.rsi_14, t.market_cap,
                      t.passes_gate, t.gate_reasons, t.avg_dollar_vol_20
               FROM daily_scores s
               LEFT JOIN daily_ta t
                 ON s.snapshot_date = t.snapshot_date AND s.ticker = t.ticker
               WHERE s.snapshot_date = ?""",
            c, params=(snapshot_date,),
        )
    return df


@st.cache_data(ttl=300)
def load_top_headlines(ticker: str, limit: int = 5) -> pd.DataFrame:
    with _conn() as c:
        df = pd.read_sql_query(
            """SELECT a.*
               FROM articles a
               JOIN article_tickers t ON t.article_id = a.id
               WHERE t.ticker = ?
               ORDER BY ABS(COALESCE(a.sentiment_score, 0)) DESC, a.published_at DESC
               LIMIT ?""",
            c, params=(ticker, limit),
        )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers — clean, no emoji clutter
# ─────────────────────────────────────────────────────────────────────────────

def fmt_sentiment(score: float | None) -> str:
    """Colored sentiment number, no emoji."""
    if score is None or pd.isna(score):
        return ":gray[—]"
    s = f"{score:+.2f}"
    if score >= 0.30:
        return f":green[**{s}**]"
    if score <= -0.30:
        return f":red[**{s}**]"
    return f":gray[{s}]"


def fmt_strength(comp: float | None) -> str:
    if comp is None or pd.isna(comp):
        return ":gray[—]"
    s = f"{comp:+.2f}"
    if comp >= 0.40:
        return f":green[**{s}**]"
    if comp >= 0.10:
        return f":green[{s}]"
    if comp <= -0.40:
        return f":red[**{s}**]"
    if comp <= -0.10:
        return f":red[{s}]"
    return f":gray[{s}]"


def fmt_pct(p: float | None) -> str:
    if p is None or pd.isna(p):
        return ":gray[—]"
    s = f"{p:+.1%}"
    if p >= 0.05:
        return f":green[{s}]"
    if p <= -0.05:
        return f":red[{s}]"
    return f":gray[{s}]"


def fmt_ta(passes: int | None) -> str:
    if passes is None or pd.isna(passes):
        return ""
    return "✅" if int(passes) == 1 else "❌"


# ─────────────────────────────────────────────────────────────────────────────
# Card rendering
# ─────────────────────────────────────────────────────────────────────────────

def _ticker_label(ticker: str) -> str:
    """Header label: **TICKER**  _Company Name_."""
    name = TICKER_NAMES.get(ticker, "")
    if not name or name.upper() == ticker:
        return f"**{ticker}**"
    return f"**{ticker}**  _{name}_"


def render_big_card(row: pd.Series) -> None:
    """BIG NAMES card — leads with sentiment, supplements with TA."""
    ticker = row["ticker"]
    sent = row["sentiment_weighted"]
    mentions = int(row["mention_count"]) if pd.notna(row["mention_count"]) else 0
    price = row.get("price")
    rsi = row.get("rsi_14")
    passes = row.get("passes_gate")

    parts = [_ticker_label(ticker), fmt_sentiment(sent)]
    if pd.notna(price):
        parts.append(f":gray[${price:,.2f}]")
    if pd.notna(rsi):
        parts.append(f":gray[RSI {rsi:.0f}]")
    ta = fmt_ta(passes)
    if ta:
        parts.append(ta)
    parts.append(f":gray[{mentions} headlines]")
    header = " &nbsp;·&nbsp; ".join(parts)

    with st.expander(header):
        _render_headlines(ticker, mentions)


def render_new_card(row: pd.Series) -> None:
    """NEW NAMES card — leads with strength + 3m return."""
    ticker = row["ticker"]
    sent = row["sentiment_weighted"]
    mentions = int(row["mention_count"]) if pd.notna(row["mention_count"]) else 0
    price = row.get("price")
    rsi = row.get("rsi_14")
    comp = row.get("strength_composite")
    r3 = row.get("ret_3m")

    parts = [_ticker_label(ticker), fmt_strength(comp)]
    if pd.notna(price):
        parts.append(f":gray[${price:,.2f}]")
    if pd.notna(r3):
        parts.append(f"3m {fmt_pct(r3)}")
    if pd.notna(rsi):
        parts.append(f":gray[RSI {rsi:.0f}]")
    if mentions > 0:
        parts.append(f"sentiment {fmt_sentiment(sent)}")
    parts.append(f":gray[{mentions} headlines]")
    header = " &nbsp;·&nbsp; ".join(parts)

    with st.expander(header):
        # Strength breakdown row
        r1 = row.get("ret_1m")
        r6 = row.get("ret_6m")
        pct_dma = row.get("pct_above_dma200")
        pct_high = row.get("pct_off_52w_high")
        m_cols = st.columns(5)
        if pd.notna(r1):
            m_cols[0].metric("1-month", f"{r1:+.1%}")
        if pd.notna(r3):
            m_cols[1].metric("3-month", f"{r3:+.1%}")
        if pd.notna(r6):
            m_cols[2].metric("6-month", f"{r6:+.1%}")
        if pd.notna(pct_dma):
            m_cols[3].metric("vs 200-DMA", f"{pct_dma:+.1%}")
        if pd.notna(pct_high):
            m_cols[4].metric("off 52w high", f"{pct_high:+.1%}")

        _render_headlines(ticker, mentions)


def _render_headlines(ticker: str, mentions: int) -> None:
    if mentions == 0:
        st.caption("_No news in the lookback window — pure technical pick._")
        return
    st.caption("Headlines driving today's sentiment:")
    heads = load_top_headlines(ticker, limit=5)
    if heads.empty:
        st.caption("_(no stored headlines)_")
        return
    for _, h in heads.iterrows():
        sc = h.get("sentiment_score")
        outlet = h.get("source_outlet") or h.get("source") or "source"
        url = h.get("url") or ""
        headline = h.get("headline") or ""
        published = h.get("published_at") or ""
        try:
            pub = datetime.fromisoformat(published).strftime("%b %d")
        except Exception:
            pub = published[:10] if published else ""
        sc_chip = fmt_sentiment(sc)
        st.markdown(
            f"- {sc_chip} &nbsp; [{headline}]({url}) "
            f":gray[— {outlet} · {pub}]",
            unsafe_allow_html=False,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main layout
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Daily Sector Sentiment",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Tighter spacing
    st.markdown(
        """<style>
        .block-container { padding-top: 1.5rem; max-width: 1500px; }
        h1 { margin-bottom: 0.25rem; }
        h2 { margin-top: 1.5rem; padding-top: 0.5rem; border-top: 1px solid rgba(255,255,255,0.08); }
        h3 { font-size: 1.05rem; opacity: 0.85; margin-bottom: 0.5rem; }
        details summary { padding: 0.35rem 0.6rem !important; }
        </style>""",
        unsafe_allow_html=True,
    )

    st.title("Daily Sector Sentiment")
    st.markdown(":gray[_by Saiesha Gupta_]")

    # Populate the global ticker → company name lookup once per session
    global TICKER_NAMES
    if not TICKER_NAMES:
        TICKER_NAMES = build_ticker_names()

    snap = latest_snapshot_date()
    if not snap:
        st.warning(
            "No data yet. Run the refresh job first:\n\n"
            "```bash\npython -m jobs.daily_refresh\n```"
        )
        st.stop()

    df = load_scores(snap)

    # Minimal sub-header
    st.caption(
        f"Snapshot **{snap}** · {len(df)} ticker rows · "
        f"refresh: `python -m amaltash_sentiment.jobs.daily_refresh`"
    )

    # Compact legend
    st.markdown(
        ":gray[**Big Names**: 10 fixed established leaders per sector, ranked by "
        "news sentiment. **New Names**: lesser-known mid/small caps with the "
        "strongest price momentum.] "
        ":green[**green** = positive] :gray[· **gray** = neutral ·] "
        ":red[**red** = negative]"
    )

    with st.expander("Methodology — technical indicators used and why"):
        st.markdown(
            "**Trend filter — Price vs. 200-day moving average.** "
            "Buy names that are above their 200-DMA (long-term uptrend); avoid "
            "names below it (falling knives). The 200-DMA is the institutional "
            "default for distinguishing uptrend from downtrend.\n\n"
            "**Momentum — 1-month, 3-month, 6-month total returns.** "
            "Three timeframes prevent whipsaw: 1-month confirms the move is "
            "still alive, 3-month establishes the trend, 6-month rules out "
            "short-lived bounces in long downtrends. Weighted 20% / 30% / 20% — "
            "3-month gets the largest weight because it best balances signal "
            "and noise.\n\n"
            "**RSI(14) in [40, 70].** Relative Strength Index measures whether "
            "buying pressure is healthy. Below 40 = oversold (often falling "
            "knife); above 70 = overbought (often near a peak). The 40–70 band "
            "is the sweet spot of sustained buying without euphoria. "
            "The strength score adds a bonus when RSI is in 50–70 specifically.\n\n"
            "**Volume confirmation — current vs. 20-day average volume.** "
            "Rising prices on rising volume = real demand. Rising prices on "
            "falling volume = weak rally that often reverses. Used as a TA "
            "gate filter (the ✅/❌ chip on Big Names cards).\n\n"
            "**Liquidity & quality floor.** Market cap ≥ \\$500M and 20-day "
            "average dollar volume ≥ \\$10M. Keeps us out of thinly-traded "
            "names where prices can be moved with little capital.\n\n"
            "**Why this stack:** trend × momentum × volume is the classic "
            "CANSLIM / O'Neil framework adapted for systematic screening. "
            "It tends to surface names that are *already working* rather than "
            "stocks investors hope will turn around — appropriate for a "
            "low-drawdown, steady-returns objective."
        )

    st.divider()

    # Controls
    ctrl_cols = st.columns([1.3, 1.3, 1.3, 2.5])
    show_held = ctrl_cols[0].toggle("Show Big Names", value=True)
    show_watch = ctrl_cols[1].toggle("Show New Names", value=True)
    only_ta_pass = ctrl_cols[2].toggle("TA gate pass only", value=False)
    sort_by = ctrl_cols[3].radio(
        "Sort New Names by",
        ["Strength", "Sentiment", "Mentions"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if only_ta_pass:
        df = df[df["passes_gate"] == 1]

    # Sort key for NEW NAMES
    if sort_by == "Sentiment":
        new_sort_col, new_sort_asc = "sentiment_weighted", False
    elif sort_by == "Mentions":
        new_sort_col, new_sort_asc = "mention_count", False
    else:
        new_sort_col, new_sort_asc = "final_score", False

    for sec in SECTOR_ORDER:
        sec_df = df[df["sector"] == sec].copy()
        if sec_df.empty:
            continue

        held = sec_df[sec_df["is_blue_chip"] == 1].sort_values(
            "sentiment_weighted", ascending=False)
        watch = sec_df[sec_df["is_blue_chip"] == 0].sort_values(
            new_sort_col, ascending=new_sort_asc, na_position="last")

        label = SECTOR_LABELS.get(sec, sec)
        st.header(label)
        st.caption(f"{len(held)} big · {len(watch)} new")

        cols = st.columns(2, gap="large")
        if show_held:
            with cols[0]:
                st.markdown("###### Big Names")
                if held.empty:
                    st.caption("_no data_")
                for _, row in held.iterrows():
                    render_big_card(row)
        if show_watch:
            with cols[1]:
                st.markdown(f"###### New Names · _by {sort_by.lower()}_")
                if watch.empty:
                    st.caption("_no candidates_")
                for _, row in watch.head(10).iterrows():
                    render_new_card(row)


if __name__ == "__main__":
    main()
