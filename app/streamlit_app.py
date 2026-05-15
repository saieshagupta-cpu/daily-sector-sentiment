"""Daily Sector Sentiment — dashboard.

Two views:
1. Landing — pick a sector you care about today.
2. Sector detail — Big Names + New Names side by side, each ticker expandable
   to its full stats + headline trail.

Methodology lives in a collapsed expander at the bottom.

Run:
    streamlit run app/streamlit_app.py
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


SECTOR_ORDER = ["energy", "healthcare", "minerals", "tech", "real_estate", "finance"]
SECTOR_LABELS = {
    "energy":      "Energy",
    "healthcare":  "Healthcare",
    "minerals":    "Minerals",
    "tech":        "Tech",
    "real_estate": "Real Estate",
    "finance":     "Finance",
}
SECTOR_BLURBS = {
    "energy":      "Oil & gas, AI power, nuclear, renewables, midstream",
    "healthcare":  "Pharma, biotech, medical devices, managed care",
    "minerals":    "Metals, mining, chemicals, battery materials",
    "tech":        "Semiconductors, software, AI infra, fintech",
    "real_estate": "REITs across industrial, residential, data center, retail",
    "finance":     "Banks, asset managers, payments, brokerages",
}


# ─────────────────────────────────────────────────────────────────────────────
# Company-name lookup
# ─────────────────────────────────────────────────────────────────────────────

_NAME_SUFFIXES_TO_STRIP = (
    " inc.", " inc", " corporation", " corp.", " corp", " co.", " co",
    " company", " holdings", " holding", " plc", " ltd.", " ltd",
    " limited", " llc", " n.v.", " s.a.", " ag",
)


def _prettify(raw: str) -> str:
    if not raw:
        return ""
    import re
    s = raw.strip()
    s = re.sub(r"\s*[-/]?\s*(class|cl)\s+[a-z](\s+shares?)?$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*[a-z]$", "", s, flags=re.I)
    s = s.title()
    lower = s.lower()
    for suf in _NAME_SUFFIXES_TO_STRIP:
        if lower.endswith(suf):
            s = s[: -len(suf)].rstrip(" ,.&-")
            break
    return s.strip()


@st.cache_data(ttl=3600)
def build_ticker_names() -> dict[str, str]:
    names: dict[str, str] = {}
    for rows in UNIVERSE.values():
        for row in rows:
            names[row["ticker"]] = row.get("name", row["ticker"])
    try:
        us_uni = get_us_universe()
        for ticker, info in us_uni.items():
            if ticker in names:
                continue
            raw = info.get("name") or ""
            if raw:
                names[ticker] = _prettify(raw)
    except Exception as e:
        print(f"[streamlit_app] universe unavailable: {e}")
    return names


TICKER_NAMES: dict[str, str] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Data
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
# Formatting
# ─────────────────────────────────────────────────────────────────────────────

def fmt_sentiment(score: float | None) -> str:
    if score is None or pd.isna(score):
        return ":gray[—]"
    s = f"{score:+.2f}"
    if score >= 0.30:  return f":green[**{s}**]"
    if score <= -0.30: return f":red[**{s}**]"
    return f":gray[{s}]"


def fmt_strength(comp: float | None) -> str:
    if comp is None or pd.isna(comp):
        return ":gray[—]"
    s = f"{comp:+.2f}"
    if comp >= 0.40:  return f":green[**{s}**]"
    if comp >= 0.10:  return f":green[{s}]"
    if comp <= -0.40: return f":red[**{s}**]"
    if comp <= -0.10: return f":red[{s}]"
    return f":gray[{s}]"


def fmt_pct(p: float | None) -> str:
    if p is None or pd.isna(p):
        return ":gray[—]"
    s = f"{p:+.1%}"
    if p >= 0.05:  return f":green[{s}]"
    if p <= -0.05: return f":red[{s}]"
    return f":gray[{s}]"


def fmt_ta(passes: int | None) -> str:
    if passes is None or pd.isna(passes):
        return ""
    return "✅" if int(passes) == 1 else "❌"


def _label(ticker: str) -> str:
    name = TICKER_NAMES.get(ticker, "")
    if not name or name.upper() == ticker:
        return f"**{ticker}**"
    return f"**{ticker}** &nbsp; *{name}*"


# ─────────────────────────────────────────────────────────────────────────────
# Cards
# ─────────────────────────────────────────────────────────────────────────────

def render_big_card(row: pd.Series) -> None:
    """BIG NAMES card — collapsed header is just label + sentiment + mentions."""
    ticker = row["ticker"]
    sent = row["sentiment_weighted"]
    mentions = int(row["mention_count"]) if pd.notna(row["mention_count"]) else 0

    header = f"{_label(ticker)} &nbsp;·&nbsp; {fmt_sentiment(sent)} &nbsp;·&nbsp; :gray[{mentions} stories]"
    with st.expander(header):
        # Price / RSI / TA gate
        m_cols = st.columns(4)
        price = row.get("price")
        rsi = row.get("rsi_14")
        passes = row.get("passes_gate")
        mcap = row.get("market_cap")
        if pd.notna(price):
            m_cols[0].metric("Price", f"${price:,.2f}")
        if pd.notna(rsi):
            m_cols[1].metric("RSI(14)", f"{rsi:.0f}")
        if pd.notna(mcap):
            mcap_str = f"${mcap/1e9:,.1f}B" if mcap >= 1e9 else f"${mcap/1e6:,.0f}M"
            m_cols[2].metric("Market cap", mcap_str)
        ta = fmt_ta(passes)
        if ta:
            m_cols[3].metric("TA gate", ta)
        _render_headlines(ticker, mentions)


def render_new_card(row: pd.Series) -> None:
    """NEW NAMES card — collapsed header is label + strength + 3m return."""
    ticker = row["ticker"]
    comp = row.get("strength_composite")
    r3 = row.get("ret_3m")
    sent = row["sentiment_weighted"]
    mentions = int(row["mention_count"]) if pd.notna(row["mention_count"]) else 0

    bits = [_label(ticker), fmt_strength(comp)]
    if pd.notna(r3):
        bits.append(f"3m {fmt_pct(r3)}")
    if mentions > 0:
        bits.append(fmt_sentiment(sent))
    bits.append(f":gray[{mentions} stories]")
    header = " &nbsp;·&nbsp; ".join(bits)

    with st.expander(header):
        # Strength breakdown
        r1 = row.get("ret_1m")
        r6 = row.get("ret_6m")
        pct_dma = row.get("pct_above_dma200")
        pct_high = row.get("pct_off_52w_high")
        price = row.get("price")
        rsi = row.get("rsi_14")

        m_cols = st.columns(6)
        if pd.notna(price):
            m_cols[0].metric("Price", f"${price:,.2f}")
        if pd.notna(r1):
            m_cols[1].metric("1-mo", f"{r1:+.1%}")
        if pd.notna(r3):
            m_cols[2].metric("3-mo", f"{r3:+.1%}")
        if pd.notna(r6):
            m_cols[3].metric("6-mo", f"{r6:+.1%}")
        if pd.notna(pct_dma):
            m_cols[4].metric("vs 200-DMA", f"{pct_dma:+.1%}")
        if pd.notna(rsi):
            m_cols[5].metric("RSI(14)", f"{rsi:.0f}")
        _render_headlines(ticker, mentions)


def _render_headlines(ticker: str, mentions: int) -> None:
    if mentions == 0:
        st.caption("_No news in lookback window — pure technical pick._")
        return
    st.markdown("**Headlines driving sentiment**")
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
        st.markdown(
            f"- {fmt_sentiment(sc)} &nbsp; [{headline}]({url}) :gray[— {outlet} · {pub}]",
            unsafe_allow_html=False,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Methodology — collapsed in footer
# ─────────────────────────────────────────────────────────────────────────────

def render_methodology() -> None:
    with st.expander("How it works"):
        st.markdown(
            """
**Big Names** are 10 fixed established leaders per sector, ranked by today's
news sentiment.

**New Names** come from a curated pool of ~30 lesser-known mid/small caps per
sector. They're ranked by a **strength composite** that blends:

| Component | Weight | What it measures |
|---|---|---|
| 1-month return | 20% | Is the move still alive? |
| 3-month return | 30% | Is the trend established? |
| 6-month return | 20% | Real recovery vs short bounce? |
| % above 200-DMA | 20% | Long-term trend strength |
| RSI(14) in 50–70 | 10% | Healthy momentum, not euphoric |

Sentiment from any headlines available is layered on top as a small bonus
(±0.20 max) so a hot news story can promote a moderately-strong name, but
technical action dominates.

**Quality floor:** market cap ≥ \\$500M and 20-day average dollar volume
≥ \\$10M — keeps thinly-traded names out.

**TA gate** (the ✅ / ❌ chip): price > 200-DMA **and** RSI ∈ [40, 70] **and**
today's volume > 20-day average. Trend + momentum + participation confirmation.

Why this stack: trend × momentum × volume is the classic CANSLIM / O'Neil
framework adapted for systematic screening. It surfaces names that are
*already working* rather than hoped-for turnarounds — appropriate for a
low-drawdown, steady-returns objective.

**Sources:** Finnhub, Marketaux, GDELT 2.0, 14 RSS feeds, 10 subreddits,
StockTwits. Refreshed automatically every morning via GitHub Actions.
"""
        )


# ─────────────────────────────────────────────────────────────────────────────
# Views
# ─────────────────────────────────────────────────────────────────────────────

def render_landing(df: pd.DataFrame, snap: str) -> None:
    st.markdown(
        '<div class="hero">'
        '<p class="kicker">DAILY SECTOR SENTIMENT</p>'
        '<h1 class="hero-title">Six sectors.<br/>One view.</h1>'
        '<p class="hero-sub">News-sentiment-scored Big Names and a strength-screened '
        'shortlist of rising mid-caps — refreshed every morning.</p>'
        f'<p class="hero-byline">by Saiesha Gupta &nbsp;·&nbsp; <span class="snap">snapshot {snap}</span></p>'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown('<h2 class="prompt">Pick a sector</h2>', unsafe_allow_html=True)

    # Grid of sector cards — 3 columns, rows of 3, last row may be partial.
    for row_start in range(0, len(SECTOR_ORDER), 3):
        cols = st.columns(3, gap="medium")
        for i, sec in enumerate(SECTOR_ORDER[row_start:row_start + 3]):
            idx = SECTOR_ORDER.index(sec) + 1
            with cols[i]:
                with st.container(border=True):
                    st.markdown(
                        f'<p class="tile-num">{idx:02d}</p>'
                        f'<h3 class="tile-title">{SECTOR_LABELS[sec]}</h3>'
                        f'<p class="tile-blurb">{SECTOR_BLURBS[sec]}</p>',
                        unsafe_allow_html=True,
                    )
                    if st.button("Open  →", key=f"open_{sec}", use_container_width=True):
                        st.session_state.sector = sec
                        st.rerun()

    st.write("")
    st.write("")
    render_methodology()


def render_sector(df: pd.DataFrame, sec: str, snap: str) -> None:
    if st.button("← all sectors", key="back"):
        st.session_state.sector = None
        st.rerun()

    sec_df = df[df["sector"] == sec].copy()
    held = sec_df[sec_df["is_blue_chip"] == 1].sort_values(
        "sentiment_weighted", ascending=False)
    watch = sec_df[sec_df["is_blue_chip"] == 0]
    idx = SECTOR_ORDER.index(sec) + 1

    st.markdown(
        f'<p class="kicker">SECTOR · {idx:02d}</p>'
        f'<h1 class="sector-title">{SECTOR_LABELS[sec]}</h1>'
        f'<p class="sector-blurb">{SECTOR_BLURBS[sec]}</p>'
        f'<p class="snap">snapshot {snap}</p>',
        unsafe_allow_html=True,
    )
    st.write("")

    # Controls — compact
    c1, c2, c3 = st.columns([1.2, 2.2, 4])
    only_ta_pass = c1.toggle("TA pass only", value=False, help="Hide names failing the TA gate")
    sort_by = c2.radio(
        "Sort new names by",
        ["Strength", "Sentiment", "Mentions"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if only_ta_pass:
        held = held[held["passes_gate"] == 1]
        watch = watch[watch["passes_gate"] == 1]

    sort_col = {"Strength": "final_score",
                "Sentiment": "sentiment_weighted",
                "Mentions": "mention_count"}[sort_by]
    watch = watch.sort_values(sort_col, ascending=False, na_position="last")

    st.divider()
    cols = st.columns(2, gap="large")
    with cols[0]:
        st.markdown(
            '<p class="col-num">01</p>'
            '<h2 class="col-title">Big Names</h2>'
            '<p class="col-sub">Established leaders, ranked by news sentiment</p>',
            unsafe_allow_html=True,
        )
        if held.empty:
            st.caption("_no data_")
        for _, row in held.iterrows():
            render_big_card(row)
    with cols[1]:
        st.markdown(
            '<p class="col-num">02</p>'
            '<h2 class="col-title">New Names</h2>'
            f'<p class="col-sub">Rising mid-caps with strong momentum · sorted by {sort_by.lower()}</p>',
            unsafe_allow_html=True,
        )
        if watch.empty:
            st.caption("_no candidates_")
        for _, row in watch.head(10).iterrows():
            render_new_card(row)

    st.write("")
    render_methodology()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Daily Sector Sentiment",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # ── CSS — magazine-style: bold typography, numbered sections, accent color
    st.markdown(
        """<style>
        /* Container */
        .block-container { padding-top: 2.5rem; padding-bottom: 5rem; max-width: 1400px; }

        /* Accent — used for section numbers + button outlines */
        :root { --accent: #FF5A1F; --ink: #0a0a0a; }

        /* Kicker label — small uppercase track above titles */
        .kicker {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size: 0.72rem !important;
            font-weight: 700;
            letter-spacing: 0.18em;
            color: var(--accent);
            margin: 0 0 0.6rem 0 !important;
            text-transform: uppercase;
        }

        /* Hero */
        .hero { padding: 0.5rem 0 2.5rem 0; border-bottom: 1px solid rgba(0,0,0,0.08); margin-bottom: 2.5rem; }
        .hero-title {
            font-size: 4.8rem !important;
            font-weight: 800;
            line-height: 0.95;
            letter-spacing: -0.035em;
            margin: 0 0 1.2rem 0 !important;
            color: var(--ink);
        }
        .hero-sub {
            font-size: 1.15rem;
            line-height: 1.5;
            margin: 0 0 1.2rem 0 !important;
            max-width: 640px;
            opacity: 0.78;
        }
        .hero-byline { font-size: 0.9rem; margin: 0 !important; opacity: 0.55; }
        .hero-byline .snap, .snap {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size: 0.82rem;
        }

        /* Prompt */
        .prompt {
            font-size: 2rem !important;
            font-weight: 800;
            letter-spacing: -0.02em;
            margin-top: 0.5rem !important;
            margin-bottom: 1.5rem !important;
        }

        /* Sector tile */
        div[data-testid="stContainer"] div[data-testid="stContainer"] {
            padding: 1.75rem 1.5rem !important;
            border-radius: 4px !important;
            transition: border-color 0.15s ease;
        }
        div[data-testid="stContainer"] div[data-testid="stContainer"]:hover {
            border-color: var(--accent) !important;
        }
        .tile-num {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size: 0.85rem;
            color: var(--accent);
            font-weight: 700;
            letter-spacing: 0.08em;
            margin: 0 0 1.5rem 0 !important;
        }
        .tile-title {
            font-size: 2rem !important;
            font-weight: 800;
            letter-spacing: -0.02em;
            line-height: 1;
            margin: 0 0 0.5rem 0 !important;
        }
        .tile-blurb {
            font-size: 0.92rem;
            opacity: 0.62;
            margin: 0 0 1.5rem 0 !important;
            min-height: 2.5rem;
        }

        /* Sector detail title */
        .sector-title {
            font-size: 4.5rem !important;
            font-weight: 800;
            letter-spacing: -0.035em;
            line-height: 1;
            margin: 0 0 0.6rem 0 !important;
        }
        .sector-blurb {
            font-size: 1.1rem;
            opacity: 0.65;
            margin: 0 0 0.75rem 0 !important;
        }

        /* Column headers (Big / New) */
        .col-num {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size: 0.78rem;
            color: var(--accent);
            font-weight: 700;
            letter-spacing: 0.12em;
            margin: 0 !important;
        }
        .col-title {
            font-size: 2.2rem !important;
            font-weight: 800;
            letter-spacing: -0.025em;
            line-height: 1;
            margin: 0.25rem 0 0.5rem 0 !important;
        }
        .col-sub {
            font-size: 0.92rem;
            opacity: 0.62;
            margin: 0 0 1.25rem 0 !important;
        }

        /* Expanders */
        details summary {
            padding: 0.65rem 0.85rem !important;
            font-size: 0.98rem;
            border-radius: 3px;
        }
        details > div { padding-top: 0.5rem; }

        /* Buttons */
        button[data-testid^="stBaseButton"] {
            border-radius: 3px !important;
            font-weight: 600 !important;
            letter-spacing: 0.02em;
        }

        /* Back button */
        button[key="back"] {
            border: none !important;
            background: transparent !important;
            padding-left: 0 !important;
            color: var(--ink) !important;
            opacity: 0.6;
        }
        button[key="back"]:hover { opacity: 1; color: var(--accent) !important; }

        /* Hide Streamlit header / footer noise */
        header[data-testid="stHeader"] { background: transparent; }
        </style>""",
        unsafe_allow_html=True,
    )

    snap = latest_snapshot_date()
    if not snap:
        st.warning("No data yet. Run the refresh job first.")
        st.stop()

    df = load_scores(snap)

    global TICKER_NAMES
    if not TICKER_NAMES:
        TICKER_NAMES = build_ticker_names()

    if "sector" not in st.session_state:
        st.session_state.sector = None

    if st.session_state.sector is None:
        render_landing(df, snap)
    else:
        render_sector(df, st.session_state.sector, snap)


if __name__ == "__main__":
    main()
