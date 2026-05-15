"""yfinance OHLCV + technical-analysis gate.

The TA gate is the *quality control* layer on top of sentiment. A ticker can
have great sentiment but be in a downtrend or illiquid — we don't want it.

Gate rules (configurable in filters.yaml):
- price > 200-DMA          (uptrend filter)
- RSI(14) in [40, 70]      (neither oversold nor euphoric)
- vol > 20-day avg          (participation confirmation)
- avg dollar volume > $X    (liquidity floor)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

from config.settings import FILTERS


@dataclass
class TechResult:
    ticker: str
    price: float
    dma_200: float
    rsi_14: float
    vol_today: float
    vol_avg_20: float
    avg_dollar_vol_20: float
    market_cap: float | None
    passes_gate: bool
    gate_reasons: list[str]
    as_of: datetime


def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff().dropna()
    if len(delta) < period:
        return float("nan")
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean().iloc[-1]
    avg_loss = loss.rolling(period).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def fetch_technicals(ticker: str) -> TechResult | None:
    cfg = FILTERS["technical_gate"]
    liq = FILTERS["liquidity"]
    dma_period = int(cfg["trend_dma"])
    rsi_period = int(cfg["rsi_period"])
    rsi_min = float(cfg["rsi_min"])
    rsi_max = float(cfg["rsi_max"])
    vol_lookback = int(cfg["volume_lookback"])
    min_mcap = float(liq["min_market_cap_usd"])
    min_dollar_vol = float(liq["min_avg_dollar_vol"])

    try:
        hist = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=False)
    except Exception as e:
        print(f"[ta] {ticker}: history fetch failed: {e}")
        return None
    if hist.empty or len(hist) < dma_period:
        return None

    close = hist["Close"].astype(float)
    volume = hist["Volume"].astype(float)
    price = float(close.iloc[-1])
    dma = float(close.rolling(dma_period).mean().iloc[-1])
    rsi = _rsi(close, rsi_period)
    vol_today = float(volume.iloc[-1])
    vol_avg = float(volume.rolling(vol_lookback).mean().iloc[-1])
    avg_dollar_vol = float((close * volume).rolling(vol_lookback).mean().iloc[-1])

    # Market cap via yfinance fast_info — best-effort, multiple keys to try.
    market_cap: float | None = None
    try:
        fi = yf.Ticker(ticker).fast_info
        for key in ("marketCap", "market_cap"):
            val = getattr(fi, key, None) if hasattr(fi, key) else None
            if val is None:
                try:
                    val = fi[key]  # type: ignore[index]
                except Exception:
                    val = None
            if val:
                market_cap = float(val)
                break
    except Exception:
        pass

    reasons: list[str] = []
    if not (price > dma):
        reasons.append(f"price {price:.2f} <= 200DMA {dma:.2f}")
    if not (rsi_min <= rsi <= rsi_max):
        reasons.append(f"RSI {rsi:.1f} out of [{rsi_min:.0f},{rsi_max:.0f}]")
    if not (vol_today > vol_avg):
        reasons.append(f"vol {vol_today:,.0f} <= 20d avg {vol_avg:,.0f}")
    if avg_dollar_vol < min_dollar_vol:
        reasons.append(f"avg $vol {avg_dollar_vol:,.0f} < {min_dollar_vol:,.0f}")
    if market_cap is not None and market_cap < min_mcap:
        reasons.append(f"mcap {market_cap:,.0f} < {min_mcap:,.0f}")

    return TechResult(
        ticker=ticker,
        price=price,
        dma_200=dma,
        rsi_14=rsi,
        vol_today=vol_today,
        vol_avg_20=vol_avg,
        avg_dollar_vol_20=avg_dollar_vol,
        market_cap=market_cap,
        passes_gate=(len(reasons) == 0),
        gate_reasons=reasons,
        as_of=datetime.utcnow(),
    )


def fetch_many(tickers: list[str]) -> dict[str, TechResult]:
    out: dict[str, TechResult] = {}
    for t in tickers:
        res = fetch_technicals(t)
        if res is not None:
            out[t] = res
    return out
