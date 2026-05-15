"""Strength scorer — measures 'how well is this stock actually doing?'.

Composite strength is built from price action only (sentiment is layered on
later as a bonus). The intent: a stock that's already in an uptrend with
healthy momentum, regardless of whether it's been in the news today.

Components (all normalised to roughly [-1, +1] then weighted):
- 1-month return        weight 0.20
- 3-month return        weight 0.30
- 6-month return        weight 0.20
- distance above 200-DMA (%)         weight 0.20
- RSI sweet spot bonus (50-70)       weight 0.10

Final composite is then nudged by sentiment in the pipeline.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class StrengthResult:
    ticker: str
    price: float
    ret_1m: float
    ret_3m: float
    ret_6m: float
    dma_200: float
    pct_above_dma200: float
    rsi_14: float
    high_52w: float
    pct_off_52w_high: float       # negative number; -5% means 5% below high
    composite: float              # final strength score, roughly [-1, +1]
    as_of: datetime


def _rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff().dropna()
    if len(delta) < period:
        return float("nan")
    gain = delta.where(delta > 0, 0.0).rolling(period).mean().iloc[-1]
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean().iloc[-1]
    if loss == 0:
        return 100.0
    rs = gain / loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _ret_n_days(close: pd.Series, n: int) -> float:
    if len(close) < n + 1:
        return 0.0
    return float((close.iloc[-1] / close.iloc[-1 - n]) - 1.0)


def _tanh_clip(x: float, scale: float = 0.2) -> float:
    """Smoothly squash x into roughly [-1, +1]. Scale controls steepness."""
    return float(math.tanh(x / scale))


def compute_strength(ticker: str) -> StrengthResult | None:
    try:
        hist = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=False)
    except Exception as e:
        print(f"[strength] {ticker}: history fetch failed: {e}")
        return None
    if hist.empty or len(hist) < 200:
        return None

    close = hist["Close"].astype(float)
    price = float(close.iloc[-1])
    dma = float(close.rolling(200).mean().iloc[-1])
    rsi = _rsi(close, 14)

    r1 = _ret_n_days(close, 21)
    r3 = _ret_n_days(close, 63)
    r6 = _ret_n_days(close, 126)
    pct_above = (price / dma - 1.0) if dma else 0.0
    high_52w = float(close.rolling(252, min_periods=50).max().iloc[-1])
    pct_off_high = (price / high_52w - 1.0) if high_52w else 0.0

    # Components, each ~[-1, +1] via tanh
    c_r1 = _tanh_clip(r1, scale=0.10)         # 10% move ≈ 0.76
    c_r3 = _tanh_clip(r3, scale=0.20)         # 20% over 3m ≈ 0.76
    c_r6 = _tanh_clip(r6, scale=0.35)         # 35% over 6m ≈ 0.76
    c_dma = _tanh_clip(pct_above, scale=0.10)
    # RSI sweet spot 50-70 is +; extreme (>80 or <30) penalises
    if 50 <= rsi <= 70:
        c_rsi = (rsi - 50) / 20.0             # 0 .. +1 within sweet spot
    elif rsi < 50:
        c_rsi = -(50 - rsi) / 50.0            # below 50 is mildly negative
    else:  # > 70
        c_rsi = -(rsi - 70) / 30.0            # overbought penalty

    composite = (
        0.20 * c_r1 +
        0.30 * c_r3 +
        0.20 * c_r6 +
        0.20 * c_dma +
        0.10 * c_rsi
    )

    return StrengthResult(
        ticker=ticker,
        price=price,
        ret_1m=r1,
        ret_3m=r3,
        ret_6m=r6,
        dma_200=dma,
        pct_above_dma200=pct_above,
        rsi_14=rsi,
        high_52w=high_52w,
        pct_off_52w_high=pct_off_high,
        composite=float(composite),
        as_of=datetime.utcnow(),
    )


def compute_many(tickers: list[str]) -> dict[str, StrengthResult]:
    out: dict[str, StrengthResult] = {}
    for i, t in enumerate(tickers):
        r = compute_strength(t)
        if r is not None:
            out[t] = r
        if (i + 1) % 20 == 0:
            print(f"[strength] {i + 1}/{len(tickers)}")
    return out
