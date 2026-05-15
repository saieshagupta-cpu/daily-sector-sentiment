"""FinBERT sentiment scorer.

ProsusAI/finbert is fine-tuned on financial text. First load downloads
~440MB to ~/.cache/huggingface/. Subsequent loads are instant.

Output convention:
    score in [-1, +1] where -1 = max negative, +1 = max positive.
    label in {"negative", "neutral", "positive"}.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

MODEL_NAME = "ProsusAI/finbert"
MAX_TOKENS = 512


@dataclass
class SentimentResult:
    label: str           # "negative" | "neutral" | "positive"
    score: float         # [-1, +1] compound
    confidence: float    # [0, 1] — top class probability


@lru_cache(maxsize=1)
def _load():
    tok = AutoTokenizer.from_pretrained(MODEL_NAME)
    mdl = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    mdl.eval()
    # FinBERT label order: positive=0, negative=1, neutral=2 (per model card)
    labels = ["positive", "negative", "neutral"]
    return tok, mdl, labels


def _logits_to_result(logits: torch.Tensor, labels: list[str]) -> SentimentResult:
    probs = torch.softmax(logits, dim=-1).squeeze(0).tolist()
    p_pos, p_neg, p_neu = probs[0], probs[1], probs[2]
    score = p_pos - p_neg  # signed compound, in [-1, +1]
    top_idx = int(max(range(3), key=lambda i: probs[i]))
    return SentimentResult(label=labels[top_idx], score=score, confidence=probs[top_idx])


def score_text(text: str) -> SentimentResult:
    text = (text or "").strip()
    if not text:
        return SentimentResult(label="neutral", score=0.0, confidence=1.0)
    tok, mdl, labels = _load()
    enc = tok(text, truncation=True, max_length=MAX_TOKENS, return_tensors="pt")
    with torch.no_grad():
        out = mdl(**enc)
    return _logits_to_result(out.logits, labels)


def score_texts(texts: Iterable[str], batch_size: int = 16) -> list[SentimentResult]:
    """Batched scoring — much faster than calling score_text in a loop."""
    items = [(t or "").strip() for t in texts]
    if not items:
        return []
    tok, mdl, labels = _load()
    results: list[SentimentResult] = []
    for i in range(0, len(items), batch_size):
        chunk = items[i : i + batch_size]
        enc = tok(chunk, truncation=True, max_length=MAX_TOKENS, padding=True, return_tensors="pt")
        with torch.no_grad():
            out = mdl(**enc)
        for row in out.logits:
            results.append(_logits_to_result(row.unsqueeze(0), labels))
    return results
