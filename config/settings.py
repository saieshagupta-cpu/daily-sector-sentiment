"""Central config — loads API keys and YAML configs once.

Key resolution order:
1. Environment variables already set in the shell
2. api_keys.env at the repo root (local dev)
3. Streamlit secrets via st.secrets (deployed on Streamlit Cloud)
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# The package root (this file lives in <root>/config/settings.py)
PACKAGE_ROOT = Path(__file__).resolve().parent.parent

# Try .env at the package root first (new self-contained layout), then fall
# back to one level up (legacy layout where api_keys.env sat in amaltash_strats/).
for _candidate in (PACKAGE_ROOT / "api_keys.env", PACKAGE_ROOT.parent / "api_keys.env"):
    if _candidate.exists():
        load_dotenv(_candidate)
        break

# Backwards-compat: some code references this name.
ROOT = PACKAGE_ROOT
ENV_FILE = PACKAGE_ROOT / "api_keys.env"


def _read_key(name: str) -> str:
    """Read a secret from env first, then Streamlit secrets (when deployed)."""
    val = os.environ.get(name, "").strip()
    if val:
        return val
    # Streamlit secrets — only available when running under `streamlit run`.
    try:
        import streamlit as st  # type: ignore
        secrets = getattr(st, "secrets", None)
        if secrets is not None and name in secrets:
            return str(secrets[name]).strip()
    except Exception:
        pass
    return ""


@dataclass(frozen=True)
class APIKeys:
    finnhub: str
    marketaux: str

    @classmethod
    def from_env(cls) -> "APIKeys":
        return cls(
            finnhub=_read_key("FINNHUB_API_KEY"),
            marketaux=_read_key("MARKETAUX_API_KEY"),
        )


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return yaml.safe_load(f)


CONFIG_DIR = Path(__file__).resolve().parent
UNIVERSE: dict[str, list[dict[str, str]]] = _load_yaml(CONFIG_DIR / "universe.yaml")
FILTERS: dict[str, Any] = _load_yaml(CONFIG_DIR / "filters.yaml")
CANDIDATES: dict[str, list[str]] = _load_yaml(CONFIG_DIR / "candidates.yaml")

SECTORS: list[str] = list(UNIVERSE.keys())
BLUE_CHIP_TICKERS: set[str] = {row["ticker"] for rows in UNIVERSE.values() for row in rows}
TICKER_TO_SECTOR: dict[str, str] = {
    row["ticker"]: sector for sector, rows in UNIVERSE.items() for row in rows
}

# Candidate sector tagging — a ticker can belong to MULTIPLE sectors.
# E.g. VRT (Vertiv) lives in both Tech (data center hardware) and Energy (AI power infra).
CANDIDATE_SECTORS: dict[str, list[str]] = {}
for _sector, _tickers in CANDIDATES.items():
    for _t in _tickers:
        CANDIDATE_SECTORS.setdefault(_t, []).append(_sector)

# Convenience: flat set of all candidate tickers (deduped) for one-time strength fetch
ALL_CANDIDATES: set[str] = set(CANDIDATE_SECTORS.keys())

KEYS = APIKeys.from_env()


def require_key(name: str) -> str:
    val = getattr(KEYS, name, "")
    if not val:
        raise RuntimeError(f"Missing API key: {name}. Add it to {ENV_FILE}.")
    return val
