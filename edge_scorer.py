"""
edge_scorer.py — Edge Scoring + Position Limit Check for Project AUGUR

Purpose:
    Scores each aligned market by comparing ensemble true probability
    against the Kalshi market YES price. Only passes markets with
    edge ratio >= 2.0 to the execution layer.

    Also checks each market's contract position limit before scoring
    and adjusts intended position size to stay within limits.

Kalshi API endpoints used:
    GET /trade-api/v2/markets/{ticker}  — live position limit check

Regulatory constraints:
    - Position limits are hard exchange constraints — never exceed
    - Read-only API calls only — no order placement here
    - All credentials from environment variables
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

MIN_EDGE_RATIO = 2.0    # true_prob / yes_price must be >= 2x
MIN_EDGE_ABS   = 0.03   # absolute edge (true_prob - price) >= 3%

KALSHI_BASE      = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def get_position_limit(ticker: str) -> Optional[float]:
    """
    Fetch max position size for the contract from Kalshi API.
    Returns None if unavailable.
    """
    url = f"{_base_url()}/markets/{ticker}"
    try:
        resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=8)
        resp.raise_for_status()
        market = resp.json().get("market", resp.json())
        limit = market.get("position_limit") or market.get("max_position_dollars")
        return float(limit) if limit is not None else None
    except Exception as e:
        _log.debug(f"Position limit fetch failed for {ticker}: {e}")
        return None


def score_market(
    market: dict,
    true_prob: float,
    intended_bet: float = 5.0,
) -> Optional[dict]:
    """
    Score one aligned market for edge.

    Args:
        market:       Aligned market dict (from market_mapper.py)
        true_prob:    Ensemble true probability (from weather_ensemble.py)
        intended_bet: Intended bet size in USD (for position limit check)

    Returns:
        Signal dict if edge qualifies, None if not.
    """
    ticker    = market.get("ticker", "")
    yes_price = market.get("_yes_price", 0)

    if yes_price <= 0 or true_prob <= 0:
        return None

    ratio = true_prob / yes_price
    edge  = true_prob - yes_price

    if ratio < MIN_EDGE_RATIO:
        return None
    if edge < MIN_EDGE_ABS:
        return None

    payout_mult = market.get("_payout_multiplier", round(1.0 / yes_price, 2))

    # Position limit check — reduce bet if needed rather than skip
    adj_bet = intended_bet
    position_limit = get_position_limit(ticker)
    if position_limit is not None and adj_bet > position_limit:
        _log.info(f"Reducing bet ${adj_bet:.2f} → ${position_limit:.2f} "
                  f"(position limit on {ticker})")
        adj_bet = position_limit

    # Score: reward high ratio × high edge
    score = ratio * edge

    return {
        "ticker":           ticker,
        "market_id":        market.get("event_ticker", ticker),
        "question":         market.get("title", ""),
        "location":         market.get("location", ""),
        "date":             market.get("resolution_dt", "")[:10],
        "resolution_dt":    market.get("resolution_dt", ""),
        "metric":           market.get("metric", "temp"),
        "threshold_type":   market.get("threshold_type", ""),
        "bracket_low":      market.get("bracket_low"),
        "bracket_high":     market.get("bracket_high"),
        "yes_price":        yes_price,
        "true_prob":        round(true_prob, 4),
        "edge":             round(edge, 4),
        "ratio":            round(ratio, 2),
        "payout_multiplier": payout_mult,
        "score":            round(score, 6),
        "intended_bet":     round(adj_bet, 2),
        "position_limit":   position_limit,
        "hours_to_resolution": market.get("_hours_to_resolution"),
    }


def score_all(
    markets: list[dict],
    true_probs: dict[str, float],
    intended_bet: float = 5.0,
) -> list[dict]:
    """
    Score a batch of aligned markets.

    Args:
        markets:      List of aligned market dicts
        true_probs:   Dict mapping ticker → true_prob from ensemble
        intended_bet: Default bet size for position limit checks

    Returns:
        Sorted list of signal dicts (best score first)
    """
    signals = []
    for m in markets:
        ticker = m.get("ticker", "")
        tp = true_probs.get(ticker)
        if tp is None:
            _log.debug(f"No true_prob for {ticker} — skipping")
            continue

        sig = score_market(m, tp, intended_bet=intended_bet)
        if sig:
            signals.append(sig)

    signals.sort(key=lambda s: -s["score"])
    _log.info(f"Edge scorer: {len(signals)} qualifying signals from {len(markets)} markets")
    return signals
