"""
liquidity_validator.py — Order Book Liquidity Checker for Project AUGUR

Purpose:
    Passively inspects Kalshi order book depth to verify there is
    sufficient liquidity to fill our intended bet without slippage.

    Rule: Available liquidity at target price must be >= 3x intended bet size.

    IMPORTANT — CFTC Compliance:
    This module is READ-ONLY. It never places orders to probe liquidity.
    No test orders, no wash trades, no rapid order placement.
    Only passive inspection of the public order book endpoint.

Kalshi API endpoints used:
    GET /trade-api/v2/markets/{ticker}/orderbook  — passive depth inspection

Regulatory constraints:
    - Read-only access only. No orders placed here.
    - CFTC-regulated exchange: wash trading and spoofing are federal violations.
    - Do not call this function in rapid loops — respect Kalshi rate limits.
    - Skipped markets logged to skipped.log with reason.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

DEPTH_MULTIPLIER     = 2.0    # require depth >= 2x intended bet
NEAR_MISS_THRESHOLD  = 3.0   # log near misses that pass 2x but fail 3x
SKIPPED_LOG          = "skipped.log"
NEAR_MISS_LOG        = "near_miss.log"
KALSHI_BASE      = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_DEMO_BASE = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def _log_skipped(ticker: str, question: str, reason: str) -> None:
    with open(SKIPPED_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "question": question,
            "reason": reason,
        }
        f.write(json.dumps(entry) + "\n")


def get_order_book(ticker: str) -> Optional[dict]:
    """
    Fetch order book for a Kalshi market.
    Kalshi orderbook endpoint is public — no auth required.
    Returns raw orderbook_fp dict or None on error.
    This is a PASSIVE, READ-ONLY call. No orders are placed.
    """
    url = f"{_base_url()}/markets/{ticker}/orderbook"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code == 401:
            resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"), timeout=8)
        resp.raise_for_status()
        data = resp.json()
        return data.get("orderbook_fp", data.get("orderbook", data))
    except Exception as e:
        _log.debug(f"Order book fetch failed for {ticker}: {e}")
        return None


def calculate_yes_depth(orderbook: dict, target_price: float, levels: int = 10) -> float:
    """
    Calculate available YES liquidity (in USD) near the target price.
    Kalshi orderbook_fp format: yes_dollars: [[price_str, size_str], ...]
    """
    yes_levels = orderbook.get("yes_dollars", orderbook.get("yes", []))
    if not yes_levels:
        return 0.0

    total_usd = 0.0
    count = 0

    try:
        if isinstance(yes_levels[0], list):
            sorted_levels = sorted(yes_levels, key=lambda x: -float(x[0]))
        else:
            sorted_levels = sorted(yes_levels, key=lambda x: -float(x.get("price", 0)))
    except Exception:
        return 0.0

    for level in sorted_levels:
        if count >= levels:
            break
        try:
            if isinstance(level, list):
                price = float(level[0])
                size  = float(level[1])
            else:
                price = float(level.get("price", 0))
                size  = float(level.get("delta", level.get("size", 0)))
            if abs(price - target_price) <= 0.30:
                total_usd += size * price
                count += 1
        except Exception:
            continue

    return round(total_usd, 2)


def validate(
    market: dict,
    intended_bet: float,
    rate_limit_sleep: float = 0.1,
) -> bool:
    """
    Check that the Kalshi order book has sufficient YES liquidity
    to absorb our intended bet without major slippage.

    Args:
        market:             Aligned market dict with ticker and _yes_price
        intended_bet:       USD amount we intend to bet
        rate_limit_sleep:   Seconds to sleep after API call (be gentle)

    Returns:
        True if liquid enough, False if market should be skipped.
    """
    ticker    = market.get("ticker", "")
    question  = market.get("title", "")
    yes_price = market.get("_yes_price", 0)
    required  = DEPTH_MULTIPLIER * intended_bet

    ob = get_order_book(ticker)
    time.sleep(rate_limit_sleep)  # gentle rate limiting

    if ob is None:
        # Can't verify — log and skip to be safe
        _log_skipped(ticker, question, "orderbook_fetch_failed")
        return False

    depth = calculate_yes_depth(ob, yes_price)

    if depth < required:
        _log_skipped(ticker, question,
                     f"insufficient_liquidity: depth=${depth:.2f} "
                     f"required=${required:.2f} (2x ${intended_bet:.2f})")
        return False

    # Near miss: passes 2x but would fail 3x — log for monitoring
    required_3x = NEAR_MISS_THRESHOLD * intended_bet
    if depth < required_3x:
        _log_near_miss(ticker, question, depth, required, required_3x)

    return True


def _log_near_miss(ticker: str, question: str, depth: float,
                   required_2x: float, required_3x: float) -> None:
    """Log markets that pass 2x depth but fail 3x for monitoring."""
    with open(NEAR_MISS_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "question": question,
            "depth": depth,
            "required_2x": round(required_2x, 2),
            "required_3x": round(required_3x, 2),
        }
        f.write(json.dumps(entry) + "\n")


def validate_batch(
    markets: list[dict],
    intended_bet: float,
) -> list[dict]:
    """
    Validate liquidity for a batch of markets.
    Returns only markets that pass the depth check.
    """
    passed = []
    for m in markets:
        if validate(m, intended_bet):
            passed.append(m)
    _log.info(f"Liquidity check: {len(passed)}/{len(markets)} markets pass")
    return passed
