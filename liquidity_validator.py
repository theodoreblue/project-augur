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

DEPTH_MULTIPLIER = 3.0    # require depth >= 3x intended bet
SKIPPED_LOG      = "skipped.log"
KALSHI_BASE      = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"


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
    Returns dict with 'yes' and 'no' lists of {price, delta} levels.
    Returns None on error.

    This is a PASSIVE, READ-ONLY call. No orders are placed.
    """
    url = f"{_base_url()}/markets/{ticker}/orderbook"
    try:
        resp = requests.get(url, headers=_auth_headers(), timeout=8)
        resp.raise_for_status()
        return resp.json().get("orderbook", resp.json())
    except Exception as e:
        _log.debug(f"Order book fetch failed for {ticker}: {e}")
        return None


def calculate_yes_depth(orderbook: dict, target_price: float, levels: int = 3) -> float:
    """
    Calculate available YES liquidity (in USD) at or below target_price
    by reading the top N price levels from the yes (bid) side.

    Kalshi orderbook 'yes' entries represent bids to buy YES contracts.
    Each entry: {"price": int (cents), "delta": int (contract count)}

    Args:
        orderbook:    Raw orderbook dict from Kalshi API
        target_price: Our intended buy price (0.0-1.0 dollars)
        levels:       Number of price levels to aggregate

    Returns:
        Total USD liquidity available at/near target price
    """
    yes_levels = orderbook.get("yes", [])
    if not yes_levels:
        return 0.0

    target_cents = int(target_price * 100)
    total_usd = 0.0
    count = 0

    # Sort by price descending (best bids first)
    sorted_levels = sorted(yes_levels, key=lambda x: -int(x.get("price", 0)))

    for level in sorted_levels:
        if count >= levels:
            break
        price_cents = int(level.get("price", 0))
        delta = int(level.get("delta", 0))  # number of contracts at this price

        # Only count levels at or above our target (we're buying, want cheap YES)
        if price_cents <= target_cents + 5:  # 5-cent tolerance
            price_dollars = price_cents / 100
            usd_at_level = delta * price_dollars  # 1 contract = $1 notional
            total_usd += usd_at_level
            count += 1

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
                     f"required=${required:.2f} (3x ${intended_bet:.2f})")
        return False

    return True


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
