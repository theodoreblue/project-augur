"""
portfolio_manager.py — Live Portfolio Risk Manager for Project AUGUR

Purpose:
    Checks open position count LIVE from the Kalshi API every scan cycle.
    Enforces the 3-position cap. Does NOT rely on in-memory state
    so the bot can restart without losing risk awareness.

Kalshi API endpoints used:
    GET /trade-api/v2/portfolio/positions  — current open positions

Regulatory constraints:
    - All credentials from environment variables
    - Read-only endpoint — no orders placed here
    - Position count is always pulled fresh from Kalshi (not cached)
      so a bot restart never causes us to accidentally exceed the cap
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

MAX_OPEN_POSITIONS = 3
KALSHI_BASE        = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE   = "https://demo-api.kalshi.co/trade-api/v2"


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def get_open_positions() -> Optional[list[dict]]:
    """
    Fetch all currently open (unresolved) positions from Kalshi.
    Returns list of position dicts, or None on API error.
    """
    url = f"{_base_url()}/portfolio/positions"
    try:
        resp = requests.get(url, headers=_auth_headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        positions = data.get("market_positions", data.get("positions", []))
        # Only count positions where we hold contracts (quantity > 0)
        open_pos = [p for p in positions if int(p.get("quantity", 0)) > 0]
        return open_pos
    except Exception as e:
        _log.warning(f"Could not fetch portfolio positions: {e}")
        return None


def count_open_positions() -> Optional[int]:
    """Return count of open positions, or None if API call fails."""
    positions = get_open_positions()
    if positions is None:
        return None
    return len(positions)


def under_cap() -> bool:
    """
    Returns True if we have fewer than MAX_OPEN_POSITIONS open,
    meaning we can accept new signals.

    If the API call fails, returns False (safe default — don't trade blind).
    """
    count = count_open_positions()
    if count is None:
        _log.warning("Portfolio cap check failed — defaulting to NO (safe)")
        return False
    if count >= MAX_OPEN_POSITIONS:
        _log.info(f"Portfolio cap reached: {count}/{MAX_OPEN_POSITIONS} open positions")
        return False
    _log.info(f"Portfolio: {count}/{MAX_OPEN_POSITIONS} open positions — slot available")
    return True


def available_slots() -> int:
    """
    Return number of slots available for new positions.
    Returns 0 if API fails or cap reached.
    """
    count = count_open_positions()
    if count is None:
        return 0
    return max(0, MAX_OPEN_POSITIONS - count)
