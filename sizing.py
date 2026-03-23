"""
sizing.py — Kelly Criterion Position Sizing for Project AUGUR (Kalshi)

Purpose:
    Computes optimal bet size using Kelly Criterion, then applies
    Kalshi-specific caps in strict priority order:
        1. Kelly output capped at 5% of bankroll
        2. Contract position limit from market metadata (live from API)
        3. Available USD buying power from Kalshi portfolio balance
        4. Rounded down to Kalshi's minimum tick increment

Kalshi API endpoints used:
    GET /trade-api/v2/portfolio/balance   — live USD buying power
    GET /trade-api/v2/markets/{ticker}    — contract position limit

Regulatory constraints:
    - All API keys loaded from environment (KALSHI_API_KEY)
    - Never size a bet larger than available balance (CFTC compliance)
    - Position limits are hard constraints from Kalshi — never exceed
    - All amounts returned in USD dollars
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

MAX_KELLY_PCT    = 0.05    # 5% hard cap per trade
MIN_BET_DOLLARS  = 1.0     # minimum $1 per trade
MAX_BET_DOLLARS  = 500.0   # absolute ceiling per trade
KALSHI_BASE      = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


# ── Kalshi API helpers ────────────────────────────────────────────────────────

def get_live_balance() -> Optional[float]:
    """
    Fetch current USD buying power from Kalshi portfolio balance endpoint.
    Returns None if the call fails (treat as unknown — size conservatively).
    """
    url = f"{_base_url()}/portfolio/balance"
    try:
        resp = requests.get(url, headers=_auth_headers("GET", "/trade-api/v2/portfolio/balance"), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # balance field is in cents as integer or dollar string depending on version
        balance = data.get("balance") or data.get("available_balance") or 0
        # Kalshi returns cents in some versions
        if isinstance(balance, int) and balance > 1000:
            return round(balance / 100, 2)
        return round(float(balance), 2)
    except Exception as e:
        _log.warning(f"Could not fetch live balance: {e}")
        return None


def get_position_limit(ticker: str) -> Optional[float]:
    """
    Fetch the maximum position size (in USD) for a specific contract.
    Returns None if the call fails (skip the position limit check).
    """
    url = f"{_base_url()}/markets/{ticker}"
    try:
        resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        market = data.get("market", data)
        # position_limit may be in notional dollars
        limit = market.get("position_limit") or market.get("max_position_dollars")
        if limit is not None:
            return float(limit)
        return None
    except Exception as e:
        _log.warning(f"Could not fetch position limit for {ticker}: {e}")
        return None


def get_tick_size(market: dict) -> float:
    """
    Get minimum price increment (tick size) in dollars.
    Kalshi tick_size is in cents (integer).
    """
    tick_cents = market.get("tick_size", 1)
    return round(tick_cents / 100, 4)


# ── Kelly formula ─────────────────────────────────────────────────────────────

def kelly_fraction(true_prob: float, payout_multiplier: float) -> float:
    """
    Kelly Criterion: f = (b*p - q) / b
    where b = net odds (payout_multiplier - 1), p = true prob, q = 1 - p.
    Returns 0.0 if no edge. Capped at MAX_KELLY_PCT.
    """
    if not (0 < true_prob < 1) or payout_multiplier <= 1:
        return 0.0

    b = payout_multiplier - 1
    p = true_prob
    q = 1.0 - p
    f = (b * p - q) / b

    if f <= 0:
        return 0.0
    return min(f, MAX_KELLY_PCT)


# ── Main sizing function ──────────────────────────────────────────────────────

def size_bet(
    bankroll: float,
    true_prob: float,
    payout_multiplier: float,
    ticker: str = "",
    market: Optional[dict] = None,
    fetch_live_balance: bool = True,
) -> dict:
    """
    Compute final bet size for a Kalshi market with all caps applied.

    Args:
        bankroll:           Last known bankroll (local state)
        true_prob:          Ensemble model probability (0-1)
        payout_multiplier:  Gross payout per $1 bet (1.0 / yes_price)
        ticker:             Kalshi contract ticker (for position limit lookup)
        market:             Raw market dict (for tick_size)
        fetch_live_balance: If True, verify against live Kalshi balance

    Returns:
        Dict with keys: bet_size, kelly_fraction, limiting_factor, notes
    """
    notes = []

    # Step 1: Kelly fraction
    fraction = kelly_fraction(true_prob, payout_multiplier)
    if fraction == 0.0:
        return {"bet_size": 0.0, "kelly_fraction": 0.0,
                "limiting_factor": "no_edge", "notes": ["Kelly says no edge"]}

    # Step 2: Raw Kelly amount
    raw = bankroll * fraction
    bet = min(raw, MAX_BET_DOLLARS)
    notes.append(f"Kelly raw=${raw:.2f} (fraction={fraction:.1%})")

    if raw > MAX_BET_DOLLARS:
        notes.append(f"Capped at MAX_BET_DOLLARS=${MAX_BET_DOLLARS}")

    # Step 3: Contract position limit (live from API)
    if ticker:
        pos_limit = get_position_limit(ticker)
        if pos_limit is not None and bet > pos_limit:
            bet = pos_limit
            notes.append(f"Position limit cap=${pos_limit:.2f}")

    # Step 4: Live balance check (never bet more than available)
    if fetch_live_balance:
        live_bal = get_live_balance()
        if live_bal is not None:
            if bet > live_bal:
                bet = live_bal
                notes.append(f"Live balance cap=${live_bal:.2f}")
        else:
            # Fallback to local bankroll
            if bet > bankroll:
                bet = bankroll
                notes.append(f"Local bankroll cap=${bankroll:.2f}")
    else:
        if bet > bankroll:
            bet = bankroll
            notes.append(f"Local bankroll cap=${bankroll:.2f}")

    # Step 5: Tick size rounding (round DOWN to nearest tick)
    tick = get_tick_size(market) if market else 0.01
    if tick > 0:
        import math
        bet = math.floor(bet / tick) * tick
        bet = round(bet, 4)

    # Step 6: Minimum bet check
    if bet < MIN_BET_DOLLARS:
        return {"bet_size": 0.0, "kelly_fraction": fraction,
                "limiting_factor": "below_minimum",
                "notes": notes + [f"Below minimum ${MIN_BET_DOLLARS}"]}

    limiting = "kelly"
    if "Position limit" in " ".join(notes):
        limiting = "position_limit"
    elif "Live balance" in " ".join(notes):
        limiting = "live_balance"
    elif "Local bankroll" in " ".join(notes):
        limiting = "local_bankroll"

    return {
        "bet_size": round(bet, 2),
        "kelly_fraction": round(fraction, 4),
        "limiting_factor": limiting,
        "notes": notes,
    }


if __name__ == "__main__":
    print("Sizing sanity check (no API calls)\n")
    cases = [
        (50.0,   0.333, 11.0, "Phoenix 95F+ — 3.5x edge"),
        (50.0,   0.103, 83.0, "Denver bracket — 8.6x edge"),
        (1000.0, 0.500,  2.0, "Fair coin (no edge)"),
        (1000.0, 0.200, 10.0, "Good long shot"),
        (50.0,   0.050, 50.0, "Extreme long shot"),
    ]
    for bankroll, p, mult, label in cases:
        result = size_bet(
            bankroll=bankroll,
            true_prob=p,
            payout_multiplier=mult,
            fetch_live_balance=False,
        )
        print(f"  {label}")
        print(f"    bet=${result['bet_size']:.2f}  kelly={result['kelly_fraction']:.1%}  "
              f"limit={result['limiting_factor']}")
        print(f"    {' | '.join(result['notes'])}\n")
