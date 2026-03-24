"""
kalshi_executor.py — Kalshi Order Execution for Project AUGUR

Purpose:
    Places YES limit orders on Kalshi via the REST API.
    Handles retry logic, error logging, and trade journaling.

    This replaces ALL crypto/wallet/MetaMask/Polygon logic from the
    original Polymarket version. Kalshi is USD-native, CFTC-regulated,
    no crypto wallet required.

Kalshi API endpoints used:
    POST /trade-api/v2/portfolio/orders  — place a new order

Regulatory constraints (CFTC):
    - No wash trading: never simultaneously place buy and sell on same contract
    - No spoofing: never place orders with intent to cancel
    - No rapid order placement/cancellation — retry ONCE after 10 seconds, then stop
    - All credentials loaded from environment variables only
    - Only trade on KYC-verified Kalshi account
    - Order size must respect contract position limits (enforced upstream in sizing.py)

Environment variables:
    KALSHI_API_KEY   — your Kalshi API key (Bearer token)
    KALSHI_USE_DEMO  — set to "true" to use demo API (no real money)
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

KALSHI_BASE      = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_DEMO_BASE = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")
SIGNALS_LOG      = "signals.log"
ERRORS_LOG       = "errors.log"
RETRY_DELAY_SEC  = 10
MAX_RETRIES      = 1  # never retry more than once (CFTC compliance)


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


# ── Logging helpers ───────────────────────────────────────────────────────────

def _log_trade(signal: dict, bet_size: float, order_id: str,
               dry_run: bool = False) -> None:
    record = {
        "ts":              datetime.now(timezone.utc).isoformat(),
        "order_id":        order_id,
        "dry_run":         dry_run,
        "ticker":          signal.get("ticker"),
        "question":        signal.get("question"),
        "location":        signal.get("location"),
        "date":            signal.get("date"),
        "metric":          signal.get("metric"),
        "side":            "YES",
        "yes_price":       signal.get("yes_price"),
        "true_prob":       signal.get("true_prob"),
        "edge":            signal.get("edge"),
        "ratio":           signal.get("ratio"),
        "payout_multiplier": signal.get("payout_multiplier"),
        "bet_size_usd":    round(bet_size, 2),
        "resolution_dt":   signal.get("resolution_dt"),
    }
    with open(SIGNALS_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")
    _log.info(
        f"{'[DRY RUN] ' if dry_run else ''}Trade logged: {signal['question'][:60]} "
        f"| ${bet_size:.2f} @ {signal['yes_price']:.4f} | order_id={order_id}"
    )


def _log_error(context: dict, error: str) -> None:
    entry = {
        "ts":    datetime.now(timezone.utc).isoformat(),
        "error": error,
        **context,
    }
    with open(ERRORS_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")
    _log.error(f"Order failed: {error} | ticker={context.get('ticker')}")


# ── Order placement ───────────────────────────────────────────────────────────

def _build_order_payload(signal: dict, bet_size: float) -> dict:
    """
    Build the Kalshi order payload.

    Kalshi order fields:
        ticker:     contract ticker (e.g. "KXHIGHY-26MAR24-T78")
        action:     "buy" or "sell"
        side:       "yes" or "no"
        type:       "limit" or "market"
        count:      number of contracts (1 contract = $1 notional at max)
        yes_price:  limit price in cents (integer, 1-99)

    We buy YES contracts. count = USD bet size (since $1 notional per contract).
    """
    yes_price_cents = max(1, min(99, round(signal["yes_price"] * 100)))
    count = max(1, round(bet_size))  # 1 contract = $1 notional

    return {
        "ticker":    signal["ticker"],
        "action":    "buy",
        "side":      "yes",
        "type":      "limit",
        "count":     count,
        "yes_price": yes_price_cents,
        # client_order_id for idempotency — unique per attempt
        "client_order_id": str(uuid.uuid4()),
    }


def place_order(
    signal: dict,
    bet_size: float,
    dry_run: bool = True,
) -> bool:
    """
    Place a YES limit order on Kalshi for the given signal.

    Retry logic:
        - On failure: wait RETRY_DELAY_SEC seconds, try once more
        - On second failure: log full context to errors.log, return False
        - NEVER retry more than once (CFTC compliance)

    Args:
        signal:   Signal dict from edge_scorer.py
        bet_size: Dollar amount to bet (from sizing.py)
        dry_run:  If True, simulate — no real order placed

    Returns:
        True if order placed successfully (or simulated), False on failure
    """
    context = {
        "ticker":       signal.get("ticker"),
        "question":     signal.get("question"),
        "intended_bet": bet_size,
        "yes_price":    signal.get("yes_price"),
        "true_prob":    signal.get("true_prob"),
    }

    if dry_run:
        fake_order_id = f"DRY-{uuid.uuid4().hex[:8].upper()}"
        _log_trade(signal, bet_size, fake_order_id, dry_run=True)
        return True

    url     = f"{_base_url()}/portfolio/orders"
    headers = _auth_headers("POST", "/trade-api/v2/portfolio/orders")
    payload = _build_order_payload(signal, bet_size)

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            resp.raise_for_status()
            order_data = resp.json()
            order_id   = order_data.get("order", {}).get("order_id", "unknown")
            _log_trade(signal, bet_size, order_id, dry_run=False)
            return True

        except requests.exceptions.HTTPError as e:
            error_body = ""
            try:
                error_body = e.response.json()
            except Exception:
                error_body = str(e)

            if attempt < MAX_RETRIES:
                _log.warning(
                    f"Order attempt {attempt + 1} failed: {error_body}. "
                    f"Retrying in {RETRY_DELAY_SEC}s..."
                )
                time.sleep(RETRY_DELAY_SEC)
                # Generate new client_order_id for retry (idempotency)
                payload["client_order_id"] = str(uuid.uuid4())
            else:
                _log_error(context, str(error_body))
                return False

        except Exception as e:
            if attempt < MAX_RETRIES:
                _log.warning(f"Order attempt {attempt + 1} error: {e}. Retrying...")
                time.sleep(RETRY_DELAY_SEC)
            else:
                _log_error(context, str(e))
                return False

    return False
