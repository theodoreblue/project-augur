"""
kalshi_scanner.py — Kalshi Market Scanner for Project AUGUR

Purpose:
    Fetches live weather prediction markets from Kalshi, applies
    time-to-resolution and minimum-contract-size filters, then passes
    valid markets downstream to market_mapper.py.

Kalshi API endpoints used:
    GET /trade-api/v2/markets             — list active markets (filtered by category)
    GET /trade-api/v2/markets/{ticker}    — single market detail (price refresh)

Regulatory constraints:
    - All credentials loaded from environment variables only (KALSHI_API_KEY)
    - Read-only at this stage — no orders placed here
    - CFTC-regulated: no wash trades, no spoofing, passive data collection only
    - Only KYC-verified accounts may use live trading endpoints

Environment variables:
    KALSHI_API_KEY   — your Kalshi API key (Bearer token)
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KALSHI_BASE          = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE     = "https://demo-api.kalshi.co/trade-api/v2"

MIN_HOURS            = 2.0    # ignore markets resolving sooner
MAX_HOURS            = 72.0   # ignore markets resolving later
MAX_CONTRACT_PCT     = 0.02   # skip if min contract > 2% of bankroll
SKIPPED_LOG          = "skipped.log"

WEATHER_KEYWORDS = [
    "rain", "snow", "temperature", "temp", "degrees", "fahrenheit",
    "celsius", "precipitation", "wind", "storm", "hurricane",
    "heat", "cold", "freeze", "frost", "flood", "high of", "low of",
]


# ── Auth header ───────────────────────────────────────────────────────────────

def _auth_headers() -> dict:
    api_key = os.getenv("KALSHI_API_KEY", "")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _base_url() -> str:
    """Return live or demo base URL based on env."""
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


# ── Logging ───────────────────────────────────────────────────────────────────

def _log_skipped(ticker: str, question: str, reason: str) -> None:
    with open(SKIPPED_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "question": question,
            "reason": reason,
        }
        f.write(json.dumps(entry) + "\n")


# ── Fetch markets ─────────────────────────────────────────────────────────────

def fetch_weather_markets(limit: int = 200) -> list[dict]:
    """
    Fetch active markets from Kalshi, filtered to weather category.
    Paginates until we have enough or run out.
    Returns raw market dicts from the API.
    """
    url = f"{_base_url()}/markets"
    headers = _auth_headers()
    all_markets = []
    cursor = None

    _log.info("Fetching weather markets from Kalshi...")

    while len(all_markets) < limit:
        params = {
            "status": "open",
            "limit": min(200, limit - len(all_markets)),
        }
        # Try category filter first; Kalshi may use event_category or series_category
        params["event_category"] = "weather"
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            # If category filter fails, fall back to keyword scan of all markets
            if e.response is not None and e.response.status_code == 400:
                _log.warning("Category filter rejected — falling back to keyword scan")
                params.pop("event_category", None)
                resp = requests.get(url, headers=headers, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            else:
                _log.error(f"Failed to fetch markets: {e}")
                return []
        except Exception as e:
            _log.error(f"Failed to fetch markets: {e}")
            return []

        markets = data.get("markets", [])
        if not markets:
            break

        all_markets.extend(markets)
        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.1)  # gentle rate limit

    _log.info(f"Raw markets fetched: {len(all_markets)}")

    # Keyword filter for weather content
    weather = [
        m for m in all_markets
        if any(kw in (m.get("title", "") + m.get("rules_primary", "")).lower()
               for kw in WEATHER_KEYWORDS)
    ]
    _log.info(f"Weather markets after keyword filter: {len(weather)}")
    return weather


# ── Time-to-resolution filter ─────────────────────────────────────────────────

def filter_by_time_window(markets: list[dict]) -> list[dict]:
    """
    Keep only markets resolving between MIN_HOURS and MAX_HOURS from now.
    Kalshi uses close_time (when market closes to trading) and
    expiration_time (when it resolves).
    """
    now = datetime.now(timezone.utc)
    valid = []

    for m in markets:
        ticker = m.get("ticker", "")
        question = m.get("title", "")

        # Prefer expiration_time, fall back to close_time
        raw = m.get("expiration_time") or m.get("close_time") or ""
        if not raw:
            _log_skipped(ticker, question, "no_resolution_time")
            continue

        try:
            res_dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            hours = (res_dt - now).total_seconds() / 3600
        except Exception:
            _log_skipped(ticker, question, "unparseable_resolution_time")
            continue

        if hours < MIN_HOURS or hours > MAX_HOURS:
            _log_skipped(ticker, question,
                         f"time_window: {hours:.1f}h outside {MIN_HOURS}-{MAX_HOURS}h")
            continue

        m["_hours_to_resolution"] = round(hours, 2)
        m["_resolution_dt"] = res_dt.isoformat()
        valid.append(m)

    _log.info(f"Time filter: {len(valid)}/{len(markets)} markets in 2-72h window")
    return valid


# ── Min contract size filter ──────────────────────────────────────────────────

def filter_by_contract_size(markets: list[dict], bankroll: float) -> list[dict]:
    """
    Skip any market where the minimum tradeable contract size exceeds
    MAX_CONTRACT_PCT of current bankroll.

    Kalshi's minimum notional is typically $1 (notional_value_dollars).
    tick_size (in cents) defines minimum price increment.
    """
    min_allowed = bankroll * MAX_CONTRACT_PCT
    valid = []

    for m in markets:
        ticker = m.get("ticker", "")
        question = m.get("title", "")

        try:
            notional = float(m.get("notional_value_dollars", "1.0") or 1.0)
        except (ValueError, TypeError):
            notional = 1.0

        if notional > min_allowed:
            _log_skipped(ticker, question,
                         f"min_contract_too_large: ${notional:.2f} > ${min_allowed:.2f} (2% of ${bankroll:.2f})")
            continue

        m["_notional"] = notional
        valid.append(m)

    _log.info(f"Contract size filter: {len(valid)}/{len(markets)} markets pass")
    return valid


# ── Price extraction ──────────────────────────────────────────────────────────

def extract_prices(markets: list[dict]) -> list[dict]:
    """
    Extract yes_ask_dollars and no_ask_dollars as floats.
    Kalshi returns these as string dollars ("0.6500").
    Skip markets with no valid price.
    """
    priced = []
    for m in markets:
        ticker = m.get("ticker", "")
        question = m.get("title", "")

        try:
            yes_ask = float(m.get("yes_ask_dollars") or 0)
            yes_bid = float(m.get("yes_bid_dollars") or 0)
            # Use midpoint if both sides exist, otherwise ask
            if yes_ask > 0 and yes_bid > 0:
                yes_price = (yes_ask + yes_bid) / 2
            elif yes_ask > 0:
                yes_price = yes_ask
            else:
                _log_skipped(ticker, question, "no_valid_yes_price")
                continue

            if yes_price <= 0 or yes_price >= 1:
                _log_skipped(ticker, question, f"invalid_yes_price: {yes_price}")
                continue

            m["_yes_price"] = round(yes_price, 4)
            m["_payout_multiplier"] = round(1.0 / yes_price, 2)
            priced.append(m)

        except (ValueError, TypeError) as e:
            _log_skipped(ticker, question, f"price_parse_error: {e}")

    _log.info(f"Price extraction: {len(priced)}/{len(markets)} markets have valid prices")
    return priced


# ── Main scanner entry point ──────────────────────────────────────────────────

def scan_markets(bankroll: float = 50.0) -> list[dict]:
    """
    Full scanner pipeline:
    1. Fetch weather markets from Kalshi
    2. Time-to-resolution filter (2-72h)
    3. Minimum contract size filter
    4. Price extraction

    Returns list of market dicts ready for market_mapper.py
    """
    markets = fetch_weather_markets()
    if not markets:
        _log.warning("No markets returned from Kalshi. Check API key and connectivity.")
        return []

    markets = filter_by_time_window(markets)
    markets = filter_by_contract_size(markets, bankroll)
    markets = extract_prices(markets)

    _log.info(f"Scanner complete: {len(markets)} markets ready for alignment")
    return markets


if __name__ == "__main__":
    markets = scan_markets(bankroll=50.0)
    print(f"\nFound {len(markets)} scannable markets:")
    for m in markets[:10]:
        print(f"  [{m['ticker']}] {m['title'][:70]}")
        print(f"     YES price: ${m['_yes_price']:.4f} ({m['_payout_multiplier']:.1f}x)  "
              f"  resolves in {m['_hours_to_resolution']:.1f}h")
