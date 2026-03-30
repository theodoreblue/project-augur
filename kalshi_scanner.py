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
KALSHI_BASE          = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_DEMO_BASE     = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")

MIN_HOURS                  = 2.0    # ignore markets resolving sooner
MAX_HOURS                  = 72.0   # ignore markets resolving later
MAX_HOURS_HIGH_CONVICTION  = 96.0   # extended window for edge ratio >= 5x
MAX_CONTRACT_PCT     = 0.02   # skip if min contract > 2% of bankroll
SKIPPED_LOG          = "skipped.log"

WEATHER_KEYWORDS = [
    "rain", "snow", "temperature", "temp", "degrees", "fahrenheit",
    "celsius", "precipitation", "wind", "storm", "hurricane",
    "heat", "cold", "freeze", "frost", "flood", "high of", "low of",
    "maximum temperature", "minimum temperature", "max temperature",
    "highest temperature", "lowest temperature", "will the high",
    "will the low", "will it snow", "will it rain",
]

# Known Kalshi weather series tickers — use these to fetch markets directly
# More reliable than keyword filtering since Kalshi categorizes inconsistently
WEATHER_SERIES = [
    "KXHIGHTPHX", "KXHIGHNY", "KXHIGHNY0", "KXHIGHMIA", "KXHIGHCHI",
    "KXHIGHDEN", "KXHIGHHOU", "KXHIGHLAX", "KXHIGHAUS", "KXHIGHTSEA",
    "KXHIGHTDAL", "KXHIGHTSATX", "KXHIGHTMIN", "KXHIGHTOKC", "KXHIGHTEMPDEN",
    "KXHIGHTHOU", "KXHIGHTATL", "KXHIGHTSFO", "KXHIGHPHIL", "KXPHILHIGH",
    "KXHOUHIGH", "KXHIGHOU", "KXDENHIGH",
    "HIGHNY", "HIGHNY0", "HIGHCHI", "HIGHMIA", "HIGHAUS",
    "KXLOWNYC", "KXLOWNY", "KXLOWLAX", "KXLOWTLAX", "KXLOWCHI",
    "KXLOWTCHI", "KXLOWDEN", "KXLOWTDEN", "KXLOWMIA", "KXLOWTMIA",
    "KXLOWAUS", "KXLOWTAUS", "KXLOWPHIL", "KXLOWTPHIL",
    "KXSNOWNYC", "KXSNOWNY", "KXSNOWNYM", "SNOWNY", "SNOWNYM",
    "KXNYCSNOWM", "KXCHISNOWM", "SNOWCHIM", "KXSNOWCHIM",
    "KXDENSNOWM", "KXSEASNOWM", "KXHOUSNOWM", "KXLAXSNOWM",
    "KXDALSNOWM", "KXAUSSNOWM", "KXSFOSNOWM", "KXMIASNOWM", "KXSNOWAZ",
    "KXRAINNYCM", "RAINNYCM", "KXCITIESWEATHER",
]


# ── Auth header ───────────────────────────────────────────────────────────────

def _auth_headers(method: str = "GET", path: str = "/trade-api/v2/markets") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def _base_url() -> str:
    """Return live or demo base URL based on env."""
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


# ── Peak Hours Logic ──────────────────────────────────────────────────────────

def is_peak_hours() -> bool:
    """
    Check if current time is within peak liquidity window.
    Peak: 12pm–8pm EST (17:00–01:00 UTC).
    Also returns True during the 6am EST model-update window (±15 min).
    """
    now = datetime.now(timezone.utc)
    utc_hour = now.hour
    utc_minute = now.minute

    # 6am EST = 11:00 UTC — dedicated scan window (±15 min)
    if utc_hour == 10 and utc_minute >= 45:
        return True
    if utc_hour == 11 and utc_minute <= 15:
        return True

    # 12pm EST = 17:00 UTC, 8pm EST = 01:00 UTC (next day)
    # So peak is 17:00–23:59 UTC and 00:00–01:00 UTC
    if utc_hour >= 17 or utc_hour < 1:
        return True

    return False


def is_model_update_window() -> bool:
    """Check if we're in the 6am EST (11:00 UTC) model update window."""
    now = datetime.now(timezone.utc)
    if now.hour == 10 and now.minute >= 45:
        return True
    if now.hour == 11 and now.minute <= 15:
        return True
    return False


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
    Fetch active weather markets from Kalshi by querying known weather series
    tickers directly. This is more reliable than keyword filtering since Kalshi
    categorizes weather markets under specific series (e.g. KXHIGHTPHX, KXHIGHNY).
    Returns raw market dicts from the API.
    """
    url = f"{_base_url()}/markets"
    all_markets = []

    _log.info("Fetching weather markets from Kalshi via series tickers...")

    # Fetch markets from each known weather series
    for series_ticker in WEATHER_SERIES:
        if len(all_markets) >= limit:
            break
        try:
            h = _auth_headers("GET", "/trade-api/v2/markets")
            params = {
                "status": "open",
                "series_ticker": series_ticker,
                "limit": 20,
            }
            resp = requests.get(url, headers=h, params=params, timeout=10)
            if resp.status_code == 200:
                markets = resp.json().get("markets", [])
                if markets:
                    all_markets.extend(markets)
                    _log.debug(f"  {series_ticker}: {len(markets)} markets")
            time.sleep(0.05)  # gentle rate limit
        except Exception as e:
            _log.debug(f"  {series_ticker}: error — {e}")
            continue

    # Deduplicate by ticker
    seen = set()
    deduped = []
    for m in all_markets:
        t = m.get("ticker", "")
        if t not in seen:
            seen.add(t)
            deduped.append(m)

    _log.info(f"Weather markets fetched: {len(deduped)} from {len(WEATHER_SERIES)} series")
    return deduped


# ── Time-to-resolution filter ─────────────────────────────────────────────────

def filter_by_time_window(markets: list[dict]) -> list[dict]:
    """
    Keep only markets resolving between MIN_HOURS and MAX_HOURS_HIGH_CONVICTION from now.
    Markets in the 72–96h range are tagged _extended_window=True so the caller
    can filter them out unless edge ratio >= 5x.
    """
    now = datetime.now(timezone.utc)
    valid = []

    for m in markets:
        ticker = m.get("ticker", "")
        question = m.get("title", "")

        raw = m.get("expected_expiration_time") or m.get("close_time") or ""
        if not raw:
            _log_skipped(ticker, question, "no_resolution_time")
            continue

        try:
            res_dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            hours = (res_dt - now).total_seconds() / 3600
        except Exception:
            _log_skipped(ticker, question, "unparseable_resolution_time")
            continue

        if hours < MIN_HOURS or hours > MAX_HOURS_HIGH_CONVICTION:
            _log_skipped(ticker, question,
                         f"time_window: {hours:.1f}h outside {MIN_HOURS}-{MAX_HOURS_HIGH_CONVICTION}h")
            continue

        m["_hours_to_resolution"] = round(hours, 2)
        m["_resolution_dt"] = res_dt.isoformat()
        m["_extended_window"] = hours > MAX_HOURS
        valid.append(m)

    extended = sum(1 for m in valid if m.get("_extended_window"))
    _log.info(f"Time filter: {len(valid)}/{len(markets)} markets in window "
              f"({extended} in extended 72-96h range)")
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

def scan_markets(bankroll: float = 50.0, skip_peak_check: bool = False) -> list[dict]:
    """
    Full scanner pipeline:
    1. Peak hours check (skip scan if off-peak)
    2. Fetch weather markets from Kalshi
    3. Time-to-resolution filter (2-96h, extended window tagged)
    4. Minimum contract size filter
    5. Price extraction

    Returns list of market dicts ready for market_mapper.py
    """
    if not skip_peak_check and not is_peak_hours():
        _log.info("Off-peak hours, skipping scan.")
        return []

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
