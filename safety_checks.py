"""
safety_checks.py — Extreme Weather & Market Safety Guards for Project AUGUR

Three safety layers added after Denver/Miami losses (2026-03-24):
1. Heat wave detection — flags anomalous temps, reduces position size
2. NWS cross-check — compares ensemble vs official NWS forecast, skips on divergence
3. Market momentum circuit breaker — flags positions where price moves against us

All checks are passive/read-only. No orders placed here.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

_log = logging.getLogger(__name__)

SAFETY_LOG = "safety.log"

# ── Climate normals (approximate 30-year March daily highs, °F) ───────────────
# Source: NOAA 1991-2020 normals, rounded
MARCH_NORMALS: dict[str, float] = {
    "Phoenix":       79.0,
    "New York":      52.0,
    "NYC":           52.0,
    "Chicago":       47.0,
    "Miami":         81.0,
    "Dallas":        67.0,
    "Seattle":       53.0,
    "Denver":        55.0,
    "Atlanta":       64.0,
    "Los Angeles":   68.0,
    "LA":            68.0,
    "Austin":        72.0,
    "San Antonio":   74.0,
    "Minneapolis":   43.0,
    "Oklahoma City": 62.0,
    "Houston":       73.0,
    "San Francisco": 62.0,
    "Philadelphia":  53.0,
}

# Extend with April/May normals as needed — for now March covers initial deployment
APRIL_NORMALS: dict[str, float] = {
    "Phoenix":       88.0,
    "New York":      62.0,
    "NYC":           62.0,
    "Chicago":       58.0,
    "Miami":         84.0,
    "Dallas":        76.0,
    "Seattle":       58.0,
    "Denver":        62.0,
    "Atlanta":       72.0,
    "Los Angeles":   70.0,
    "LA":            70.0,
    "Austin":        79.0,
    "San Antonio":   80.0,
    "Minneapolis":   56.0,
    "Oklahoma City": 72.0,
    "Houston":       79.0,
    "San Francisco": 64.0,
    "Philadelphia":  63.0,
}

ANOMALY_THRESHOLD_F      = 15.0   # flag if any ensemble member is 15°F+ above normal
ANOMALY_SIZE_REDUCTION   = 0.25   # reduce to 25% of normal Kelly
NWS_DIVERGENCE_THRESHOLD = 5.0    # skip market if NWS differs by 5°F+
MOMENTUM_THRESHOLD       = 0.15   # flag if price moved 15%+ against us


def _log_safety(event_type: str, details: dict) -> None:
    """Log safety event to safety.log."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": event_type,
        **details,
    }
    with open(SAFETY_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _get_normals(city: str, month: int) -> Optional[float]:
    """Get climate normal for a city and month."""
    if month == 3:
        return MARCH_NORMALS.get(city)
    elif month == 4:
        return APRIL_NORMALS.get(city)
    # For other months, return None (no data yet)
    return None


# ── 1. Heat Wave Detection ────────────────────────────────────────────────────

def check_heat_wave(
    city: str,
    ensemble_members: list[float],
    resolution_date: str,
) -> dict:
    """
    Check if ensemble members indicate anomalous heat.

    If ANY ensemble member forecasts temps 15°F+ above the 30-year normal,
    flag as high-uncertainty.

    Returns:
        {
            "flagged": bool,
            "max_member": float,
            "normal": float,
            "anomaly": float,
            "size_multiplier": float,  # 1.0 = normal, 0.25 = reduced
        }
    """
    try:
        month = int(resolution_date[5:7])
    except (ValueError, IndexError):
        month = 3

    normal = _get_normals(city, month)
    if normal is None:
        return {"flagged": False, "size_multiplier": 1.0, "reason": "no_normal_data"}

    if not ensemble_members:
        return {"flagged": False, "size_multiplier": 1.0, "reason": "no_ensemble_data"}

    max_member = max(ensemble_members)
    anomaly = max_member - normal

    if anomaly >= ANOMALY_THRESHOLD_F:
        details = {
            "city": city,
            "date": resolution_date,
            "max_ensemble_member": round(max_member, 1),
            "climate_normal": normal,
            "anomaly_f": round(anomaly, 1),
            "action": f"reducing position size to {ANOMALY_SIZE_REDUCTION:.0%}",
        }
        _log_safety("heat_wave_detected", details)
        _log.warning(
            f"⚠️ HEAT WAVE FLAG: {city} {resolution_date} — "
            f"ensemble member at {max_member:.0f}°F, normal={normal:.0f}°F, "
            f"anomaly={anomaly:.0f}°F → size reduced to {ANOMALY_SIZE_REDUCTION:.0%}"
        )
        return {
            "flagged": True,
            "max_member": max_member,
            "normal": normal,
            "anomaly": anomaly,
            "size_multiplier": ANOMALY_SIZE_REDUCTION,
        }

    return {
        "flagged": False,
        "max_member": max_member,
        "normal": normal,
        "anomaly": anomaly,
        "size_multiplier": 1.0,
    }


# ── 2. NWS Cross-Check ───────────────────────────────────────────────────────

def fetch_nws_forecast(lat: float, lon: float) -> Optional[dict]:
    """
    Fetch the official NWS point forecast.
    NWS API: /points/{lat},{lon} → /gridpoints/{office}/{x},{y}/forecast

    Returns dict mapping date_str → forecasted high temp (°F), or None on error.
    """
    try:
        # Step 1: Get grid point
        points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
        headers = {"User-Agent": "ProjectAUGUR/1.0 (weather-betting-bot)"}
        resp = requests.get(points_url, headers=headers, timeout=10)
        resp.raise_for_status()
        props = resp.json().get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            return None

        time.sleep(0.2)  # be gentle with NWS API

        # Step 2: Get forecast
        resp2 = requests.get(forecast_url, headers=headers, timeout=10)
        resp2.raise_for_status()
        periods = resp2.json().get("properties", {}).get("periods", [])

        result = {}
        for p in periods:
            if not p.get("isDaytime", True):
                continue
            # startTime: "2026-03-30T06:00:00-07:00"
            date_str = p.get("startTime", "")[:10]
            temp = p.get("temperature")
            unit = p.get("temperatureUnit", "F")
            if temp is not None and date_str:
                if unit == "C":
                    temp = temp * 9.0 / 5.0 + 32.0
                result[date_str] = round(float(temp), 1)

        return result if result else None

    except Exception as e:
        _log.debug(f"NWS forecast fetch failed: {e}")
        return None


def check_nws_divergence(
    city: str,
    lat: float,
    lon: float,
    ensemble_mean: float,
    resolution_date: str,
) -> dict:
    """
    Compare Open-Meteo ensemble mean against NWS official forecast.
    If they diverge by more than 5°F, flag the market as unsafe.

    Returns:
        {
            "skip": bool,
            "nws_high": float or None,
            "ensemble_mean": float,
            "divergence": float or None,
        }
    """
    nws_forecasts = fetch_nws_forecast(lat, lon)
    if nws_forecasts is None:
        _log.debug(f"NWS forecast unavailable for {city} — no cross-check")
        return {"skip": False, "nws_high": None, "ensemble_mean": ensemble_mean, "divergence": None}

    nws_high = nws_forecasts.get(resolution_date)
    if nws_high is None:
        _log.debug(f"NWS has no forecast for {city} on {resolution_date}")
        return {"skip": False, "nws_high": None, "ensemble_mean": ensemble_mean, "divergence": None}

    divergence = abs(ensemble_mean - nws_high)

    if divergence >= NWS_DIVERGENCE_THRESHOLD:
        details = {
            "city": city,
            "date": resolution_date,
            "ensemble_mean": round(ensemble_mean, 1),
            "nws_high": nws_high,
            "divergence_f": round(divergence, 1),
            "action": "SKIP — NWS divergence exceeds threshold",
        }
        _log_safety("nws_divergence", details)
        _log.warning(
            f"⚠️ NWS DIVERGENCE: {city} {resolution_date} — "
            f"ensemble={ensemble_mean:.1f}°F vs NWS={nws_high:.1f}°F "
            f"(diff={divergence:.1f}°F) → SKIPPING MARKET"
        )
        return {
            "skip": True,
            "nws_high": nws_high,
            "ensemble_mean": round(ensemble_mean, 1),
            "divergence": round(divergence, 1),
        }

    _log.info(f"NWS cross-check OK: {city} {resolution_date} — "
              f"ensemble={ensemble_mean:.1f}°F, NWS={nws_high:.1f}°F, "
              f"diff={divergence:.1f}°F")

    return {
        "skip": False,
        "nws_high": nws_high,
        "ensemble_mean": round(ensemble_mean, 1),
        "divergence": round(divergence, 1),
    }


# ── 3. Market Momentum Circuit Breaker ────────────────────────────────────────

def check_market_momentum(
    ticker: str,
    entry_price: float,
    side: str = "YES",
) -> dict:
    """
    Check if the Kalshi market price has moved significantly against our position.
    If the current price has dropped 15%+ from entry (for YES) or risen 15%+ (for NO),
    flag for review.

    Returns:
        {
            "flagged": bool,
            "entry_price": float,
            "current_price": float,
            "move_pct": float,
        }
    """
    from kalshi_scanner import _auth_headers, _base_url

    try:
        url = f"{_base_url()}/markets/{ticker}"
        resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=8)
        resp.raise_for_status()
        market = resp.json().get("market", resp.json())

        yes_ask = float(market.get("yes_ask_dollars") or 0)
        yes_bid = float(market.get("yes_bid_dollars") or 0)
        if yes_ask > 0 and yes_bid > 0:
            current_price = (yes_ask + yes_bid) / 2
        elif yes_ask > 0:
            current_price = yes_ask
        else:
            return {"flagged": False, "reason": "no_current_price"}

    except Exception as e:
        _log.debug(f"Momentum check failed for {ticker}: {e}")
        return {"flagged": False, "reason": f"api_error: {e}"}

    if side.upper() == "YES":
        # Bad if price dropped (market moving toward NO)
        if entry_price > 0:
            move_pct = (entry_price - current_price) / entry_price
        else:
            move_pct = 0
    else:
        # Bad if price rose (market moving toward YES, against our NO bet)
        if entry_price < 1:
            move_pct = (current_price - entry_price) / (1 - entry_price)
        else:
            move_pct = 0

    if move_pct >= MOMENTUM_THRESHOLD:
        details = {
            "ticker": ticker,
            "side": side,
            "entry_price": round(entry_price, 4),
            "current_price": round(current_price, 4),
            "adverse_move_pct": round(move_pct * 100, 1),
            "action": "FLAG — market moving against position",
        }
        _log_safety("momentum_adverse", details)
        _log.warning(
            f"⚠️ MOMENTUM FLAG: {ticker} [{side}] — "
            f"entry={entry_price:.4f} → current={current_price:.4f} "
            f"({move_pct:.1%} adverse) → FLAGGED FOR REVIEW"
        )
        return {
            "flagged": True,
            "entry_price": round(entry_price, 4),
            "current_price": round(current_price, 4),
            "move_pct": round(move_pct, 4),
        }

    return {
        "flagged": False,
        "entry_price": round(entry_price, 4),
        "current_price": round(current_price, 4),
        "move_pct": round(move_pct, 4),
    }
