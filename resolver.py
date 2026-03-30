"""
resolver.py — Auto Outcome Detection & Resolution for Project AUGUR

Runs every hour via cron. Loops through signals.log, checks resolved markets
on Kalshi API, writes outcomes back, and updates calibration tracking.

Usage:
    python3 resolver.py           # resolve all pending trades
    python3 resolver.py --dry     # check but don't write

Crontab:
    0 * * * * cd /path/to/augur && python3 resolver.py >> resolver.log 2>&1
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from calibration import log_resolution, check_market_resolution

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("resolver.log"),
    ]
)
_log = logging.getLogger(__name__)

SIGNALS_LOG = "signals.log"


def load_signals() -> list[dict]:
    """Load all entries from signals.log."""
    if not os.path.exists(SIGNALS_LOG):
        return []
    records = []
    with open(SIGNALS_LOG) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def save_signals(records: list[dict]) -> None:
    """Rewrite signals.log with updated records."""
    with open(SIGNALS_LOG, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def fetch_actual_temperature(city: str, date_str: str) -> Optional[float]:
    """
    Fetch actual observed high temperature from NWS API for a resolved date.
    Uses the observations endpoint for the nearest station.
    """
    import requests
    from weather_ensemble import CITIES

    coords = CITIES.get(city)
    if not coords:
        return None

    try:
        # Get nearest observation station
        headers = {"User-Agent": "ProjectAUGUR/1.0"}
        points_url = f"https://api.weather.gov/points/{coords['lat']:.4f},{coords['lon']:.4f}"
        resp = requests.get(points_url, headers=headers, timeout=10)
        resp.raise_for_status()
        stations_url = resp.json()["properties"]["observationStations"]

        time.sleep(0.2)
        resp2 = requests.get(stations_url, headers=headers, timeout=10)
        resp2.raise_for_status()
        stations = resp2.json().get("features", [])
        if not stations:
            return None
        station_id = stations[0]["properties"]["stationIdentifier"]

        # Get observations for the date
        time.sleep(0.2)
        obs_url = (f"https://api.weather.gov/stations/{station_id}/observations"
                   f"?start={date_str}T00:00:00Z&end={date_str}T23:59:59Z")
        resp3 = requests.get(obs_url, headers=headers, timeout=10)
        resp3.raise_for_status()
        observations = resp3.json().get("features", [])

        # Find max temperature across all observations that day
        max_temp_c = None
        for obs in observations:
            props = obs.get("properties", {})
            temp = props.get("maxTemperatureLast24Hours", {})
            if temp and temp.get("value") is not None:
                val = float(temp["value"])
                if max_temp_c is None or val > max_temp_c:
                    max_temp_c = val
            # Also check regular temperature readings
            temp2 = props.get("temperature", {})
            if temp2 and temp2.get("value") is not None:
                val = float(temp2["value"])
                if max_temp_c is None or val > max_temp_c:
                    max_temp_c = val

        if max_temp_c is not None:
            return round(max_temp_c * 9 / 5 + 32, 1)  # Convert C to F
        return None

    except Exception as e:
        _log.debug(f"NWS observation fetch failed for {city} {date_str}: {e}")
        return None


def resolve_trade(record: dict) -> Optional[dict]:
    """
    Check if a trade's market has resolved and determine outcome.

    Returns updated record with outcome fields, or None if not yet resolved.
    """
    ticker = record.get("ticker", "")
    if not ticker:
        return None

    result = check_market_resolution(ticker)
    if result is None:
        return None  # Not yet resolved

    side = record.get("side", "YES").upper()

    # Determine win/loss based on side
    if side == "YES":
        won = result is True   # YES resolved YES = win
    else:
        won = result is False  # NO side: market resolved NO = win

    outcome = "won" if won else "lost"
    bet_size = record.get("bet_size_usd", record.get("intended_bet", 0))
    payout_mult = record.get("payout_multiplier", 2.0)

    if won:
        pnl = bet_size * payout_mult - bet_size
    else:
        pnl = -bet_size

    # Fetch actual temperature
    city = record.get("location", "")
    date = record.get("date", (record.get("resolution_dt") or "")[:10])
    actual_temp = fetch_actual_temperature(city, date)

    # Ensemble prediction
    ensemble_predicted = None
    true_prob = record.get("true_prob")
    if true_prob is not None:
        ensemble_predicted = round(true_prob, 4)

    # Kalshi threshold from question parsing
    kalshi_threshold = record.get("question", "")

    # Error delta
    error_delta = None
    if actual_temp is not None and ensemble_predicted is not None:
        # This is true_prob error, not temp error
        error_delta = round(abs(ensemble_predicted - (1.0 if won else 0.0)), 4)

    # Update record
    record["outcome"] = outcome
    record["pnl"] = round(pnl, 2)
    record["actual_temperature"] = actual_temp
    record["ensemble_predicted"] = ensemble_predicted
    record["kalshi_threshold"] = kalshi_threshold
    record["error_delta"] = error_delta
    record["resolved_at"] = datetime.now(timezone.utc).isoformat()

    return record


def run_resolver(dry_run: bool = False):
    """Main resolver loop — check all unresolved trades."""
    signals = load_signals()
    if not signals:
        _log.info("No signals to resolve.")
        return

    resolved_count = 0
    total_pnl = 0.0

    for i, record in enumerate(signals):
        # Skip dry runs, already resolved, or records without tickers
        if record.get("dry_run", False):
            continue
        if record.get("outcome"):
            continue
        if not record.get("ticker"):
            continue

        # Check resolution time — don't bother checking if market hasn't closed
        res_dt = record.get("resolution_dt", "")
        if res_dt:
            try:
                res_time = datetime.fromisoformat(res_dt.replace("Z", "+00:00"))
                if res_time > datetime.now(timezone.utc):
                    continue  # Not yet past resolution time
            except Exception:
                pass

        updated = resolve_trade(record)
        if updated:
            resolved_count += 1
            pnl = updated.get("pnl", 0)
            total_pnl += pnl

            _log.info(
                f"Resolved: {updated['ticker']} | {updated['outcome'].upper()} | "
                f"PnL=${pnl:+.2f} | actual_temp={updated.get('actual_temperature', '?')}°F"
            )

            if not dry_run:
                signals[i] = updated

                # Also log to calibration.py
                log_resolution(
                    market_id=updated["ticker"],
                    question=updated.get("question", ""),
                    predicted_prob=updated.get("true_prob", 0),
                    actual_outcome=updated["outcome"] == "won",
                    market_price=updated.get("yes_price", 0),
                    payout_multiplier=updated.get("payout_multiplier", 1),
                    bet_size=updated.get("bet_size_usd", updated.get("intended_bet", 0)),
                    pnl=pnl,
                    city=updated.get("location", ""),
                    date=updated.get("date", ""),
                )

        time.sleep(0.2)  # Rate limit Kalshi API

    if not dry_run and resolved_count > 0:
        save_signals(signals)

        # Generate post-mortems for newly resolved trades
        try:
            from postmortem import analyze_all_resolved
            analyze_all_resolved()
        except Exception as e:
            _log.warning(f"Post-mortem generation failed: {e}")

    _log.info(f"Resolver complete: {resolved_count} trades resolved, "
              f"PnL=${total_pnl:+.2f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AUGUR Trade Resolver")
    parser.add_argument("--dry", action="store_true", help="Check only, don't write")
    args = parser.parse_args()
    run_resolver(dry_run=args.dry)
