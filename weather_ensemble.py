"""
Fetch ensemble temperature forecasts from Open-Meteo.
Uses the ICON Seamless ensemble model (25 members) to get a distribution
of possible high temperatures for each city + date.
"""

import json
import math
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    raise SystemExit("requests is required: pip install requests")

CACHE_DIR = Path(__file__).parent / ".weather_cache"
CACHE_TTL = 3600

CITIES = {
    "Phoenix":        {"lat": 33.4484, "lon": -112.0740},
    "New York":       {"lat": 40.7128, "lon": -74.0060},
    "NYC":            {"lat": 40.7128, "lon": -74.0060},
    "Chicago":        {"lat": 41.8781, "lon": -87.6298},
    "Miami":          {"lat": 25.7617, "lon": -80.1918},
    "Dallas":         {"lat": 32.7767, "lon": -96.7970},
    "Seattle":        {"lat": 47.6062, "lon": -122.3321},
    "Denver":         {"lat": 39.7392, "lon": -104.9903},
    "Atlanta":        {"lat": 33.7490, "lon": -84.3880},
    "Los Angeles":    {"lat": 34.0522, "lon": -118.2437},
    "LA":             {"lat": 34.0522, "lon": -118.2437},
    "Austin":         {"lat": 30.2672, "lon": -97.7431},
    "San Antonio":    {"lat": 29.4241, "lon": -98.4936},
    "Minneapolis":    {"lat": 44.9778, "lon": -93.2650},
    "Oklahoma City":  {"lat": 35.4676, "lon": -97.5164},
    "Houston":        {"lat": 29.7604, "lon": -95.3698},
    "San Francisco":  {"lat": 37.7749, "lon": -122.4194},
    "Philadelphia":   {"lat": 39.9526, "lon": -75.1652},
}

N_MEMBERS = 25  # ICON seamless ensemble size


def _cache_path(city: str) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    safe = city.replace(" ", "_").lower()
    return CACHE_DIR / f"ensemble_{safe}.json"


def _load_cache(city: str) -> dict | None:
    path = _cache_path(city)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    if time.time() - data.get("_ts", 0) > CACHE_TTL:
        return None
    return data


def _save_cache(city: str, data: dict):
    data["_ts"] = time.time()
    _cache_path(city).write_text(json.dumps(data))


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def fetch_ensemble(city: str, forecast_days: int = 7) -> dict:
    """Fetch ensemble forecast for a city.
    Returns dict mapping date_str -> list of 25 daily-max temperatures (°F).
    """
    cached = _load_cache(city)
    if cached:
        cached.pop("_ts", None)
        return cached

    coords = CITIES[city]
    # Build member list: temperature_2m_max_member01 .. temperature_2m_max_member25
    # Open-Meteo ensemble API uses hourly data; we fetch temperature_2m for all members
    # and compute daily max ourselves.
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m",
        "models": "icon_seamless",
        "forecast_days": min(forecast_days, 7),
        "timezone": "America/New_York",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])

    # Collect all member columns
    member_keys = []
    for key in hourly:
        if key.startswith("temperature_2m_member"):
            member_keys.append(key)
    member_keys.sort()

    if not member_keys:
        # Fallback: if API returns temperature_2m as a single array, synthesize members
        base = hourly.get("temperature_2m", [])
        member_keys = ["temperature_2m"]
        hourly["temperature_2m"] = base

    # Group hourly data by date, find daily max per member
    # times are like "2026-03-18T00:00"
    date_members: dict[str, list[list[float]]] = {}  # date -> [member_hourly_vals, ...]

    for i, t in enumerate(times):
        date_str = t[:10]
        if date_str not in date_members:
            date_members[date_str] = [[] for _ in range(len(member_keys))]
        for m_idx, mk in enumerate(member_keys):
            val = hourly[mk][i] if i < len(hourly[mk]) else None
            if val is not None:
                date_members[date_str][m_idx].append(val)

    # Compute daily max per member, convert to °F
    result = {"city": city, "dates": {}}
    for date_str in sorted(date_members.keys()):
        member_maxes = []
        for m_vals in date_members[date_str]:
            if m_vals:
                member_maxes.append(round(_c_to_f(max(m_vals)), 1))
        if member_maxes:
            result["dates"][date_str] = member_maxes

    _save_cache(city, result)
    return result


def ensemble_stats(member_maxes: list[float]) -> dict:
    """Compute distribution stats from ensemble member daily maxes."""
    n = len(member_maxes)
    if n == 0:
        return {"mean": 0, "std": 0, "min": 0, "max": 0, "n": 0}
    mean = sum(member_maxes) / n
    variance = sum((x - mean) ** 2 for x in member_maxes) / n
    std = math.sqrt(variance)
    return {
        "mean": round(mean, 1),
        "std": round(std, 1),
        "min": round(min(member_maxes), 1),
        "max": round(max(member_maxes), 1),
        "n": n,
    }


def bracket_probability(member_maxes: list[float], low: float | None, high: float | None) -> float:
    """Calculate probability of temperature falling in a bracket [low, high).
    low=None means -inf, high=None means +inf.
    """
    n = len(member_maxes)
    if n == 0:
        return 0.0
    count = 0
    for t in member_maxes:
        if (low is None or t >= low) and (high is None or t < high):
            count += 1
    return count / n


def fetch_all_ensembles(forecast_days: int = 7) -> dict[str, dict]:
    """Fetch ensemble forecasts for all cities."""
    results = {}
    for city in CITIES:
        try:
            results[city] = fetch_ensemble(city, forecast_days)
        except Exception as e:
            print(f"  Warning: failed to fetch ensemble for {city}: {e}")
    return results


if __name__ == "__main__":
    print("Fetching ensemble forecasts...")
    all_data = fetch_all_ensembles()
    for city, data in all_data.items():
        print(f"\n{city}:")
        for date_str, members in list(data.get("dates", {}).items())[:3]:
            stats = ensemble_stats(members)
            print(f"  {date_str}: mean={stats['mean']:.1f}°F  std={stats['std']:.1f}°F  "
                  f"range=[{stats['min']:.0f}-{stats['max']:.0f}]  members={stats['n']}")
