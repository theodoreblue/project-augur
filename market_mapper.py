"""
market_mapper.py — Market-to-Model Alignment Layer for Project AUGUR

Purpose:
    Parses Kalshi weather market questions and extracts the four fields
    required by weather_ensemble.py:
        1. location   — city name + lat/lon coordinates
        2. metric     — temp / rain / snow / wind
        3. threshold  — numeric value and comparison type
        4. resolution_dt — exact UTC datetime

    Markets that fail to parse any field are logged to unmatched.log and dropped.
    Markets that pass are enriched with structured fields for the ensemble model.

Kalshi API endpoints used:
    None — this module is a pure parser. Input comes from kalshi_scanner.py.

Regulatory constraints:
    - No API calls made here. Pure data transformation.
    - All parsing is deterministic and auditable.
"""

import json
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

_log = logging.getLogger(__name__)
UNMATCHED_LOG = "unmatched.log"

# ── City → lat/lon lookup ─────────────────────────────────────────────────────
# Kalshi weather contracts commonly use these US cities.
# Coordinates feed directly into Open-Meteo API calls.

CITY_COORDS: dict[str, dict] = {
    "New York":      {"lat": 40.7128,  "lon": -74.0060},
    "New York City": {"lat": 40.7128,  "lon": -74.0060},
    "NYC":           {"lat": 40.7128,  "lon": -74.0060},
    "Los Angeles":   {"lat": 34.0522,  "lon": -118.2437},
    "Chicago":       {"lat": 41.8781,  "lon": -87.6298},
    "Houston":       {"lat": 29.7604,  "lon": -95.3698},
    "Phoenix":       {"lat": 33.4484,  "lon": -112.0740},
    "Philadelphia":  {"lat": 39.9526,  "lon": -75.1652},
    "San Antonio":   {"lat": 29.4241,  "lon": -98.4936},
    "San Diego":     {"lat": 32.7157,  "lon": -117.1611},
    "Dallas":        {"lat": 32.7767,  "lon": -96.7970},
    "San Jose":      {"lat": 37.3382,  "lon": -121.8863},
    "Austin":        {"lat": 30.2672,  "lon": -97.7431},
    "Jacksonville":  {"lat": 30.3322,  "lon": -81.6557},
    "Fort Worth":    {"lat": 32.7555,  "lon": -97.3308},
    "Columbus":      {"lat": 39.9612,  "lon": -82.9988},
    "Charlotte":     {"lat": 35.2271,  "lon": -80.8431},
    "Indianapolis":  {"lat": 39.7684,  "lon": -86.1581},
    "Seattle":       {"lat": 47.6062,  "lon": -122.3321},
    "Denver":        {"lat": 39.7392,  "lon": -104.9903},
    "Nashville":     {"lat": 36.1627,  "lon": -86.7816},
    "Miami":         {"lat": 25.7617,  "lon": -80.1918},
    "Atlanta":       {"lat": 33.7490,  "lon": -84.3880},
    "Boston":        {"lat": 42.3601,  "lon": -71.0589},
    "Las Vegas":     {"lat": 36.1699,  "lon": -115.1398},
    "Portland":      {"lat": 45.5051,  "lon": -122.6750},
    "Memphis":       {"lat": 35.1495,  "lon": -90.0490},
    "Baltimore":     {"lat": 39.2904,  "lon": -76.6122},
    "Milwaukee":     {"lat": 43.0389,  "lon": -87.9065},
    "Albuquerque":   {"lat": 35.0844,  "lon": -106.6504},
    "Tucson":        {"lat": 32.2226,  "lon": -110.9747},
    "Fresno":        {"lat": 36.7378,  "lon": -119.7871},
    "Sacramento":    {"lat": 38.5816,  "lon": -121.4944},
    "Mesa":          {"lat": 33.4152,  "lon": -111.8315},
    "Kansas City":   {"lat": 39.0997,  "lon": -94.5786},
    "Omaha":         {"lat": 41.2565,  "lon": -95.9345},
    "Raleigh":       {"lat": 35.7796,  "lon": -78.6382},
    "Cleveland":     {"lat": 41.4993,  "lon": -81.6944},
    "Minneapolis":   {"lat": 44.9778,  "lon": -93.2650},
    "New Orleans":   {"lat": 29.9511,  "lon": -90.0715},
    "Tampa":         {"lat": 27.9506,  "lon": -82.4572},
    "Pittsburgh":    {"lat": 40.4406,  "lon": -79.9959},
    "Cincinnati":    {"lat": 39.1031,  "lon": -84.5120},
    "St. Louis":     {"lat": 38.6270,  "lon": -90.1994},
    "Reno":          {"lat": 39.5296,  "lon": -119.8138},
    "Baton Rouge":   {"lat": 30.4515,  "lon": -91.1871},
    "Durham":        {"lat": 35.9940,  "lon": -78.8986},
    "Madison":       {"lat": 43.0731,  "lon": -89.4012},
    "Lubbock":       {"lat": 33.5779,  "lon": -101.8552},
    "Scottsdale":    {"lat": 33.4942,  "lon": -111.9261},
    "Louisville":    {"lat": 38.2527,  "lon": -85.7585},
    "Richmond":      {"lat": 37.5407,  "lon": -77.4360},
    "Salt Lake City":{"lat": 40.7608,  "lon": -111.8910},
    "Anchorage":     {"lat": 61.2181,  "lon": -149.9003},
    "Honolulu":      {"lat": 21.3069,  "lon": -157.8583},
}

# Build a lowercase → canonical lookup
_CITY_LOWER: dict[str, str] = {k.lower(): k for k in CITY_COORDS}

# ── Regex patterns ────────────────────────────────────────────────────────────

# Temperature: "be 78°F or higher", "be between 78-80°F", "be 78°F on", "exceed 90°F"
_RE_TEMP_BETWEEN = re.compile(
    r"between\s+(\d+(?:\.\d+)?)\s*[–\-]\s*(\d+(?:\.\d+)?)\s*[°]?\s*([fc])",
    re.IGNORECASE,
)
_RE_TEMP_UPPER = re.compile(
    r"(\d+(?:\.\d+)?)\s*[°]?\s*([fc])\s+or\s+(?:higher|above|more|exceed)|"
    r"(?:exceed|above|reach)\s+(\d+(?:\.\d+)?)\s*[°]?\s*([fc])",
    re.IGNORECASE,
)
_RE_TEMP_LOWER = re.compile(
    r"(\d+(?:\.\d+)?)\s*[°]?\s*([fc])\s+or\s+(?:lower|below|less|under)|"
    r"(?:below|under)\s+(\d+(?:\.\d+)?)\s*[°]?\s*([fc])",
    re.IGNORECASE,
)
_RE_TEMP_EXACT = re.compile(
    r"(?:be|reach|hit)\s+(\d+(?:\.\d+)?)\s*[°]?\s*([fc])(?:\s+on|\s+by|\s*\?|$)",
    re.IGNORECASE,
)
_RE_TEMP_HIGH = re.compile(
    r"high\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*[°]?\s*([fc])",
    re.IGNORECASE,
)

# Rain/precipitation: "more than 0.5 inches", "at least 1 inch", "exceed 0.25 inches"
_RE_RAIN = re.compile(
    r"(?:rain|precipitation|rainfall)\s+.*?"
    r"(?:more than|at least|exceed|over|greater than)\s+"
    r"(\d+(?:\.\d+)?)\s+inch",
    re.IGNORECASE,
)
_RE_RAIN_UNDER = re.compile(
    r"(?:rain|precipitation|rainfall)\s+.*?"
    r"(?:less than|under|below|no more than)\s+"
    r"(\d+(?:\.\d+)?)\s+inch",
    re.IGNORECASE,
)
_RE_RAIN_ANY = re.compile(r"(?:will it rain|any rain|rain at all)", re.IGNORECASE)

# Snow: "more than X inches of snow"
_RE_SNOW = re.compile(
    r"(?:snow|snowfall)\s+.*?"
    r"(?:more than|at least|exceed|over)\s+(\d+(?:\.\d+)?)\s+inch",
    re.IGNORECASE,
)

# Date patterns
_RE_DATE_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})")
_RE_DATE_HUMAN = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})(?:,?\s*(\d{4}))?",
    re.IGNORECASE,
)
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# City extraction — look for known city names in the question
def _find_city(text: str) -> Optional[str]:
    lower = text.lower()
    # Try longest match first to avoid "San" matching before "San Antonio"
    for key in sorted(_CITY_LOWER.keys(), key=len, reverse=True):
        if key in lower:
            return _CITY_LOWER[key]
    return None


def _parse_date(text: str) -> Optional[datetime]:
    iso = _RE_DATE_ISO.search(text)
    if iso:
        return datetime.strptime(iso.group(1), "%Y-%m-%d").replace(
            hour=23, minute=59, tzinfo=timezone.utc
        )
    human = _RE_DATE_HUMAN.search(text)
    if human:
        m = _MONTHS.get(human.group(1).lower()[:3])
        d = int(human.group(2))
        y = int(human.group(3)) if human.group(3) else datetime.now(timezone.utc).year
        if m:
            return datetime(y, m, d, 23, 59, tzinfo=timezone.utc)
    return None


def _to_f(val: float, unit: str) -> float:
    return round(val * 9 / 5 + 32, 1) if unit.lower() == "c" else float(val)


def _parse_threshold(text: str) -> Optional[dict]:
    """Extract metric + threshold from question text."""

    # Rain: any rain (binary yes/no)
    if _RE_RAIN_ANY.search(text):
        return {"metric": "rain", "type": "any", "low": None, "high": None}

    # Rain: > X inches
    m = _RE_RAIN.search(text)
    if m:
        return {"metric": "rain", "type": "upper", "low": float(m.group(1)), "high": None}

    # Rain: < X inches
    m = _RE_RAIN_UNDER.search(text)
    if m:
        return {"metric": "rain", "type": "lower", "low": None, "high": float(m.group(1))}

    # Snow: > X inches
    m = _RE_SNOW.search(text)
    if m:
        return {"metric": "snow", "type": "upper", "low": float(m.group(1)), "high": None}

    # Temp: between X-Y
    m = _RE_TEMP_BETWEEN.search(text)
    if m:
        lo = _to_f(float(m.group(1)), m.group(3))
        hi = _to_f(float(m.group(2)), m.group(3))
        return {"metric": "temp", "type": "bracket", "low": lo, "high": hi}

    # Temp: X or higher
    m = _RE_TEMP_UPPER.search(text)
    if m:
        val = float(m.group(1) or m.group(3))
        unit = m.group(2) or m.group(4) or "f"
        return {"metric": "temp", "type": "upper", "low": _to_f(val, unit), "high": None}

    # Temp: X or lower
    m = _RE_TEMP_LOWER.search(text)
    if m:
        val = float(m.group(1) or m.group(3))
        unit = m.group(2) or m.group(4) or "f"
        return {"metric": "temp", "type": "lower", "low": None, "high": _to_f(val, unit)}

    # Temp: high of X
    m = _RE_TEMP_HIGH.search(text)
    if m:
        val = _to_f(float(m.group(1)), m.group(2))
        return {"metric": "temp", "type": "bracket", "low": val - 0.5, "high": val + 0.5}

    # Temp: exact value
    m = _RE_TEMP_EXACT.search(text)
    if m:
        val = _to_f(float(m.group(1)), m.group(2))
        return {"metric": "temp", "type": "bracket", "low": val - 0.5, "high": val + 0.5}

    return None


def _log_unmatched(ticker: str, question: str, reason: str) -> None:
    with open(UNMATCHED_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "question": question,
            "reason": reason,
        }
        f.write(json.dumps(entry) + "\n")


# ── Main public API ───────────────────────────────────────────────────────────

def align_market(market: dict) -> Optional[dict]:
    """
    Parse one Kalshi market dict and extract all four required fields.
    Returns enriched dict or None if parsing fails.

    Required output fields:
        location, lat, lon, metric, threshold_type,
        bracket_low, bracket_high, resolution_dt
    """
    ticker   = market.get("ticker", "")
    question = (market.get("title") or market.get("rules_primary") or "").strip()

    # 1. City / location
    city = _find_city(question)
    if not city:
        _log_unmatched(ticker, question, "city_not_found")
        return None

    coords = CITY_COORDS[city]

    # 2. Threshold (metric + value)
    thresh = _parse_threshold(question)
    if not thresh:
        _log_unmatched(ticker, question, "threshold_not_parsed")
        return None

    # 3. Resolution datetime — prefer market's _resolution_dt if already set
    res_raw = (market.get("_resolution_dt")
               or market.get("expiration_time")
               or market.get("close_time")
               or question)
    res_dt  = _parse_date(str(res_raw))
    if not res_dt:
        _log_unmatched(ticker, question, "resolution_date_not_found")
        return None

    enriched = dict(market)
    enriched.update({
        "location":        city,
        "lat":             coords["lat"],
        "lon":             coords["lon"],
        "metric":          thresh["metric"],
        "threshold_type":  thresh["type"],
        "bracket_low":     thresh["low"],
        "bracket_high":    thresh["high"],
        "resolution_dt":   res_dt.isoformat(),
        "_aligned":        True,
    })
    return enriched


def align_markets(markets: list[dict]) -> list[dict]:
    """Align a batch of markets. Logs failures. Returns only successes."""
    aligned = []
    for m in markets:
        result = align_market(m)
        if result:
            aligned.append(result)
    _log.info(f"Alignment: {len(aligned)}/{len(markets)} markets parsed successfully")
    return aligned


if __name__ == "__main__":
    tests = [
        {"ticker": "T1", "title": "Will the high temperature in Phoenix exceed 95°F on March 25?"},
        {"ticker": "T2", "title": "Will it rain more than 0.5 inches in Seattle on March 24?"},
        {"ticker": "T3", "title": "Will Denver have a high of 78°F on March 26?"},
        {"ticker": "T4", "title": "Will the Miami Heat win the NBA Finals?"},
        {"ticker": "T5", "title": "Will there be any rain in Chicago on March 25?"},
        {"ticker": "T6", "title": "Will snowfall exceed 2 inches in Boston on March 24?"},
    ]
    results = align_markets(tests)
    print(f"\nAligned {len(results)}/{len(tests)} markets:")
    for r in results:
        print(f"  {r['ticker']} | {r['location']} ({r['lat']},{r['lon']}) | "
              f"{r['metric']} {r['threshold_type']} "
              f"[{r['bracket_low']}, {r['bracket_high']}] | {r['resolution_dt']}")
