"""
market_mapper.py — Market-to-Model Alignment Layer for Project AUGUR

Purpose:
    Parses Kalshi weather market questions and extracts the four fields
    required by weather_ensemble.py:
        1. location   — city name + lat/lon coordinates
        2. metric     — temp / rain / snow
        3. threshold  — numeric value and comparison type
        4. resolution_dt — exact UTC datetime

    Kalshi question formats observed:
        "Will the maximum temperature be  98-99° on Mar 24, 2026?"
        "Will the maximum temperature be  <96° on Mar 24, 2026?"
        "Will the maximum temperature be  >103° on Mar 24, 2026?"
        "Will the **high temp in NYC** be >53° on Mar 24, 2026?"
        "Will the **high temp in NYC** be 52-53° on Mar 24, 2026?"
        "Rain in NYC in Mar 2026?"

Kalshi API endpoints used:
    None — pure parser. Input comes from kalshi_scanner.py.

Regulatory constraints:
    - No API calls made here. Pure data transformation.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

_log = logging.getLogger(__name__)
UNMATCHED_LOG = "unmatched.log"

# ── City → lat/lon lookup ─────────────────────────────────────────────────────
CITY_COORDS: dict[str, dict] = {
    "New York":       {"lat": 40.7128,  "lon": -74.0060},
    "NYC":            {"lat": 40.7128,  "lon": -74.0060},
    "New York City":  {"lat": 40.7128,  "lon": -74.0060},
    "Los Angeles":    {"lat": 34.0522,  "lon": -118.2437},
    "LA":             {"lat": 34.0522,  "lon": -118.2437},
    "Chicago":        {"lat": 41.8781,  "lon": -87.6298},
    "Houston":        {"lat": 29.7604,  "lon": -95.3698},
    "Phoenix":        {"lat": 33.4484,  "lon": -112.0740},
    "Philadelphia":   {"lat": 39.9526,  "lon": -75.1652},
    "Philly":         {"lat": 39.9526,  "lon": -75.1652},
    "San Antonio":    {"lat": 29.4241,  "lon": -98.4936},
    "San Diego":      {"lat": 32.7157,  "lon": -117.1611},
    "Dallas":         {"lat": 32.7767,  "lon": -96.7970},
    "Austin":         {"lat": 30.2672,  "lon": -97.7431},
    "Jacksonville":   {"lat": 30.3322,  "lon": -81.6557},
    "Columbus":       {"lat": 39.9612,  "lon": -82.9988},
    "Charlotte":      {"lat": 35.2271,  "lon": -80.8431},
    "Indianapolis":   {"lat": 39.7684,  "lon": -86.1581},
    "Seattle":        {"lat": 47.6062,  "lon": -122.3321},
    "Denver":         {"lat": 39.7392,  "lon": -104.9903},
    "Nashville":      {"lat": 36.1627,  "lon": -86.7816},
    "Miami":          {"lat": 25.7617,  "lon": -80.1918},
    "Atlanta":        {"lat": 33.7490,  "lon": -84.3880},
    "Boston":         {"lat": 42.3601,  "lon": -71.0589},
    "Las Vegas":      {"lat": 36.1699,  "lon": -115.1398},
    "Portland":       {"lat": 45.5051,  "lon": -122.6750},
    "Memphis":        {"lat": 35.1495,  "lon": -90.0490},
    "Baltimore":      {"lat": 39.2904,  "lon": -76.6122},
    "Milwaukee":      {"lat": 43.0389,  "lon": -87.9065},
    "Albuquerque":    {"lat": 35.0844,  "lon": -106.6504},
    "Tucson":         {"lat": 32.2226,  "lon": -110.9747},
    "Sacramento":     {"lat": 38.5816,  "lon": -121.4944},
    "Kansas City":    {"lat": 39.0997,  "lon": -94.5786},
    "Omaha":          {"lat": 41.2565,  "lon": -95.9345},
    "Raleigh":        {"lat": 35.7796,  "lon": -78.6382},
    "Cleveland":      {"lat": 41.4993,  "lon": -81.6944},
    "Minneapolis":    {"lat": 44.9778,  "lon": -93.2650},
    "New Orleans":    {"lat": 29.9511,  "lon": -90.0715},
    "Tampa":          {"lat": 27.9506,  "lon": -82.4572},
    "Pittsburgh":     {"lat": 40.4406,  "lon": -79.9959},
    "Cincinnati":     {"lat": 39.1031,  "lon": -84.5120},
    "St. Louis":      {"lat": 38.6270,  "lon": -90.1994},
    "Reno":           {"lat": 39.5296,  "lon": -119.8138},
    "Salt Lake City": {"lat": 40.7608,  "lon": -111.8910},
    "Oklahoma City":  {"lat": 35.4676,  "lon": -97.5164},
    "OKC":            {"lat": 35.4676,  "lon": -97.5164},
    "San Francisco":  {"lat": 37.7749,  "lon": -122.4194},
    "SF":             {"lat": 37.7749,  "lon": -122.4194},
    "San Jose":       {"lat": 37.3382,  "lon": -121.8863},
    "Fort Worth":     {"lat": 32.7555,  "lon": -97.3308},
}

# Build lowercase lookup — longest match wins
_CITY_LOWER = {k.lower(): k for k in CITY_COORDS}

# ── Regex patterns for Kalshi question formats ────────────────────────────────

# "Will the maximum temperature be  98-99° on Mar 24, 2026?"
# "Will the maximum temperature be  98.5-99.5° on Mar 24, 2026?"
_RE_BRACKET = re.compile(
    r"be\s+(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*°?\s*(?:f|fahrenheit)?",
    re.IGNORECASE,
)

# "Will the maximum temperature be  >103° on Mar 24, 2026?"
# "Will the **high temp in NYC** be >53° on Mar 24, 2026?"
_RE_UPPER = re.compile(
    r"be\s+[>≥]\s*(\d+(?:\.\d+)?)\s*°?\s*(?:f|fahrenheit)?",
    re.IGNORECASE,
)

# "Will the maximum temperature be  <96° on Mar 24, 2026?"
_RE_LOWER = re.compile(
    r"be\s+[<≤]\s*(\d+(?:\.\d+)?)\s*°?\s*(?:f|fahrenheit)?",
    re.IGNORECASE,
)

# "Will the high temp in NYC be 52-53° on Mar 24, 2026?"
# Also matches "high temp in NYC"
_RE_CITY_IN = re.compile(
    r"(?:temp(?:erature)?\s+in|in)\s+([A-Za-z][A-Za-z\s\.]+?)(?:\s+be|\s+on|\s*\?|$)",
    re.IGNORECASE,
)

# Date: "Mar 24, 2026" or "March 24, 2026" or "Mar 24 2026"
_RE_DATE = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})(?:,?\s*(\d{4}))?",
    re.IGNORECASE,
)
_MONTHS = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}


def _find_city(text: str, ticker: str = "") -> Optional[str]:
    """Find city from question text or series ticker."""
    lower = text.lower()

    # Try city name in text first (longest match)
    for key in sorted(_CITY_LOWER.keys(), key=len, reverse=True):
        if key in lower:
            return _CITY_LOWER[key]

    # Fall back to ticker-based lookup
    ticker_map = {
        "PHX": "Phoenix",  "TPHX": "Phoenix",
        "NYC": "New York", "NY": "New York",   "LNYC": "New York",
        "CHI": "Chicago",  "LCHI": "Chicago",
        "MIA": "Miami",    "LMIA": "Miami",    "LTMIA": "Miami",
        "DEN": "Denver",   "LDEN": "Denver",   "LTDEN": "Denver",
        "HOU": "Houston",  "THOU": "Houston",
        "LAX": "Los Angeles", "TLAX": "Los Angeles",
        "SEA": "Seattle",  "TSEA": "Seattle",
        "ATL": "Atlanta",  "TATL": "Atlanta",
        "DAL": "Dallas",   "TDAL": "Dallas",
        "AUS": "Austin",   "TAUS": "Austin",   "LTAUS": "Austin",
        "SFO": "San Francisco", "TSFO": "San Francisco",
        "SATX": "San Antonio", "TSATX": "San Antonio",
        "MIN": "Minneapolis", "TMIN": "Minneapolis",
        "OKC": "Oklahoma City", "TOKC": "Oklahoma City",
        "PHIL": "Philadelphia",
    }
    # Strip KX prefix and series suffix to find city code
    t = ticker.upper().replace("KXHIGHT", "").replace("KXHIGH", "").replace("KXLOW", "").replace("KXLOWT", "").replace("KXRAIN", "").replace("KXSNOW", "")
    for code, city in ticker_map.items():
        if t.startswith(code):
            return city

    return None


def _parse_threshold(text: str) -> Optional[dict]:
    """Extract metric type and threshold bounds from question text."""

    # Detect metric
    lower = text.lower()
    if any(w in lower for w in ["rain", "precipitation", "rainfall"]):
        return {"metric": "rain", "type": "any", "low": None, "high": None}
    if any(w in lower for w in ["snow", "snowfall"]):
        return {"metric": "snow", "type": "any", "low": None, "high": None}

    # Temperature bracket: "98-99°"
    m = _RE_BRACKET.search(text)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        return {"metric": "temp", "type": "bracket", "low": lo, "high": hi}

    # Temperature upper: ">103°"
    m = _RE_UPPER.search(text)
    if m:
        return {"metric": "temp", "type": "upper", "low": float(m.group(1)), "high": None}

    # Temperature lower: "<96°"
    m = _RE_LOWER.search(text)
    if m:
        return {"metric": "temp", "type": "lower", "low": None, "high": float(m.group(1))}

    return None


def _parse_date(text: str, fallback: str = "") -> Optional[datetime]:
    """Extract resolution date from question text or fallback string."""
    for src in [text, fallback]:
        m = _RE_DATE.search(src)
        if m:
            month = _MONTHS.get(m.group(1).lower()[:3])
            day = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else datetime.now(timezone.utc).year
            if month:
                return datetime(year, month, day, 23, 59, tzinfo=timezone.utc)
    return None


def _log_unmatched(ticker: str, question: str, reason: str) -> None:
    with open(UNMATCHED_LOG, "a") as f:
        f.write(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker, "question": question, "reason": reason,
        }) + "\n")


def align_market(market: dict) -> Optional[dict]:
    """
    Parse one Kalshi market dict and extract all four required fields.
    Returns enriched dict or None if parsing fails.
    """
    ticker   = market.get("ticker", "")
    question = (market.get("title") or market.get("rules_primary") or "").strip()
    # Strip markdown bold markers Kalshi sometimes includes
    question_clean = re.sub(r"\*\*([^*]+)\*\*", r"\1", question)

    # 1. City
    city = _find_city(question_clean, ticker)
    if not city:
        _log_unmatched(ticker, question, "city_not_found")
        return None

    coords = CITY_COORDS.get(city, CITY_COORDS.get("New York"))

    # 2. Threshold
    thresh = _parse_threshold(question_clean)
    if not thresh:
        _log_unmatched(ticker, question, "threshold_not_parsed")
        return None

    # 3. Resolution datetime
    res_raw = (market.get("_resolution_dt")
               or market.get("expected_expiration_time")
               or market.get("close_time") or "")
    res_dt = _parse_date(question_clean, str(res_raw))
    if not res_dt:
        # Use expected_expiration_time directly if date parse fails
        if res_raw:
            try:
                res_dt = datetime.fromisoformat(str(res_raw).replace("Z", "+00:00"))
            except Exception:
                pass
    if not res_dt:
        _log_unmatched(ticker, question, "resolution_date_not_found")
        return None

    enriched = dict(market)
    enriched.update({
        "location":       city,
        "lat":            coords["lat"],
        "lon":            coords["lon"],
        "metric":         thresh["metric"],
        "threshold_type": thresh["type"],
        "bracket_low":    thresh["low"],
        "bracket_high":   thresh["high"],
        "resolution_dt":  res_dt.isoformat(),
        "_aligned":       True,
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
        {"ticker": "KXHIGHTPHX-26MAR24-B98.5",  "title": "Will the maximum temperature be  98-99° on Mar 24, 2026?",       "expected_expiration_time": "2026-03-25T19:00:00Z"},
        {"ticker": "KXHIGHTPHX-26MAR24-T96",     "title": "Will the maximum temperature be  <96° on Mar 24, 2026?",         "expected_expiration_time": "2026-03-25T19:00:00Z"},
        {"ticker": "KXHIGHTPHX-26MAR24-T103",    "title": "Will the maximum temperature be  >103° on Mar 24, 2026?",        "expected_expiration_time": "2026-03-25T19:00:00Z"},
        {"ticker": "KXHIGHNY-26MAR24-B52",       "title": "Will the **high temp in NYC** be 52-53° on Mar 24, 2026?",       "expected_expiration_time": "2026-03-25T19:00:00Z"},
        {"ticker": "KXHIGHNY-26MAR24-T53",       "title": "Will the **high temp in NYC** be >53° on Mar 24, 2026?",         "expected_expiration_time": "2026-03-25T19:00:00Z"},
        {"ticker": "KXRAINNYCM-26MAR",           "title": "Rain in NYC in Mar 2026?",                                       "expected_expiration_time": "2026-03-31T19:00:00Z"},
        {"ticker": "KXSNOWNYC-26MAR24",          "title": "Will it snow in NYC on Mar 24, 2026?",                           "expected_expiration_time": "2026-03-25T19:00:00Z"},
    ]
    results = align_markets(tests)
    print(f"\nAligned {len(results)}/{len(tests)} markets:")
    for r in results:
        print(f"  {r['ticker']} | {r['location']} | {r['metric']} {r['threshold_type']} "
              f"[{r['bracket_low']}, {r['bracket_high']}] | {r['resolution_dt'][:10]}")
