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
UNMATCHED_LOG     = "unmatched.log"
MAPPING_AUDIT_LOG = "mapping_audit.log"

# Metric → Open-Meteo variable and unit conversion documentation
_METEO_MAPPING = {
    "temp": {
        "variable": "temperature_2m",
        "unit_conversion": "°C → °F via (C * 9/5 + 32)",
    },
    "rain": {
        "variable": "precipitation",
        "unit_conversion": "mm (no conversion)",
    },
    "snow": {
        "variable": "snowfall",
        "unit_conversion": "cm (no conversion)",
    },
}

# ── City → lat/lon lookup ─────────────────────────────────────────────────────
# Airport station coordinates — Kalshi settles on these stations
CITY_COORDS: dict[str, dict] = {
    "New York":       {"lat": 40.6413,  "lon": -73.7781},   # KJFK
    "NYC":            {"lat": 40.6413,  "lon": -73.7781},   # KJFK
    "New York City":  {"lat": 40.6413,  "lon": -73.7781},   # KJFK
    "Los Angeles":    {"lat": 33.9416,  "lon": -118.4085},  # KLAX
    "LA":             {"lat": 33.9416,  "lon": -118.4085},  # KLAX
    "Chicago":        {"lat": 41.9742,  "lon": -87.9073},   # KORD
    "Houston":        {"lat": 29.9844,  "lon": -95.3414},   # KIAH
    "Phoenix":        {"lat": 33.4373,  "lon": -112.0078},  # KPHX
    "Philadelphia":   {"lat": 39.8721,  "lon": -75.2411},   # KPHL
    "Philly":         {"lat": 39.8721,  "lon": -75.2411},   # KPHL
    "San Antonio":    {"lat": 29.5337,  "lon": -98.4698},   # KSAT
    "San Diego":      {"lat": 32.7336,  "lon": -117.1897},  # KSAN
    "Dallas":         {"lat": 32.8998,  "lon": -97.0403},   # KDFW
    "Austin":         {"lat": 30.1944,  "lon": -97.6700},   # KAUS
    "Jacksonville":   {"lat": 30.4941,  "lon": -81.6879},   # KJAX
    "Columbus":       {"lat": 39.9980,  "lon": -82.8919},   # KCMH
    "Charlotte":      {"lat": 35.2140,  "lon": -80.9431},   # KCLT
    "Indianapolis":   {"lat": 39.7173,  "lon": -86.2944},   # KIND
    "Seattle":        {"lat": 47.4502,  "lon": -122.3088},  # KSEA
    "Denver":         {"lat": 39.8561,  "lon": -104.6737},  # KDEN
    "Nashville":      {"lat": 36.1245,  "lon": -86.6782},   # KBNA
    "Miami":          {"lat": 25.7959,  "lon": -80.2870},   # KMIA
    "Atlanta":        {"lat": 33.6407,  "lon": -84.4277},   # KATL
    "Boston":         {"lat": 42.3656,  "lon": -71.0096},   # KBOS
    "Las Vegas":      {"lat": 36.0840,  "lon": -115.1537},  # KLAS
    "Portland":       {"lat": 45.5887,  "lon": -122.5975},  # KPDX
    "Memphis":        {"lat": 35.0424,  "lon": -89.9767},   # KMEM
    "Baltimore":      {"lat": 39.1754,  "lon": -76.6684},   # KBWI
    "Milwaukee":      {"lat": 42.9472,  "lon": -87.8966},   # KMKE
    "Albuquerque":    {"lat": 35.0402,  "lon": -106.6090},  # KABQ
    "Tucson":         {"lat": 32.1161,  "lon": -110.9410},  # KTUS
    "Sacramento":     {"lat": 38.6955,  "lon": -121.5908},  # KSMF
    "Kansas City":    {"lat": 39.2976,  "lon": -94.7139},   # KMCI
    "Omaha":          {"lat": 41.3032,  "lon": -95.8941},   # KOMA
    "Raleigh":        {"lat": 35.8776,  "lon": -78.7875},   # KRDU
    "Cleveland":      {"lat": 41.4117,  "lon": -81.8498},   # KCLE
    "Minneapolis":    {"lat": 44.8848,  "lon": -93.2223},   # KMSP
    "New Orleans":    {"lat": 29.9934,  "lon": -90.2580},   # KMSY
    "Tampa":          {"lat": 27.9755,  "lon": -82.5332},   # KTPA
    "Pittsburgh":     {"lat": 40.4915,  "lon": -80.2329},   # KPIT
    "Cincinnati":     {"lat": 39.0489,  "lon": -84.6678},   # KCVG
    "St. Louis":      {"lat": 38.7487,  "lon": -90.3700},   # KSTL
    "Reno":           {"lat": 39.4991,  "lon": -119.7681},  # KRNO
    "Salt Lake City": {"lat": 40.7884,  "lon": -111.9778},  # KSLC
    "Oklahoma City":  {"lat": 35.3931,  "lon": -97.6007},   # KOKC
    "OKC":            {"lat": 35.3931,  "lon": -97.6007},   # KOKC
    "San Francisco":  {"lat": 37.6213,  "lon": -122.3790},  # KSFO
    "SF":             {"lat": 37.6213,  "lon": -122.3790},  # KSFO
    "San Jose":       {"lat": 37.3626,  "lon": -121.9291},  # KSJC
    "Fort Worth":     {"lat": 32.8998,  "lon": -97.0403},   # KDFW (shared with Dallas)
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

    # Mapping audit log
    meteo_info = _METEO_MAPPING.get(thresh["metric"], {"variable": "unknown", "unit_conversion": "unknown"})
    _log_mapping_audit(
        ticker=ticker,
        question=question,
        city=city,
        metric=thresh["metric"],
        threshold_type=thresh["type"],
        bracket_low=thresh["low"],
        bracket_high=thresh["high"],
        resolution_dt=res_dt.isoformat(),
        open_meteo_variable=meteo_info["variable"],
        unit_conversion=meteo_info["unit_conversion"],
    )

    return enriched


def _log_mapping_audit(**kwargs) -> None:
    """Log mapping audit entry for manual spot-checking."""
    entry = {"ts": datetime.now(timezone.utc).isoformat(), **kwargs}
    with open(MAPPING_AUDIT_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


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
