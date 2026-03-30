"""
calibration.py — Model Calibration Tracker for Project AUGUR

Purpose:
    After each Kalshi market resolves, compare the ensemble model's
    predicted probability against the actual binary outcome (YES=1, NO=0).

    Tracks Brier Score over time:
        BS = (predicted_prob - actual_outcome)^2
        Lower is better. Perfect = 0.0, Random = 0.25, Terrible = 1.0

    Logs each resolved bet to calibration.log (JSONL format).
    Provides weekly_summary() to print edge accuracy over the past 7 days.

Kalshi API endpoints used:
    GET /trade-api/v2/markets/{ticker}  — check resolution result field

Regulatory constraints:
    - Read-only — no order placement
    - All credentials from environment variables
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

KALSHI_BASE      = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_DEMO_BASE = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def check_market_resolution(ticker: str) -> Optional[bool]:
    """
    Check if a Kalshi market has resolved and return the YES outcome.

    Returns:
        True  — market resolved YES
        False — market resolved NO
        None  — market not yet resolved or API error
    """
    url = f"{_base_url()}/markets/{ticker}"
    try:
        resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=10)
        resp.raise_for_status()
        market = resp.json().get("market", resp.json())
        status = market.get("status", "")
        result = market.get("result", "")
        if status in ("finalized", "settled", "closed") and result:
            return result.lower() == "yes"
        return None
    except Exception as e:
        logging.getLogger(__name__).warning(f"Resolution check failed for {ticker}: {e}")
        return None

_log = logging.getLogger(__name__)

CALIBRATION_LOG = "calibration.log"


def log_resolution(
    market_id: str,
    question: str,
    predicted_prob: float,
    actual_outcome: bool,
    market_price: float,
    payout_multiplier: float,
    bet_size: float,
    pnl: float,
    city: str = "",
    date: str = "",
) -> dict:
    """
    Record a resolved market to calibration.log.

    Args:
        market_id:        Polymarket market ID
        question:         Full market question string
        predicted_prob:   Ensemble model probability (0-1)
        actual_outcome:   True if YES won, False if NO won
        market_price:     Price paid (0-1)
        payout_multiplier: Gross payout per $1
        bet_size:         Amount bet in USDC
        pnl:              Realized profit/loss in USDC
        city:             City name
        date:             Resolution date (YYYY-MM-DD)

    Returns:
        The calibration record dict (also written to log)
    """
    outcome_int = 1 if actual_outcome else 0
    brier = (predicted_prob - outcome_int) ** 2
    edge_realized = (1 if actual_outcome else 0) - market_price
    edge_predicted = predicted_prob - market_price

    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market_id": market_id,
        "question": question,
        "city": city,
        "date": date,
        "predicted_prob": round(predicted_prob, 4),
        "market_price": round(market_price, 4),
        "actual_outcome": actual_outcome,
        "brier_score": round(brier, 4),
        "edge_predicted": round(edge_predicted, 4),
        "edge_realized": round(edge_realized, 4),
        "payout_multiplier": payout_multiplier,
        "bet_size": bet_size,
        "pnl": round(pnl, 2),
    }

    with open(CALIBRATION_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")

    _log.info(f"Calibration: {question[:60]}... | "
              f"pred={predicted_prob:.1%} actual={'WIN' if actual_outcome else 'LOSS'} "
              f"brier={brier:.3f} pnl=${pnl:+.2f}")

    return record


def load_records(since_days: Optional[int] = None) -> list[dict]:
    """Load all calibration records, optionally filtered to last N days."""
    if not os.path.exists(CALIBRATION_LOG):
        return []

    records = []
    cutoff = None
    if since_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).isoformat()

    with open(CALIBRATION_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                if cutoff and r.get("ts", "") < cutoff:
                    continue
                records.append(r)
            except json.JSONDecodeError:
                continue
    return records


def brier_score(records: list[dict]) -> float:
    """Average Brier Score across a set of records. Lower = better."""
    if not records:
        return 0.0
    return round(sum(r["brier_score"] for r in records) / len(records), 4)


def rolling_brier_score(weeks: int = 4) -> list[tuple[str, float]]:
    """
    Calculate Brier Score per ISO week for the last N weeks.
    Returns list of (week_label, brier_score) tuples.
    Prints warning if Brier degrades for 3+ consecutive weeks.
    Also logs weekly scores to calibration.log.
    """
    records = load_records(since_days=weeks * 7)
    if not records:
        return []

    # Group by ISO week
    weekly: dict[str, list[dict]] = {}
    for r in records:
        try:
            ts = datetime.fromisoformat(r["ts"])
            week_label = f"{ts.isocalendar()[0]}-W{ts.isocalendar()[1]:02d}"
        except Exception:
            continue
        weekly.setdefault(week_label, []).append(r)

    results = []
    for week in sorted(weekly.keys()):
        recs = weekly[week]
        bs = round(sum(r["brier_score"] for r in recs) / len(recs), 4)
        results.append((week, bs))

        # Log weekly brier to calibration.log
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": "weekly_brier",
            "week": week,
            "brier_score": bs,
            "n_bets": len(recs),
        }
        with open(CALIBRATION_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")

        _log.info(f"Weekly Brier [{week}]: {bs:.4f} ({len(recs)} bets)")

    # Check for 3 consecutive weeks of degradation
    if len(results) >= 3:
        consecutive_worse = 0
        for i in range(1, len(results)):
            if results[i][1] > results[i - 1][1]:
                consecutive_worse += 1
            else:
                consecutive_worse = 0

            if consecutive_worse >= 2:
                _log.warning(
                    f"⚠️ CALIBRATION WARNING: Brier Score degraded for 3+ consecutive weeks! "
                    f"Recent: {', '.join(f'{w}={b:.4f}' for w, b in results[-3:])}"
                )
                break

    return results


def daily_calibration_cycle(open_trades: list[dict]) -> list[dict]:
    """
    Run the daily calibration cycle:
    1. Pull resolutions for open trades
    2. Calculate rolling Brier scores
    3. Return list of still-open trades

    Called from run_augur.py during 6am EST cycle.
    """
    still_open = []
    for trade in open_trades:
        ticker = trade.get("ticker")
        outcome = check_market_resolution(ticker)
        if outcome is None:
            still_open.append(trade)
            continue

        pnl = (trade["bet_size"] * trade["payout_multiplier"] - trade["bet_size"]
               if outcome else -trade["bet_size"])
        log_resolution(
            market_id       = ticker,
            question        = trade.get("question", ""),
            predicted_prob  = trade.get("true_prob", 0),
            actual_outcome  = outcome,
            market_price    = trade.get("yes_price", 0),
            payout_multiplier = trade.get("payout_multiplier", 1),
            bet_size        = trade.get("bet_size", 0),
            pnl             = pnl,
            city            = trade.get("location", ""),
            date            = trade.get("date", ""),
        )
        _log.info(f"Calibration resolved: {trade.get('question', '')[:50]} | "
                  f"{'WIN' if outcome else 'LOSS'} | PnL=${pnl:+.2f}")

    # Run rolling Brier check
    rolling_brier_score(weeks=4)

    return still_open


def check_model_drift() -> dict:
    """
    Check for model drift:
    1. If Brier Score worsens 3 consecutive weeks → pause live betting
    2. If a specific city has Brier > 0.5 over last 10 bets → flag as unreliable

    Returns:
        {
            "pause_recommended": bool,
            "unreliable_cities": list[str],
            "weekly_trend": list[tuple[str, float]],
        }
    """
    result = {
        "pause_recommended": False,
        "unreliable_cities": [],
        "weekly_trend": [],
    }

    # Check weekly Brier trend
    records = load_records(since_days=28)
    if not records:
        return result

    weekly: dict[str, list[dict]] = {}
    for r in records:
        try:
            ts = datetime.fromisoformat(r["ts"])
            week_label = f"{ts.isocalendar()[0]}-W{ts.isocalendar()[1]:02d}"
        except Exception:
            continue
        weekly.setdefault(week_label, []).append(r)

    weekly_scores = []
    for week in sorted(weekly.keys()):
        recs = weekly[week]
        bs = round(sum(r["brier_score"] for r in recs) / len(recs), 4)
        weekly_scores.append((week, bs))

    result["weekly_trend"] = weekly_scores

    # Check 3 consecutive weeks of degradation
    if len(weekly_scores) >= 3:
        consecutive_worse = 0
        for i in range(1, len(weekly_scores)):
            if weekly_scores[i][1] > weekly_scores[i - 1][1]:
                consecutive_worse += 1
            else:
                consecutive_worse = 0

            if consecutive_worse >= 2:
                result["pause_recommended"] = True
                _log.warning(
                    "⚠️ MODEL DRIFT DETECTED: Brier Score degraded 3+ consecutive weeks. "
                    "RECOMMENDING PAUSE ON LIVE BETTING. "
                    f"Recent: {', '.join(f'{w}={b:.4f}' for w, b in weekly_scores[-3:])}"
                )

                # Write pause flag
                pause_entry = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "type": "drift_pause",
                    "weekly_scores": weekly_scores[-3:],
                    "action": "LIVE BETTING PAUSED — investigate model drift",
                }
                with open(CALIBRATION_LOG, "a") as f:
                    f.write(json.dumps(pause_entry) + "\n")
                break

    # Check per-city Brier scores
    city_scores: dict[str, list[float]] = {}
    for r in records:
        city = r.get("city", "unknown")
        city_scores.setdefault(city, []).append(r.get("brier_score", 0))

    for city, scores in city_scores.items():
        if len(scores) >= 10:
            avg = sum(scores) / len(scores)
            if avg > 0.5:
                result["unreliable_cities"].append(city)
                _log.warning(
                    f"⚠️ UNRELIABLE CITY: {city} Brier={avg:.3f} over "
                    f"{len(scores)} bets → flagged for skip"
                )

    return result


def is_live_paused() -> bool:
    """Check if live betting should be paused due to model drift."""
    if not os.path.exists(CALIBRATION_LOG):
        return False
    # Check last 10 entries for a pause flag
    try:
        with open(CALIBRATION_LOG) as f:
            lines = f.readlines()
        for line in reversed(lines[-20:]):
            try:
                entry = json.loads(line.strip())
                if entry.get("type") == "drift_pause":
                    return True
                if entry.get("type") == "drift_resume":
                    return False
            except json.JSONDecodeError:
                continue
    except Exception:
        pass
    return False


def resume_live_betting():
    """Clear the drift pause flag."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": "drift_resume",
        "action": "Live betting resumed after investigation",
    }
    with open(CALIBRATION_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")
    _log.info("Live betting resumed — drift pause cleared")


def weekly_summary() -> str:
    """
    Print a weekly calibration summary showing:
    - Total bets resolved
    - Win rate
    - Average Brier Score
    - Average predicted edge vs realized edge
    - Total PnL
    """
    records = load_records(since_days=7)

    if not records:
        return "No calibration data in the past 7 days."

    n = len(records)
    wins = sum(1 for r in records if r["actual_outcome"])
    win_rate = wins / n
    avg_brier = brier_score(records)
    avg_pred_edge = sum(r["edge_predicted"] for r in records) / n
    avg_real_edge = sum(r["edge_realized"] for r in records) / n
    total_pnl = sum(r["pnl"] for r in records)
    total_bet = sum(r["bet_size"] for r in records)
    roi = total_pnl / total_bet * 100 if total_bet > 0 else 0

    city_stats: dict[str, list] = {}
    for r in records:
        c = r.get("city", "unknown")
        if c not in city_stats:
            city_stats[c] = []
        city_stats[c].append(r)

    lines = [
        "",
        "=" * 60,
        "  PROJECT POLY — WEEKLY CALIBRATION SUMMARY",
        f"  Period: last 7 days  |  {n} resolved markets",
        "=" * 60,
        f"  Win rate        : {win_rate:.1%}  ({wins}/{n})",
        f"  Avg Brier Score : {avg_brier:.4f}  (0=perfect, 0.25=random)",
        f"  Avg pred edge   : {avg_pred_edge:+.1%}  (model predicted)",
        f"  Avg real edge   : {avg_real_edge:+.1%}  (what happened)",
        f"  Total PnL       : ${total_pnl:+.2f}  on ${total_bet:.2f} bet  ({roi:+.1f}% ROI)",
        "",
        "  By City:",
    ]

    for city, recs in sorted(city_stats.items()):
        c_wins = sum(1 for r in recs if r["actual_outcome"])
        c_pnl = sum(r["pnl"] for r in recs)
        lines.append(f"    {city:20s} {c_wins}/{len(recs)} wins  ${c_pnl:+.2f}")

    lines += ["=" * 60, ""]
    summary = "\n".join(lines)
    print(summary)
    return summary


if __name__ == "__main__":
    # Simulate some resolutions for testing
    test_cases = [
        ("m1", "Will Denver be 78-79F on Mar 24?", 0.333, True,  0.095, 11.0, 5.0,  47.5,  "Denver", "2026-03-24"),
        ("m2", "Will Atlanta be 54-55F on Mar 25?", 0.103, False, 0.012, 83.0, 5.0,  -5.0,  "Atlanta", "2026-03-25"),
        ("m3", "Will Miami be 85-86F on Mar 26?",   0.250, True,  0.120, 8.3,  5.0,  36.5,  "Miami",   "2026-03-26"),
        ("m4", "Will Denver be 90F+ on Mar 27?",    0.450, False, 0.200, 5.0,  5.0,  -5.0,  "Denver",  "2026-03-27"),
    ]
    print("Logging test resolutions...")
    for args in test_cases:
        log_resolution(*args)
    weekly_summary()
