"""
postmortem.py — Post-Mortem Analysis for Every Resolved Bet

After every resolved bet, writes a detailed post-mortem to postmortem.log:
- What did the ensemble predict?
- What did NWS actually record?
- How far off was the prediction?
- Was there a heat wave / cold snap anomaly?
- Was the market price moving against us?
- What would the correct bet have been in hindsight?

Called by resolver.py after each resolution.

Usage:
    python3 postmortem.py                    # analyze all resolved trades
    python3 postmortem.py --ticker KXHIGH... # analyze one trade
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger(__name__)

POSTMORTEM_LOG = "postmortem.log"
SIGNALS_LOG = "signals.log"
SAFETY_LOG = "safety.log"


def _load_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def _get_safety_events(ticker: str) -> list[dict]:
    """Get all safety events for a ticker."""
    events = _load_jsonl(SAFETY_LOG)
    return [e for e in events if e.get("ticker") == ticker]


def generate_postmortem(trade: dict) -> dict:
    """
    Generate a post-mortem analysis for a resolved trade.

    Returns a postmortem dict with all analysis fields.
    """
    ticker = trade.get("ticker", "")
    outcome = trade.get("outcome", "unknown")
    side = trade.get("side", "YES")
    true_prob = trade.get("true_prob", 0)
    yes_price = trade.get("yes_price", 0)
    actual_temp = trade.get("actual_temperature")
    ensemble_predicted = trade.get("ensemble_predicted", true_prob)
    city = trade.get("location", "")
    date = trade.get("date", "")

    # Parse threshold from question
    question = trade.get("question", "") or trade.get("kalshi_threshold", "")
    bracket_low = trade.get("bracket_low")
    bracket_high = trade.get("bracket_high")
    threshold_type = trade.get("threshold_type", "")

    # Calculate error delta (temperature)
    temp_error = None
    if actual_temp is not None:
        if threshold_type == "bracket" and bracket_low is not None and bracket_high is not None:
            bracket_mid = (bracket_low + bracket_high) / 2
            temp_error = round(actual_temp - bracket_mid, 1)
        elif threshold_type == "upper" and bracket_low is not None:
            temp_error = round(actual_temp - bracket_low, 1)
        elif threshold_type == "lower" and bracket_high is not None:
            temp_error = round(actual_temp - bracket_high, 1)

    # Check for anomaly flags from safety events
    safety_events = _get_safety_events(ticker)
    anomaly_flag = any(e.get("type") in ("heat_wave_detected", "nws_divergence")
                       for e in safety_events)
    momentum_signal = any(e.get("type") == "momentum_adverse" for e in safety_events)

    # Hindsight correct bet
    if actual_temp is not None:
        if threshold_type == "bracket" and bracket_low is not None and bracket_high is not None:
            temp_in_bracket = bracket_low <= actual_temp < bracket_high
            hindsight = "YES" if temp_in_bracket else "NO"
        elif threshold_type == "upper" and bracket_low is not None:
            hindsight = "YES" if actual_temp >= bracket_low else "NO"
        elif threshold_type == "lower" and bracket_high is not None:
            hindsight = "YES" if actual_temp < bracket_high else "NO"
        else:
            hindsight = "unknown"
    else:
        hindsight = "unknown"

    # Confidence error: how wrong was our probability estimate?
    prob_error = None
    if true_prob is not None:
        actual_binary = 1.0 if outcome == "won" else 0.0
        if side == "NO":
            # For NO bets, we predicted 1-true_prob chance of winning
            prob_error = round(abs((1 - true_prob) - actual_binary), 4)
        else:
            prob_error = round(abs(true_prob - actual_binary), 4)

    postmortem = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "city": city,
        "date": date,
        "side": side,
        "outcome": outcome,
        "question": question[:200],
        # Predictions
        "ensemble_predicted_prob": ensemble_predicted,
        "ensemble_confidence": f"{true_prob:.1%}" if true_prob else "unknown",
        "yes_price_at_entry": yes_price,
        "edge_ratio": trade.get("ratio"),
        # Actuals
        "actual_temperature": actual_temp,
        "kalshi_threshold": {
            "type": threshold_type,
            "low": bracket_low,
            "high": bracket_high,
        },
        # Analysis
        "error_delta_temp_f": temp_error,
        "error_delta_prob": prob_error,
        "anomaly_flag": anomaly_flag,
        "momentum_signal": momentum_signal,
        "hindsight_correct_bet": hindsight,
        "our_bet_was_correct": (side == hindsight) if hindsight != "unknown" else None,
        # Lessons
        "safety_events": len(safety_events),
        "pnl": trade.get("pnl", 0),
    }

    return postmortem


def write_postmortem(postmortem: dict) -> None:
    """Append postmortem to postmortem.log."""
    with open(POSTMORTEM_LOG, "a") as f:
        f.write(json.dumps(postmortem) + "\n")

    status = "✅ WIN" if postmortem["outcome"] == "won" else "❌ LOSS"
    _log.info(
        f"Postmortem: {postmortem['ticker']} [{postmortem['side']}] {status} | "
        f"actual={postmortem['actual_temperature']}°F | "
        f"error={postmortem.get('error_delta_temp_f', '?')}°F | "
        f"anomaly={'YES' if postmortem['anomaly_flag'] else 'no'}"
    )


def analyze_all_resolved():
    """Generate post-mortems for all resolved trades missing one."""
    signals = _load_jsonl(SIGNALS_LOG)
    existing = {pm.get("ticker") for pm in _load_jsonl(POSTMORTEM_LOG)}

    count = 0
    for trade in signals:
        if not trade.get("outcome"):
            continue
        if trade.get("ticker") in existing:
            continue
        if trade.get("dry_run", False):
            continue

        pm = generate_postmortem(trade)
        write_postmortem(pm)
        count += 1

    _log.info(f"Generated {count} new post-mortems")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AUGUR Post-Mortem Analyzer")
    parser.add_argument("--ticker", help="Analyze specific ticker only")
    args = parser.parse_args()

    if args.ticker:
        signals = _load_jsonl(SIGNALS_LOG)
        trade = next((s for s in signals if s.get("ticker") == args.ticker), None)
        if trade and trade.get("outcome"):
            pm = generate_postmortem(trade)
            write_postmortem(pm)
            print(json.dumps(pm, indent=2))
        else:
            print(f"Trade {args.ticker} not found or not yet resolved")
    else:
        analyze_all_resolved()
