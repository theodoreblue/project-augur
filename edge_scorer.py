"""
edge_scorer.py — Edge Scoring + Position Limit Check for Project AUGUR

Purpose:
    Scores each aligned market by comparing ensemble true probability
    against the Kalshi market YES price. Only passes markets with
    edge ratio >= 2.0 to the execution layer.

    Also checks each market's contract position limit before scoring
    and adjusts intended position size to stay within limits.

Kalshi API endpoints used:
    GET /trade-api/v2/markets/{ticker}  — live position limit check

Regulatory constraints:
    - Position limits are hard exchange constraints — never exceed
    - Read-only API calls only — no order placement here
    - All credentials from environment variables
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)

MIN_EDGE_RATIO   = 2.0    # true_prob / yes_price must be >= 2x
MIN_EDGE_ABS     = 0.03   # absolute edge (true_prob - price) >= 3%
MIN_TRUE_PROB    = 0.65   # minimum confidence threshold — reject below 65%
SKIPPED_LOG      = "skipped.log"


def spread_confidence_factor(std_dev: float) -> float:
    """
    Reduce confidence when ensemble spread is wide.
    Wide model disagreement = uncertain forecast = lower bet confidence.

    std < 2°F:  full confidence (1.0)
    std 2-4°F:  moderate reduction (0.8-1.0)
    std 4-8°F:  significant reduction (0.5-0.8)
    std > 8°F:  heavy reduction (0.3-0.5)
    """
    if std_dev <= 2.0:
        return 1.0
    elif std_dev <= 4.0:
        return 1.0 - 0.1 * (std_dev - 2.0)   # 1.0 → 0.8
    elif std_dev <= 8.0:
        return 0.8 - 0.075 * (std_dev - 4.0)  # 0.8 → 0.5
    else:
        return max(0.3, 0.5 - 0.02 * (std_dev - 8.0))

KALSHI_BASE      = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_DEMO_BASE = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")


def _base_url() -> str:
    if os.getenv("KALSHI_USE_DEMO", "false").lower() == "true":
        return KALSHI_DEMO_BASE
    return KALSHI_BASE


def _auth_headers(method: str = "GET", path: str = "/trade-api/v2") -> dict:
    from kalshi_auth import get_auth_headers
    return get_auth_headers(method, path)


def get_position_limit(ticker: str) -> Optional[float]:
    """
    Fetch max position size for the contract from Kalshi API.
    Returns None if unavailable.
    """
    url = f"{_base_url()}/markets/{ticker}"
    try:
        resp = requests.get(url, headers=_auth_headers("GET", f"/trade-api/v2/markets/{ticker}"), timeout=8)
        resp.raise_for_status()
        market = resp.json().get("market", resp.json())
        limit = market.get("position_limit") or market.get("max_position_dollars")
        return float(limit) if limit is not None else None
    except Exception as e:
        _log.debug(f"Position limit fetch failed for {ticker}: {e}")
        return None


def score_market(
    market: dict,
    true_prob: float,
    intended_bet: float = 5.0,
    ensemble_std: float = None,
) -> Optional[dict]:
    """
    Score one aligned market for edge.

    Args:
        market:        Aligned market dict (from market_mapper.py)
        true_prob:     Ensemble true probability (from weather_ensemble.py)
        intended_bet:  Intended bet size in USD (for position limit check)
        ensemble_std:  Standard deviation of ensemble members (°F). If provided,
                       applies spread confidence penalty to true_prob.

    Returns:
        Signal dict if edge qualifies, None if not.
    """
    ticker    = market.get("ticker", "")
    yes_price = market.get("_yes_price", 0)

    if yes_price <= 0 or true_prob <= 0:
        return None

    # Apply spread confidence penalty if ensemble stats available
    if ensemble_std is not None:
        confidence = spread_confidence_factor(ensemble_std)
        true_prob = true_prob * confidence

    # Minimum confidence filter — reject low-probability signals
    if true_prob < MIN_TRUE_PROB:
        _log_low_confidence(ticker, market.get("title", ""), true_prob, yes_price)
        return None

    ratio = true_prob / yes_price
    edge  = true_prob - yes_price

    if ratio < MIN_EDGE_RATIO:
        return None
    if edge < MIN_EDGE_ABS:
        return None

    payout_mult = market.get("_payout_multiplier", round(1.0 / yes_price, 2))

    # Position limit check — reduce bet if needed rather than skip
    adj_bet = intended_bet
    position_limit = get_position_limit(ticker)
    if position_limit is not None and adj_bet > position_limit:
        _log.info(f"Reducing bet ${adj_bet:.2f} → ${position_limit:.2f} "
                  f"(position limit on {ticker})")
        adj_bet = position_limit

    # Score: reward high ratio × high edge
    score = ratio * edge

    return {
        "ticker":           ticker,
        "market_id":        market.get("event_ticker", ticker),
        "question":         market.get("title", ""),
        "location":         market.get("location", ""),
        "date":             market.get("resolution_dt", "")[:10],
        "resolution_dt":    market.get("resolution_dt", ""),
        "metric":           market.get("metric", "temp"),
        "threshold_type":   market.get("threshold_type", ""),
        "bracket_low":      market.get("bracket_low"),
        "bracket_high":     market.get("bracket_high"),
        "yes_price":        yes_price,
        "true_prob":        round(true_prob, 4),
        "edge":             round(edge, 4),
        "ratio":            round(ratio, 2),
        "payout_multiplier": payout_mult,
        "score":            round(score, 6),
        "intended_bet":     round(adj_bet, 2),
        "position_limit":   position_limit,
        "hours_to_resolution": market.get("_hours_to_resolution"),
    }


def _log_low_confidence(ticker: str, question: str, true_prob: float, yes_price: float) -> None:
    """Log rejected low-confidence signals to skipped.log."""
    with open(SKIPPED_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "question": question,
            "reason": f"low_confidence: true_prob={true_prob:.3f} < {MIN_TRUE_PROB} "
                      f"(yes_price={yes_price:.4f}, ratio would be {true_prob/yes_price:.2f}x)",
        }
        f.write(json.dumps(entry) + "\n")
    _log.debug(f"Rejected {ticker}: true_prob={true_prob:.1%} < {MIN_TRUE_PROB:.0%} minimum")


def score_market_no_side(
    market: dict,
    true_prob: float,
    intended_bet: float = 5.0,
    ensemble_std: float = None,
) -> Optional[dict]:
    """
    Score a market for NO-side edge.

    Generates a NO signal if:
    - true_prob < 0.5 (event is unlikely)
    - (1 - yes_price) / (1 - true_prob) >= 2.0

    The NO contract costs (1 - yes_price) and pays $1 if event doesn't happen.
    """
    ticker    = market.get("ticker", "")
    yes_price = market.get("_yes_price", 0)

    # Apply spread confidence penalty for NO side too
    if ensemble_std is not None:
        confidence = spread_confidence_factor(ensemble_std)
        true_prob = true_prob * confidence

    if yes_price <= 0 or yes_price >= 1 or true_prob >= 0.5:
        return None

    no_price = 1.0 - yes_price
    no_true_prob = 1.0 - true_prob

    if no_true_prob <= 0:
        return None

    # User-specified formula: (1 - yes_price) / (1 - true_prob)
    no_ratio = no_price / no_true_prob

    if no_ratio < 2.0:
        return None

    no_edge = no_true_prob - no_price
    if no_edge < MIN_EDGE_ABS:
        return None

    payout_mult = round(1.0 / no_price, 2)

    # Position limit check
    adj_bet = intended_bet
    position_limit = get_position_limit(ticker)
    if position_limit is not None and adj_bet > position_limit:
        adj_bet = position_limit

    score = no_ratio * no_edge

    return {
        "ticker":           ticker,
        "market_id":        market.get("event_ticker", ticker),
        "question":         market.get("title", ""),
        "location":         market.get("location", ""),
        "date":             market.get("resolution_dt", "")[:10],
        "resolution_dt":    market.get("resolution_dt", ""),
        "metric":           market.get("metric", "temp"),
        "threshold_type":   market.get("threshold_type", ""),
        "bracket_low":      market.get("bracket_low"),
        "bracket_high":     market.get("bracket_high"),
        "side":             "NO",
        "yes_price":        yes_price,
        "no_price":         round(no_price, 4),
        "true_prob":        round(true_prob, 4),
        "no_true_prob":     round(no_true_prob, 4),
        "edge":             round(no_edge, 4),
        "ratio":            round(no_ratio, 2),
        "payout_multiplier": payout_mult,
        "score":            round(score, 6),
        "intended_bet":     round(adj_bet, 2),
        "position_limit":   position_limit,
        "hours_to_resolution": market.get("_hours_to_resolution"),
    }


def check_reentry(
    signal: dict,
    open_trades: list[dict],
    bankroll: float,
    max_positions: int = 3,
) -> Optional[dict]:
    """
    Check if a market with an existing position qualifies for re-entry.

    Conditions:
    - Existing open position on this ticker
    - Current edge ratio > 2x the entry edge ratio
    - Bankroll allows
    - Under position limit
    - Add-on capped at 1.5x original bet size
    """
    ticker = signal.get("ticker", "")
    matching = [t for t in open_trades if t.get("ticker") == ticker]
    if not matching:
        return None

    trade = matching[0]
    entry_true_prob = trade.get("true_prob", 0)
    entry_yes_price = trade.get("yes_price", 0)
    original_bet    = trade.get("bet_size", 0)

    if entry_yes_price <= 0:
        return None

    entry_ratio   = entry_true_prob / entry_yes_price
    current_ratio = signal.get("ratio", 0)

    if current_ratio <= entry_ratio * 2.0:
        return None

    # Cap add-on at 1.5x original bet
    addon_bet = min(original_bet * 1.5, bankroll * 0.05)
    if addon_bet < 1.0:
        return None

    addon_signal = dict(signal)
    addon_signal.update({
        "type":         "add-on",
        "intended_bet": round(addon_bet, 2),
        "note":         f"Re-entry: original ratio={entry_ratio:.2f}x, "
                        f"new ratio={current_ratio:.2f}x "
                        f"(>{entry_ratio * 2:.2f}x threshold)",
    })

    _log.info(f"Re-entry signal: {ticker} | original={entry_ratio:.2f}x → "
              f"current={current_ratio:.2f}x | add-on=${addon_bet:.2f}")

    return addon_signal


def score_all(
    markets: list[dict],
    true_probs: dict[str, float],
    intended_bet: float = 5.0,
    ensemble_stds: dict[str, float] = None,
) -> list[dict]:
    """
    Score a batch of aligned markets for both YES and NO sides.

    Args:
        markets:        List of aligned market dicts
        true_probs:     Dict mapping ticker → true_prob from ensemble
        intended_bet:   Default bet size for position limit checks
        ensemble_stds:  Dict mapping ticker → ensemble std dev (°F) for
                        spread confidence penalty. If None, no penalty applied.

    Returns:
        Sorted list of signal dicts (best score first), including NO signals
    """
    signals = []
    no_evaluated = 0
    best_no_ratio = 0.0
    no_qualified = 0
    if ensemble_stds is None:
        ensemble_stds = {}

    for m in markets:
        ticker = m.get("ticker", "")
        tp = true_probs.get(ticker)
        if tp is None:
            _log.debug(f"No true_prob for {ticker} — skipping")
            continue

        std = ensemble_stds.get(ticker)

        # YES side
        sig = score_market(m, tp, intended_bet=intended_bet, ensemble_std=std)
        if sig:
            sig["side"] = "YES"
            signals.append(sig)

        # NO side — evaluate and track debug stats
        yes_price = m.get("_yes_price", 0)
        if yes_price > 0 and tp is not None:
            no_evaluated += 1
            no_price = 1.0 - yes_price
            no_true_prob = 1.0 - tp
            if no_true_prob > 0:
                this_no_ratio = no_price / no_true_prob
                best_no_ratio = max(best_no_ratio, this_no_ratio)

        no_sig = score_market_no_side(m, tp, intended_bet=intended_bet, ensemble_std=std)
        if no_sig:
            no_qualified += 1
            signals.append(no_sig)

    # NO-side debug logging
    _log.info(f"NO-side evaluation: {no_evaluated} markets checked, "
              f"best ratio {best_no_ratio:.2f}x, {no_qualified} qualified")

    signals.sort(key=lambda s: -s["score"])
    yes_count = sum(1 for s in signals if s.get("side") == "YES")
    no_count  = sum(1 for s in signals if s.get("side") == "NO")
    _log.info(f"Edge scorer: {len(signals)} signals ({yes_count} YES, {no_count} NO) "
              f"from {len(markets)} markets")
    return signals
