"""
run_augur.py — Project AUGUR Main Runner

Purpose:
    Orchestrates the full 10-minute scan → score → size → execute pipeline.
    Pulls live portfolio state from Kalshi API every cycle (no stale state).

Pipeline each cycle:
    1. Check portfolio cap (live API) → skip if 3+ open
    2. Scan Kalshi weather markets (time + contract size filters)
    3. Align markets (market_mapper) → drop unparseable
    4. Validate liquidity (passive order book read)
    5. Run Open-Meteo ensemble → true probability
    6. Score edge (true_prob >= 2x yes_price)
    7. Size bet (Kelly capped at 5%, position limit, live balance)
    8. Execute order (retry once on fail)
    9. Log everything

Usage:
    python run_augur.py               # dry-run (scan + size, no real orders)
    python run_augur.py --live        # live trading (real money)
    python run_augur.py --once        # one cycle and exit
    python run_augur.py --calibrate   # check open trades for resolutions

Environment variables (all required for --live):
    KALSHI_API_KEY      — Kalshi API key
    KALSHI_USE_DEMO     — "true" for demo API (optional, default false)
    STARTING_BANKROLL   — starting USD balance (optional, default 50.0)

Regulatory constraints:
    - KYC-verified Kalshi account required for live trading
    - All credentials from environment variables — never hardcoded
    - Passive-only market reads; no spoofing or wash trading
    - Position limits enforced via live API check every cycle
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from kalshi_scanner      import scan_markets, is_peak_hours, is_model_update_window
from market_mapper       import align_markets
from liquidity_validator import validate_batch
from weather_ensemble    import fetch_ensemble, bracket_probability, ensemble_stats
from edge_scorer         import score_all, check_reentry
from sizing              import size_bet, get_live_balance
from kalshi_executor     import place_order
from portfolio_manager   import available_slots
from calibration         import (check_market_resolution, log_resolution,
                                 daily_calibration_cycle, rolling_brier_score)
from safety_checks       import check_heat_wave, check_nws_divergence, check_market_momentum

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("augur.log"),
    ]
)
_log = logging.getLogger(__name__)

SCAN_INTERVAL_SEC = 600    # 10 minutes
STATE_FILE        = "augur_state.json"
SIGNALS_LOG       = "signals.log"


# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    defaults = {
        "bankroll":       float(os.getenv("STARTING_BANKROLL", "50.0")),
        "open_trades":    [],
        "total_bets":     0,
        "total_pnl":      0.0,
        "cycles_run":     0,
    }
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                saved = json.load(f)
                defaults.update(saved)
        except Exception:
            pass
    return defaults


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Ensemble probability for a market ─────────────────────────────────────────

def compute_true_prob(market: dict) -> tuple[float, list[float]]:
    """
    Fetch Open-Meteo ensemble for this market's city/date and compute
    true probability for the market's specific threshold.

    Returns:
        (probability, ensemble_members) — members list for safety checks
    """
    city = market.get("location")
    if not city:
        return 0.0, []

    try:
        ensemble = fetch_ensemble(city)
    except Exception as e:
        _log.warning(f"Ensemble fetch failed for {city}: {e}")
        return 0.0, []

    # Get member max temps for the resolution date
    res_date = (market.get("resolution_dt") or "")[:10]
    members  = ensemble.get("dates", {}).get(res_date, [])
    if not members:
        # Try nearest available date
        dates = sorted(ensemble.get("dates", {}).keys())
        if dates:
            members = ensemble["dates"][dates[0]]

    if not members:
        return 0.0, []

    metric = market.get("metric", "temp")
    ttype  = market.get("threshold_type", "bracket")
    lo     = market.get("bracket_low")
    hi     = market.get("bracket_high")

    prob = 0.0
    if metric == "temp":
        if ttype == "bracket":
            prob = bracket_probability(members, lo, hi)
        elif ttype == "upper":
            prob = bracket_probability(members, lo, None)
        elif ttype == "lower":
            prob = bracket_probability(members, None, hi)
    else:
        _log.debug(f"Metric '{metric}' not fully modeled — skipping probability calc")

    return prob, members


# ── Calibration check ─────────────────────────────────────────────────────────

def check_resolutions(state: dict) -> dict:
    """Check all open trades for resolution and log calibration data."""
    still_open = []
    for trade in state.get("open_trades", []):
        ticker  = trade.get("ticker")
        outcome = check_market_resolution(ticker)
        if outcome is None:
            still_open.append(trade)
            continue

        # Resolved — log calibration
        pnl = (trade["bet_size"] * trade["payout_multiplier"] - trade["bet_size"]
                if outcome else -trade["bet_size"])
        log_resolution(
            market_id       = ticker,
            question        = trade["question"],
            predicted_prob  = trade["true_prob"],
            actual_outcome  = outcome,
            market_price    = trade["yes_price"],
            payout_multiplier = trade["payout_multiplier"],
            bet_size        = trade["bet_size"],
            pnl             = pnl,
            city            = trade.get("location", ""),
            date            = trade.get("date", ""),
        )
        state["total_pnl"] += pnl
        state["bankroll"]  += (trade["bet_size"] * trade["payout_multiplier"]
                                if outcome else 0)
        _log.info(f"Trade resolved: {trade['question'][:50]} | "
                  f"{'WIN' if outcome else 'LOSS'} | PnL=${pnl:+.2f}")

    state["open_trades"] = still_open
    return state


# ── Main scan cycle ───────────────────────────────────────────────────────────

def _log_signal(signal: dict) -> None:
    """Write signal to signals.log."""
    with open(SIGNALS_LOG, "a") as f:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": signal.get("ticker"),
            "side": signal.get("side", "YES"),
            "type": signal.get("type", "new"),
            "question": signal.get("question"),
            "true_prob": signal.get("true_prob"),
            "yes_price": signal.get("yes_price"),
            "edge": signal.get("edge"),
            "ratio": signal.get("ratio"),
            "intended_bet": signal.get("intended_bet"),
            "note": signal.get("note"),
        }
        f.write(json.dumps(entry) + "\n")


def run_cycle(state: dict, dry_run: bool = True) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    state["cycles_run"] += 1

    # Step 0: Sync bankroll from live Kalshi balance
    live_bal = get_live_balance()
    if live_bal is not None:
        if abs(live_bal - state["bankroll"]) > 0.01:
            _log.info(f"Bankroll sync: local=${state['bankroll']:.2f} → live=${live_bal:.2f}")
        state["bankroll"] = live_bal
        save_state(state)
    else:
        _log.warning("Could not fetch live balance — using local state")

    _log.info(f"=== AUGUR Cycle #{state['cycles_run']} | {now} | "
              f"bankroll=${state['bankroll']:.2f} ===")

    # Step 1: Check resolutions on open trades (always, even off-peak)
    state = check_resolutions(state)

    # Step 1b: Daily calibration at 6am EST (11:00 UTC) window
    if is_model_update_window():
        _log.info("6am EST model update window — running daily calibration cycle")
        state["open_trades"] = daily_calibration_cycle(state["open_trades"])
        save_state(state)

    # Step 2: Peak hours check
    if not is_peak_hours():
        _log.info("Off-peak hours, skipping scan.")
        return state

    # Step 3: Portfolio cap — live API check
    slots = available_slots()
    if slots == 0:
        _log.info("Portfolio cap reached. Skipping scan this cycle.")
        return state

    # Step 4: Scan Kalshi (peak check already passed)
    markets = scan_markets(bankroll=state["bankroll"], skip_peak_check=True)
    if not markets:
        _log.info("No markets from scanner this cycle.")
        return state

    # Step 5: Align markets (parse questions)
    aligned = align_markets(markets)
    if not aligned:
        _log.info("No aligned markets after parsing.")
        return state

    # Step 6: Liquidity validation (passive order book read)
    default_bet = max(1.0, state["bankroll"] * 0.02)
    liquid = validate_batch(aligned, intended_bet=default_bet)
    if not liquid:
        _log.info("No markets passed liquidity check.")
        return state

    # Step 7: Compute true probabilities + safety checks
    true_probs: dict[str, float] = {}
    size_multipliers: dict[str, float] = {}  # safety-adjusted sizing
    safety_skips: set[str] = set()

    for m in liquid:
        ticker = m.get("ticker", "")
        tp, members = compute_true_prob(m)
        if tp <= 0:
            continue
        true_probs[ticker] = tp

        city = m.get("location", "")
        res_date = (m.get("resolution_dt") or "")[:10]

        # Safety check 1: Heat wave detection
        if members and city:
            hw = check_heat_wave(city, members, res_date)
            size_multipliers[ticker] = hw["size_multiplier"]
            if hw["flagged"]:
                _log.info(f"Heat wave flag on {ticker} — size reduced to {hw['size_multiplier']:.0%}")

        # Safety check 2: NWS cross-check
        lat = m.get("lat", 0)
        lon = m.get("lon", 0)
        if lat and lon and members:
            ens_mean = sum(members) / len(members)
            nws = check_nws_divergence(city, lat, lon, ens_mean, res_date)
            if nws["skip"]:
                safety_skips.add(ticker)
                _log.info(f"NWS divergence skip: {ticker} — "
                          f"ensemble={nws['ensemble_mean']}°F vs NWS={nws['nws_high']}°F")

    # Remove NWS-skipped markets
    if safety_skips:
        liquid = [m for m in liquid if m.get("ticker") not in safety_skips]
        for t in safety_skips:
            true_probs.pop(t, None)
        _log.info(f"Safety: skipped {len(safety_skips)} markets due to NWS divergence")

    # Step 8: Edge scoring (YES + NO sides)
    signals = score_all(liquid, true_probs, intended_bet=default_bet)

    # Step 8b: Filter extended window markets — only keep if edge ratio >= 5x
    filtered_signals = []
    for sig in signals:
        ticker = sig["ticker"]
        # Find the original market to check _extended_window
        orig = next((m for m in liquid if m.get("ticker") == ticker), {})
        if orig.get("_extended_window") and sig.get("ratio", 0) < 5.0:
            _log.info(f"Filtered extended-window market {ticker} "
                      f"(ratio={sig.get('ratio', 0):.1f}x < 5.0x)")
            continue
        filtered_signals.append(sig)
    signals = filtered_signals

    if not signals:
        _log.info("No edge signals found this cycle.")
        return state

    _log.info(f"Found {len(signals)} signals. {slots} slots available.")

    # Log all signals
    for sig in signals:
        _log_signal(sig)

    # Step 9: Size + Execute (up to available slots)
    open_tickers = {t["ticker"] for t in state["open_trades"]}

    for signal in signals[:slots]:
        ticker = signal["ticker"]
        if ticker in open_tickers:
            continue

        side = signal.get("side", "YES")
        true_prob_for_sizing = signal["true_prob"]
        payout_mult = signal["payout_multiplier"]

        # Kelly sizing with all caps
        sizing = size_bet(
            bankroll          = state["bankroll"],
            true_prob         = true_prob_for_sizing if side == "YES" else signal.get("no_true_prob", true_prob_for_sizing),
            payout_multiplier = payout_mult,
            ticker            = ticker,
            fetch_live_balance = not dry_run,
        )
        bet = sizing["bet_size"]

        # Apply safety size multiplier (heat wave reduction)
        safety_mult = size_multipliers.get(ticker, 1.0)
        if safety_mult < 1.0:
            original_bet = bet
            bet = round(bet * safety_mult, 2)
            _log.info(f"Safety sizing: ${original_bet:.2f} → ${bet:.2f} "
                      f"({safety_mult:.0%} of normal, heat wave flag)")
        if bet <= 0:
            _log.info(f"Sizing returned $0 for {ticker} — skip")
            continue

        _log.info(
            f"Signal [{side}]: {signal['question'][:60]}\n"
            f"  true_prob={signal['true_prob']:.1%} price={signal['yes_price']:.4f} "
            f"edge={signal['edge']:.1%} ratio={signal['ratio']:.1f}x "
            f"bet=${bet:.2f} kelly={sizing['kelly_fraction']:.1%}"
        )

        success = place_order(signal, bet, dry_run=dry_run)
        if success:
            state["open_trades"].append({
                "ticker":           ticker,
                "question":         signal["question"],
                "location":         signal.get("location"),
                "date":             signal.get("date"),
                "side":             side,
                "yes_price":        signal["yes_price"],
                "true_prob":        signal["true_prob"],
                "payout_multiplier": payout_mult,
                "bet_size":         bet,
                "entry_ratio":      signal.get("ratio", 0),
                "opened_at":        datetime.now(timezone.utc).isoformat(),
            })
            state["bankroll"]  -= bet
            state["total_bets"] += 1
            open_tickers.add(ticker)
            save_state(state)
            _log.info(f"Position opened [{side}]. Bankroll: ${state['bankroll']:.2f}")

    # Step 10: Re-entry check on existing positions
    for signal in signals:
        addon = check_reentry(
            signal=signal,
            open_trades=state["open_trades"],
            bankroll=state["bankroll"],
        )
        if addon:
            _log_signal(addon)
            addon_sizing = size_bet(
                bankroll=state["bankroll"],
                true_prob=addon["true_prob"],
                payout_multiplier=addon["payout_multiplier"],
                ticker=addon["ticker"],
                fetch_live_balance=not dry_run,
            )
            addon_bet = min(addon_sizing["bet_size"], addon["intended_bet"])
            if addon_bet > 0:
                success = place_order(addon, addon_bet, dry_run=dry_run)
                if success:
                    # Update the existing trade's bet size
                    for t in state["open_trades"]:
                        if t["ticker"] == addon["ticker"]:
                            t["bet_size"] += addon_bet
                            break
                    state["bankroll"] -= addon_bet
                    state["total_bets"] += 1
                    save_state(state)
                    _log.info(f"Add-on position: {addon['ticker']} +${addon_bet:.2f} | "
                              f"{addon.get('note', '')}")

    # Step 11: Market momentum check on all open positions
    for trade in state["open_trades"]:
        momentum = check_market_momentum(
            ticker=trade.get("ticker", ""),
            entry_price=trade.get("yes_price", 0),
            side=trade.get("side", "YES"),
        )
        if momentum.get("flagged"):
            _log.warning(
                f"⚠️ MOMENTUM ALERT: {trade['ticker']} — "
                f"entry={momentum['entry_price']:.4f} → "
                f"current={momentum['current_price']:.4f} "
                f"({momentum['move_pct']:.1%} adverse move)"
            )

    return state


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Project AUGUR — Kalshi Weather Bot")
    parser.add_argument("--live",       action="store_true",
                        help="Enable real order placement (default: dry-run)")
    parser.add_argument("--once",       action="store_true",
                        help="Run one cycle and exit")
    parser.add_argument("--calibrate",  action="store_true",
                        help="Check open trades for resolutions and exit")
    args = parser.parse_args()

    dry_run = not args.live

    if dry_run:
        _log.info("Project AUGUR starting in DRY RUN mode (no real orders)")
    else:
        _log.info("Project AUGUR starting in LIVE mode — real orders will be placed")
        required = ["KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"]
        missing  = [k for k in required if not os.getenv(k)]
        if missing:
            _log.error(f"Missing required env vars: {missing}")
            return

    state = load_state()
    _log.info(f"State: bankroll=${state['bankroll']:.2f} | "
              f"open={len(state['open_trades'])} | cycles={state['cycles_run']}")

    if args.calibrate:
        state = check_resolutions(state)
        save_state(state)
        return

    if args.once:
        state = run_cycle(state, dry_run=dry_run)
        save_state(state)
        return

    # Main loop
    while True:
        try:
            state = run_cycle(state, dry_run=dry_run)
            save_state(state)
        except KeyboardInterrupt:
            _log.info("AUGUR stopped by user.")
            break
        except Exception as e:
            _log.error(f"Unexpected error in cycle: {e}", exc_info=True)

        _log.info(f"Sleeping {SCAN_INTERVAL_SEC // 60} min until next cycle...")
        time.sleep(SCAN_INTERVAL_SEC)


if __name__ == "__main__":
    main()
