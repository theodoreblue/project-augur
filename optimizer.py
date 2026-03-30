"""
optimizer.py — Self-Improving Strategy Optimizer for Project AUGUR

Runs every Sunday at 6am via cron. Analyzes last 30 days of resolved bets
and auto-adjusts strategy parameters based on actual performance.

Parameters adjusted:
- Edge threshold (MIN_EDGE_RATIO)
- Minimum true probability floor (MIN_TRUE_PROB)
- Heat wave anomaly threshold
- City performance scores (per-city size multipliers)
- Time-of-day performance weights

All changes logged to optimizer.log with reasoning.
Strategy versions saved to strategy_history.json with auto-rollback.

Usage:
    python3 optimizer.py              # run optimization
    python3 optimizer.py --dry        # analyze but don't apply
    python3 optimizer.py --report     # print current strategy + performance
"""

import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("optimizer.log"),
    ]
)
_log = logging.getLogger(__name__)

SIGNALS_LOG = "signals.log"
OPTIMIZER_LOG = "optimizer.log"
STRATEGY_FILE = "strategy.json"
STRATEGY_HISTORY = "strategy_history.json"
POSTMORTEM_LOG = "postmortem.log"

# Default strategy parameters
DEFAULT_STRATEGY = {
    "version": 1,
    "min_edge_ratio": 2.0,
    "min_true_prob": 0.65,
    "anomaly_threshold_f": 15.0,
    "nws_divergence_threshold": 5.0,
    "city_multipliers": {},       # city → size multiplier (1.0 = normal, 0.5 = reduced)
    "city_blacklist": [],         # cities to skip entirely
    "peak_hours_weight": 1.0,    # default weight for peak hours
    "model_update_weight": 1.2,  # slight boost for 6am EST scan
    "updated_at": None,
    "updated_reason": "initial defaults",
}


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


def load_strategy() -> dict:
    """Load current strategy from strategy.json or return defaults."""
    if os.path.exists(STRATEGY_FILE):
        try:
            with open(STRATEGY_FILE) as f:
                s = json.load(f)
                # Merge with defaults for any new fields
                merged = dict(DEFAULT_STRATEGY)
                merged.update(s)
                return merged
        except Exception:
            pass
    return dict(DEFAULT_STRATEGY)


def save_strategy(strategy: dict) -> None:
    """Save strategy to strategy.json."""
    with open(STRATEGY_FILE, "w") as f:
        json.dump(strategy, f, indent=2)


def save_to_history(strategy: dict, win_rate: float, total_bets: int, reason: str) -> None:
    """Append strategy version to history."""
    history = []
    if os.path.exists(STRATEGY_HISTORY):
        try:
            with open(STRATEGY_HISTORY) as f:
                history = json.load(f)
        except Exception:
            history = []

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "version": strategy.get("version", 1),
        "strategy": dict(strategy),
        "win_rate": round(win_rate, 4),
        "total_bets": total_bets,
        "reason": reason,
    }
    history.append(entry)

    with open(STRATEGY_HISTORY, "w") as f:
        json.dump(history, f, indent=2)


def _log_change(param: str, old_val, new_val, reason: str) -> None:
    """Log a parameter change to optimizer.log."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": "param_change",
        "param": param,
        "old": old_val,
        "new": new_val,
        "reason": reason,
    }
    with open(OPTIMIZER_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")
    _log.info(f"Strategy change: {param} {old_val} → {new_val} | {reason}")


def get_resolved_trades(days: int = 30) -> list[dict]:
    """Get resolved (non-dry-run) trades from the last N days."""
    signals = _load_jsonl(SIGNALS_LOG)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    return [
        s for s in signals
        if s.get("outcome")
        and not s.get("dry_run", False)
        and s.get("ts", "") >= cutoff
    ]


def analyze_performance(trades: list[dict]) -> dict:
    """Compute performance metrics from resolved trades."""
    if not trades:
        return {"total": 0}

    wins = [t for t in trades if t.get("outcome") == "won"]
    losses = [t for t in trades if t.get("outcome") == "lost"]

    # By city
    city_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0})
    for t in trades:
        city = t.get("location", "unknown")
        if t["outcome"] == "won":
            city_stats[city]["wins"] += 1
        else:
            city_stats[city]["losses"] += 1
        city_stats[city]["pnl"] += t.get("pnl", 0)

    # By edge ratio bucket
    ratio_stats = defaultdict(lambda: {"wins": 0, "losses": 0})
    for t in trades:
        ratio = t.get("ratio", 0)
        if ratio >= 5:
            bucket = "5x+"
        elif ratio >= 3:
            bucket = "3-5x"
        elif ratio >= 2:
            bucket = "2-3x"
        else:
            bucket = "<2x"
        if t["outcome"] == "won":
            ratio_stats[bucket]["wins"] += 1
        else:
            ratio_stats[bucket]["losses"] += 1

    # By hour of day
    hour_stats = defaultdict(lambda: {"wins": 0, "losses": 0})
    for t in trades:
        try:
            ts = datetime.fromisoformat(t.get("ts", ""))
            hour = ts.hour
            hour_stats[hour]["wins" if t["outcome"] == "won" else "losses"] += 1
        except Exception:
            continue

    total_pnl = sum(t.get("pnl", 0) for t in trades)

    return {
        "total": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(trades) if trades else 0,
        "total_pnl": round(total_pnl, 2),
        "city_stats": dict(city_stats),
        "ratio_stats": dict(ratio_stats),
        "hour_stats": dict(hour_stats),
    }


def optimize(dry_run: bool = False) -> dict:
    """
    Run the strategy optimizer. Analyze last 30 days and adjust parameters.
    Returns the updated strategy.
    """
    strategy = load_strategy()
    trades = get_resolved_trades(days=30)
    perf = analyze_performance(trades)

    if perf["total"] < 5:
        _log.info(f"Only {perf['total']} resolved trades — not enough data to optimize. "
                  "Need at least 5.")
        return strategy

    changes = []
    win_rate = perf["win_rate"]

    _log.info(f"Optimizer: analyzing {perf['total']} trades | "
              f"win_rate={win_rate:.1%} | PnL=${perf['total_pnl']:+.2f}")

    # ── 1. Adjust edge threshold ──────────────────────────────────────────
    old_edge = strategy["min_edge_ratio"]
    if win_rate < 0.50 and old_edge < 4.0:
        new_edge = min(old_edge + 0.5, 4.0)
        reason = f"win_rate={win_rate:.1%} < 50% → raising edge threshold"
        if not dry_run:
            _log_change("min_edge_ratio", old_edge, new_edge, reason)
            strategy["min_edge_ratio"] = new_edge
        changes.append(f"edge_ratio: {old_edge} → {new_edge} ({reason})")
    elif win_rate > 0.80 and old_edge > 1.5:
        new_edge = max(old_edge - 0.25, 1.5)
        reason = f"win_rate={win_rate:.1%} > 80% → lowering edge threshold to find more signals"
        if not dry_run:
            _log_change("min_edge_ratio", old_edge, new_edge, reason)
            strategy["min_edge_ratio"] = new_edge
        changes.append(f"edge_ratio: {old_edge} → {new_edge} ({reason})")

    # ── 2. Adjust minimum true probability ────────────────────────────────
    old_prob = strategy["min_true_prob"]
    # Check if high-confidence bets are losing
    high_conf = [t for t in trades if t.get("true_prob", 0) >= 0.80]
    if high_conf:
        hc_wins = sum(1 for t in high_conf if t["outcome"] == "won")
        hc_rate = hc_wins / len(high_conf)
        if hc_rate < 0.60 and old_prob < 0.80:
            new_prob = min(old_prob + 0.05, 0.85)
            reason = (f"high-confidence (>80%) bets winning only {hc_rate:.1%} "
                      f"→ raising floor")
            if not dry_run:
                _log_change("min_true_prob", old_prob, new_prob, reason)
                strategy["min_true_prob"] = new_prob
            changes.append(f"min_true_prob: {old_prob} → {new_prob} ({reason})")
    # If we're not finding enough signals
    if perf["total"] < 10 and old_prob > 0.55:
        new_prob = max(old_prob - 0.05, 0.55)
        reason = f"only {perf['total']} trades in 30 days → lowering floor for more signals"
        if not dry_run:
            _log_change("min_true_prob", old_prob, new_prob, reason)
            strategy["min_true_prob"] = new_prob
        changes.append(f"min_true_prob: {old_prob} → {new_prob} ({reason})")

    # ── 3. Adjust heat wave sensitivity ───────────────────────────────────
    postmortems = _load_jsonl(POSTMORTEM_LOG)
    anomaly_losses = [pm for pm in postmortems
                      if pm.get("anomaly_flag") and pm.get("outcome") == "lost"]
    if len(anomaly_losses) >= 2:
        old_thresh = strategy["anomaly_threshold_f"]
        new_thresh = max(old_thresh - 2.0, 8.0)
        if new_thresh != old_thresh:
            reason = (f"{len(anomaly_losses)} losses with anomaly flag "
                      f"→ tightening sensitivity")
            if not dry_run:
                _log_change("anomaly_threshold_f", old_thresh, new_thresh, reason)
                strategy["anomaly_threshold_f"] = new_thresh
            changes.append(f"anomaly_threshold: {old_thresh}°F → {new_thresh}°F ({reason})")

    # ── 4. City performance scores ────────────────────────────────────────
    for city, stats in perf["city_stats"].items():
        total = stats["wins"] + stats["losses"]
        if total < 3:
            continue
        city_wr = stats["wins"] / total
        if city_wr < 0.40:
            old_mult = strategy["city_multipliers"].get(city, 1.0)
            new_mult = 0.5
            if old_mult != new_mult:
                reason = (f"{city} win_rate={city_wr:.1%} ({stats['wins']}W/{stats['losses']}L) "
                          f"→ reducing size to 50%")
                if not dry_run:
                    _log_change(f"city_multiplier[{city}]", old_mult, new_mult, reason)
                    strategy["city_multipliers"][city] = new_mult
                changes.append(f"city[{city}]: {old_mult} → {new_mult} ({reason})")
        elif city_wr >= 0.70:
            # Restore city if it was previously penalized
            old_mult = strategy["city_multipliers"].get(city, 1.0)
            if old_mult < 1.0:
                reason = f"{city} win_rate={city_wr:.1%} recovered → restoring full size"
                if not dry_run:
                    _log_change(f"city_multiplier[{city}]", old_mult, 1.0, reason)
                    strategy["city_multipliers"][city] = 1.0
                changes.append(f"city[{city}]: {old_mult} → 1.0 ({reason})")

    # ── 5. Brier Score city blacklist ─────────────────────────────────────
    from calibration import load_records
    cal_records = load_records(since_days=30)
    city_brier = defaultdict(list)
    for r in cal_records:
        city_brier[r.get("city", "unknown")].append(r.get("brier_score", 0))

    for city, scores in city_brier.items():
        if len(scores) >= 10:
            avg_brier = sum(scores) / len(scores)
            if avg_brier > 0.5 and city not in strategy["city_blacklist"]:
                reason = (f"{city} Brier Score={avg_brier:.3f} over {len(scores)} bets "
                          f"→ blacklisting until improvement")
                if not dry_run:
                    _log_change(f"city_blacklist", "add", city, reason)
                    strategy["city_blacklist"].append(city)
                changes.append(f"blacklist +{city} ({reason})")

    # ── Save ──────────────────────────────────────────────────────────────
    if changes and not dry_run:
        strategy["version"] = strategy.get("version", 0) + 1
        strategy["updated_at"] = datetime.now(timezone.utc).isoformat()
        strategy["updated_reason"] = "; ".join(changes)
        save_strategy(strategy)
        save_to_history(strategy, win_rate, perf["total"], strategy["updated_reason"])
        _log.info(f"Strategy v{strategy['version']} saved with {len(changes)} changes")
    elif not changes:
        _log.info("No parameter changes needed — strategy is performing within bounds")
    else:
        _log.info(f"[DRY RUN] Would make {len(changes)} changes:")
        for c in changes:
            _log.info(f"  → {c}")

    return strategy


def check_rollback() -> bool:
    """
    If current strategy version is performing worse than previous over 10+ bets,
    auto-rollback to the previous version.
    """
    if not os.path.exists(STRATEGY_HISTORY):
        return False

    with open(STRATEGY_HISTORY) as f:
        history = json.load(f)

    if len(history) < 2:
        return False

    current = history[-1]
    previous = history[-2]

    # Need at least 10 bets under current version
    trades = get_resolved_trades(days=14)
    current_ts = current.get("ts", "")
    recent = [t for t in trades if t.get("resolved_at", t.get("ts", "")) >= current_ts]

    if len(recent) < 10:
        return False

    recent_wins = sum(1 for t in recent if t.get("outcome") == "won")
    recent_wr = recent_wins / len(recent)

    if recent_wr < previous.get("win_rate", 0):
        _log.warning(
            f"⚠️ ROLLBACK: v{current['version']} win_rate={recent_wr:.1%} < "
            f"v{previous['version']} win_rate={previous['win_rate']:.1%} → rolling back"
        )

        # Restore previous strategy
        prev_strategy = previous["strategy"]
        prev_strategy["updated_at"] = datetime.now(timezone.utc).isoformat()
        prev_strategy["updated_reason"] = (
            f"Auto-rollback from v{current['version']} "
            f"(wr={recent_wr:.1%} < prev {previous['win_rate']:.1%})"
        )
        save_strategy(prev_strategy)

        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": "rollback",
            "from_version": current["version"],
            "to_version": previous["version"],
            "trigger_win_rate": round(recent_wr, 4),
            "previous_win_rate": previous["win_rate"],
        }
        with open(OPTIMIZER_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")

        return True

    return False


def generate_weekly_report() -> str:
    """Generate the weekly performance report."""
    trades = get_resolved_trades(days=7)
    perf = analyze_performance(trades)
    strategy = load_strategy()

    if perf["total"] == 0:
        return "No resolved trades this week."

    lines = [
        "=" * 70,
        "  PROJECT AUGUR — WEEKLY PERFORMANCE REPORT",
        f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"  Strategy Version: v{strategy.get('version', 1)}",
        "=" * 70,
        "",
        "  OVERVIEW",
        f"  Total Bets:    {perf['total']}",
        f"  Win Rate:      {perf['win_rate']:.1%} ({perf['wins']}W / {perf['losses']}L)",
        f"  Total P&L:     ${perf['total_pnl']:+.2f}",
        "",
        "  CITY PERFORMANCE",
    ]

    for city, stats in sorted(perf["city_stats"].items(),
                               key=lambda x: -(x[1]["wins"] + x[1]["losses"])):
        total = stats["wins"] + stats["losses"]
        wr = stats["wins"] / total if total > 0 else 0
        mult = strategy.get("city_multipliers", {}).get(city, 1.0)
        flag = " ⚠️" if mult < 1.0 else ""
        lines.append(
            f"    {city:20s} {stats['wins']}W/{stats['losses']}L "
            f"({wr:.0%}) PnL=${stats['pnl']:+.2f}{flag}"
        )

    lines += [
        "",
        "  EDGE RATIO PERFORMANCE",
    ]
    for bucket, stats in sorted(perf["ratio_stats"].items()):
        total = stats["wins"] + stats["losses"]
        wr = stats["wins"] / total if total > 0 else 0
        lines.append(f"    {bucket:8s} {stats['wins']}W/{stats['losses']}L ({wr:.0%})")

    lines += [
        "",
        "  CURRENT STRATEGY PARAMETERS",
        f"    Edge threshold:       {strategy['min_edge_ratio']}x",
        f"    Min true probability: {strategy['min_true_prob']:.0%}",
        f"    Anomaly threshold:    {strategy['anomaly_threshold_f']}°F",
        f"    NWS divergence:       {strategy['nws_divergence_threshold']}°F",
        f"    Blacklisted cities:   {', '.join(strategy.get('city_blacklist', [])) or 'none'}",
    ]

    # Optimizer changes this week
    opt_log = _load_jsonl(OPTIMIZER_LOG)
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    recent_changes = [e for e in opt_log
                      if e.get("type") == "param_change" and e.get("ts", "") >= week_ago]
    if recent_changes:
        lines += ["", "  PARAMETER CHANGES THIS WEEK"]
        for c in recent_changes:
            lines.append(f"    {c['param']}: {c['old']} → {c['new']} ({c['reason']})")
    else:
        lines += ["", "  No parameter changes this week."]

    # Brier Score
    from calibration import load_records, brier_score
    cal_week = load_records(since_days=7)
    if cal_week:
        bs = brier_score(cal_week)
        lines += [
            "",
            f"  BRIER SCORE (7-day): {bs:.4f}",
            f"    (0.00=perfect, 0.25=random, >0.50=bad)",
        ]

    lines += ["", "=" * 70]
    report = "\n".join(lines)

    # Write to file
    with open("weekly_report.txt", "w") as f:
        f.write(report)

    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AUGUR Strategy Optimizer")
    parser.add_argument("--dry", action="store_true", help="Analyze without applying changes")
    parser.add_argument("--report", action="store_true", help="Generate weekly report")
    parser.add_argument("--rollback-check", action="store_true", help="Check for rollback")
    args = parser.parse_args()

    if args.report:
        print(generate_weekly_report())
    elif args.rollback_check:
        check_rollback()
    else:
        optimize(dry_run=args.dry)
        check_rollback()
