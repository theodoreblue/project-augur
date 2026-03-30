"""
dashboard.py — AUGUR Live Trading Dashboard

Flask web dashboard for monitoring Project AUGUR trades, P&L, and live Kalshi balance.
Runs on http://localhost:5000 with 60-second auto-refresh.

Usage:
    python3 dashboard.py              # start dashboard
    python3 dashboard.py --port 8080  # custom port
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Optional

from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv

load_dotenv()

# Import AUGUR auth for Kalshi API calls
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger(__name__)

SIGNALS_LOG      = os.path.join(os.path.dirname(__file__), "signals.log")
CALIBRATION_LOG  = os.path.join(os.path.dirname(__file__), "calibration.log")
STATE_FILE       = os.path.join(os.path.dirname(__file__), "augur_state.json")
STRATEGY_FILE    = os.path.join(os.path.dirname(__file__), "strategy.json")
POSTMORTEM_LOG   = os.path.join(os.path.dirname(__file__), "postmortem.log")
AUGUR_DIR        = os.path.dirname(os.path.abspath(__file__))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_signals() -> list[dict]:
    """Load all entries from signals.log."""
    if not os.path.exists(SIGNALS_LOG):
        return []
    records = []
    with open(SIGNALS_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _load_calibration() -> list[dict]:
    """Load resolved bet records from calibration.log."""
    if not os.path.exists(CALIBRATION_LOG):
        return []
    records = []
    with open(CALIBRATION_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                if r.get("type") == "weekly_brier":
                    continue  # skip weekly summaries
                records.append(r)
            except json.JSONDecodeError:
                continue
    return records


def _load_state() -> dict:
    """Load augur_state.json."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _kalshi_balance() -> Optional[float]:
    """Fetch live balance from Kalshi API."""
    try:
        from sizing import get_live_balance
        return get_live_balance()
    except Exception as e:
        _log.warning(f"Failed to fetch Kalshi balance: {e}")
        return None


def _kalshi_positions() -> Optional[int]:
    """Fetch open position count from Kalshi API."""
    try:
        from portfolio_manager import count_open_positions
        return count_open_positions()
    except Exception as e:
        _log.warning(f"Failed to fetch positions: {e}")
        return None


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    signals = _load_signals()
    calibration = _load_calibration()

    total_bets = len(signals)
    yes_bets = sum(1 for s in signals if s.get("side", "YES") == "YES")
    no_bets = sum(1 for s in signals if s.get("side") == "NO")
    dry_runs = sum(1 for s in signals if s.get("dry_run", False))

    # Resolved outcomes from signals.log (resolver.py writes these)
    resolved_signals = [s for s in signals if s.get("outcome")]
    wins = sum(1 for s in resolved_signals if s.get("outcome") == "won")
    losses = sum(1 for s in resolved_signals if s.get("outcome") == "lost")
    total_pnl = sum(s.get("pnl", 0) for s in resolved_signals)

    # Fallback to calibration.log if signals.log has no outcomes yet
    if not resolved_signals and calibration:
        wins = sum(1 for c in calibration if c.get("actual_outcome") is True)
        losses = sum(1 for c in calibration if c.get("actual_outcome") is False)
        total_pnl = sum(c.get("pnl", 0) for c in calibration)

    # Best edge from signals
    best_edge = max((s.get("ratio", 0) for s in signals), default=0)

    # P&L from live balance vs starting bankroll
    starting = float(os.getenv("STARTING_BANKROLL", "50.0"))
    state = _load_state()
    live_pnl = state.get("bankroll", starting) - starting
    # Use the more accurate source
    if resolved_signals:
        display_pnl = total_pnl
    else:
        display_pnl = live_pnl

    resolved_tickers = {s.get("ticker") for s in resolved_signals}
    open_count = sum(1 for s in signals
                     if not s.get("dry_run", False)
                     and not s.get("outcome")
                     and s.get("ticker") not in resolved_tickers)

    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

    return jsonify({
        "total_bets": total_bets,
        "yes_bets": yes_bets,
        "no_bets": no_bets,
        "dry_runs": dry_runs,
        "wins": wins,
        "losses": losses,
        "open_count": open_count,
        "total_pnl": round(display_pnl, 2),
        "win_rate": round(win_rate, 1),
        "best_edge": round(best_edge, 2),
    })


@app.route("/api/trades")
def api_trades():
    signals = _load_signals()
    calibration = _load_calibration()

    # Build resolution lookup
    resolutions = {}
    for c in calibration:
        ticker = c.get("market_id")
        if ticker:
            resolutions[ticker] = {
                "outcome": "won" if c.get("actual_outcome") else "lost",
                "pnl": c.get("pnl", 0),
            }

    trades = []
    for s in reversed(signals):  # newest first
        ticker = s.get("ticker", "")
        status = "open"
        if s.get("dry_run", False):
            status = "dry_run"
        elif ticker in resolutions:
            status = resolutions[ticker]["outcome"]

        trades.append({
            "ts": s.get("ts", ""),
            "ticker": ticker,
            "question": s.get("question", ""),
            "side": s.get("side", "YES"),
            "type": s.get("type", "new"),
            "true_prob": s.get("true_prob", 0),
            "yes_price": s.get("yes_price", 0),
            "edge": s.get("edge", 0),
            "ratio": s.get("ratio", 0),
            "bet_size_usd": s.get("bet_size_usd", s.get("intended_bet", 0)),
            "order_id": s.get("order_id", ""),
            "dry_run": s.get("dry_run", False),
            "status": status,
            "note": s.get("note", ""),
        })

    return jsonify(trades)


@app.route("/api/balance")
def api_balance():
    live = _kalshi_balance()
    state = _load_state()
    return jsonify({
        "live_balance": live,
        "state_balance": state.get("bankroll"),
        "source": "kalshi_api" if live is not None else "state_file",
    })


@app.route("/api/positions")
def api_positions():
    count = _kalshi_positions()
    state = _load_state()
    # Also count unresolved trades from signals.log
    signals = _load_signals()
    live_unresolved = sum(1 for s in signals
                         if not s.get("dry_run", False) and not s.get("outcome"))

    return jsonify({
        "open_positions": count if count is not None else live_unresolved,
        "kalshi_count": count,
        "signals_unresolved": live_unresolved,
        "state_open": len(state.get("open_trades", [])),
        "source": "kalshi_api" if count is not None else "signals_log",
    })


@app.route("/api/strategy")
def api_strategy():
    """Return current strategy version and parameters."""
    if os.path.exists(STRATEGY_FILE):
        try:
            with open(STRATEGY_FILE) as f:
                return jsonify(json.load(f))
        except Exception:
            pass
    return jsonify({"version": 1, "status": "default"})


@app.route("/api/city-performance")
def api_city_performance():
    """Return win rate and P&L per city."""
    calibration = _load_calibration()
    cities: dict[str, dict] = {}
    for c in calibration:
        city = c.get("city", "unknown")
        if city not in cities:
            cities[city] = {"wins": 0, "losses": 0, "pnl": 0, "brier_sum": 0, "count": 0}
        if c.get("actual_outcome") is True:
            cities[city]["wins"] += 1
        elif c.get("actual_outcome") is False:
            cities[city]["losses"] += 1
        cities[city]["pnl"] += c.get("pnl", 0)
        cities[city]["brier_sum"] += c.get("brier_score", 0)
        cities[city]["count"] += 1

    result = []
    for city, stats in sorted(cities.items()):
        total = stats["wins"] + stats["losses"]
        result.append({
            "city": city,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_rate": round(stats["wins"] / total * 100, 1) if total > 0 else 0,
            "pnl": round(stats["pnl"], 2),
            "avg_brier": round(stats["brier_sum"] / stats["count"], 4) if stats["count"] > 0 else 0,
        })
    return jsonify(result)


@app.route("/api/postmortems")
def api_postmortems():
    """Return the last 10 post-mortem entries."""
    if not os.path.exists(POSTMORTEM_LOG):
        return jsonify([])
    records = []
    with open(POSTMORTEM_LOG) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return jsonify(records[-10:])


@app.route("/api/brier-trend")
def api_brier_trend():
    """Return Brier Score trend by week."""
    calibration = _load_calibration()
    if not calibration:
        return jsonify([])

    weekly: dict[str, list[float]] = {}
    for c in calibration:
        try:
            ts = datetime.fromisoformat(c["ts"])
            week = f"{ts.isocalendar()[0]}-W{ts.isocalendar()[1]:02d}"
            weekly.setdefault(week, []).append(c.get("brier_score", 0))
        except Exception:
            continue

    trend = []
    for week in sorted(weekly.keys()):
        scores = weekly[week]
        trend.append({
            "week": week,
            "brier_score": round(sum(scores) / len(scores), 4),
            "n_bets": len(scores),
        })
    return jsonify(trend)


@app.route("/api/run", methods=["POST"])
def api_run():
    """Trigger a scan cycle as a subprocess."""
    mode = request.json.get("mode", "dry") if request.is_json else "dry"
    cmd = [sys.executable, os.path.join(AUGUR_DIR, "run_augur.py"), "--once"]
    if mode == "live":
        cmd.append("--live")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=AUGUR_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output, _ = proc.communicate(timeout=120)
        return jsonify({
            "status": "completed",
            "mode": mode,
            "return_code": proc.returncode,
            "output": output[-2000:] if output else "",
        })
    except subprocess.TimeoutExpired:
        proc.kill()
        return jsonify({"status": "timeout", "mode": mode}), 504
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AUGUR Dashboard")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    print(f"\n  AUGUR Dashboard: http://localhost:{args.port}\n")
    app.run(host=args.host, port=args.port, debug=False)
