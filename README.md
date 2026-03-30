# Project AUGUR — Kalshi Weather Bot

Automated weather-based prediction market bot for Kalshi (CFTC-regulated).

## Files

| File | Purpose |
|---|---|
| `run_augur.py` | Main runner — orchestrates everything |
| `kalshi_scanner.py` | Fetches live Kalshi weather markets (peak hours + 6am EST scan) |
| `market_mapper.py` | Parses market questions → structured fields + mapping audit |
| `weather_ensemble.py` | Open-Meteo ensemble → true probability |
| `liquidity_validator.py` | Passive order book depth check (2x threshold + near-miss logging) |
| `edge_scorer.py` | Scores edge for YES + NO sides, re-entry detection |
| `sizing.py` | Kelly Criterion bet sizing with Kalshi caps |
| `kalshi_executor.py` | Places YES/NO orders via Kalshi REST API |
| `portfolio_manager.py` | Live position count from Kalshi API |
| `calibration.py` | Brier Score tracking, rolling weekly scores, degradation warnings |

## Self-Improving Strategy Engine

| File | Purpose |
|---|---|
| `resolver.py` | Hourly cron — auto-detects outcomes, writes to signals.log, triggers post-mortems |
| `postmortem.py` | Generates detailed post-mortem for every resolved bet (ensemble vs actual) |
| `optimizer.py` | Weekly Sunday cron — auto-adjusts edge ratio, probability floor, city weights |
| `safety_checks.py` | Heat wave detection, NWS cross-check, momentum circuit breaker |

## Log Files

| File | Purpose |
|---|---|
| `augur.log` | Main application log |
| `signals.log` | All trade signals with outcomes (YES, NO, add-on, won/lost) |
| `skipped.log` | Markets filtered out (time, size, liquidity, low confidence) |
| `calibration.log` | Resolved bets + weekly Brier scores + drift alerts |
| `postmortem.log` | Detailed post-mortem for every resolved bet |
| `optimizer.log` | All strategy parameter changes with reasoning |
| `safety.log` | Heat wave flags, NWS divergence events, momentum alerts |
| `near_miss.log` | Markets passing 2x liquidity but failing 3x |
| `mapping_audit.log` | Every mapped market with threshold, variable, unit conversion |
| `unmatched.log` | Markets that failed question parsing |
| `errors.log` | Order execution errors |
| `strategy.json` | Current strategy parameters (auto-updated by optimizer) |
| `strategy_history.json` | Version history of all strategy changes |
| `weekly_report.txt` | Monday morning performance report |

## Key Features

- **Peak Hours Scanning**: Only runs full scans 12pm–8pm EST. Dedicated 6am EST scan for overnight model updates (highest mispricing window).
- **NO-Side Betting**: Evaluates both sides of every market. If `true_prob < 0.5` and the NO edge ratio ≥ 2x, generates a NO signal.
- **Extended Time Window**: Markets 72–96h out are included but only traded if edge ratio ≥ 5x.
- **Re-Entry**: If an existing position's edge improves to 2x+ the entry edge, adds to the position (capped at 1.5x original bet).
- **Calibration Tracking**: Rolling Brier Score per week, warns on 3 consecutive weeks of degradation.
- **Near-Miss Monitoring**: Logs markets that pass the 2x depth check but would have failed the old 3x threshold.
- **Mapping Audit**: Every market logged with extracted threshold, Open-Meteo variable, and unit conversion for spot-checking.

## Setup

```bash
pip install -r requirements.txt
cp .env.template .env
# Fill in KALSHI_API_KEY in .env
```

## Run

```bash
python3 run_augur.py            # dry-run (default)
python3 run_augur.py --live     # live trading
python3 run_augur.py --once     # one cycle only
python3 run_augur.py --calibrate # check resolutions
```
