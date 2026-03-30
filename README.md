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

## Log Files

| File | Purpose |
|---|---|
| `augur.log` | Main application log |
| `signals.log` | All trade signals (YES, NO, add-on) |
| `skipped.log` | Markets filtered out (time, size, liquidity) |
| `calibration.log` | Resolved bets + weekly Brier scores |
| `near_miss.log` | Markets passing 2x liquidity but failing 3x |
| `mapping_audit.log` | Every mapped market with threshold, variable, unit conversion |
| `unmatched.log` | Markets that failed question parsing |
| `errors.log` | Order execution errors |

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
