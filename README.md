# Project AUGUR — Kalshi Weather Bot

Automated weather-based prediction market bot for Kalshi (CFTC-regulated).

## Files

| File | Purpose |
|---|---|
| `run_augur.py` | Main runner — orchestrates everything |
| `kalshi_scanner.py` | Fetches live Kalshi weather markets |
| `market_mapper.py` | Parses market questions → structured fields |
| `weather_ensemble.py` | Open-Meteo ensemble → true probability |
| `liquidity_validator.py` | Passive order book depth check |
| `edge_scorer.py` | Scores edge (true_prob / price >= 2x) |
| `sizing.py` | Kelly Criterion bet sizing with Kalshi caps |
| `kalshi_executor.py` | Places orders via Kalshi REST API |
| `portfolio_manager.py` | Live position count from Kalshi API |
| `calibration.py` | Brier Score tracking post-resolution |

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
