#!/bin/bash
# watchdog.sh — Project AUGUR crash recovery
# Runs every 5 minutes via cron.
# If run_augur.py is not running, relaunches it in live mode.

LOG="$HOME/augur/logs/cron.log"
AUGUR_DIR="$HOME/augur"
PYTHON="python3"
SCRIPT="run_augur.py"

mkdir -p "$HOME/augur/logs"

# Check if the bot is already running
if pgrep -f "run_augur.py" > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] AUGUR running — no action needed" >> "$LOG"
    exit 0
fi

# Bot is not running — restart it
echo "[$(date '+%Y-%m-%d %H:%M:%S')] AUGUR not running — restarting..." >> "$LOG"
cd "$AUGUR_DIR" || exit 1
nohup $PYTHON $SCRIPT --live >> "$LOG" 2>&1 &
echo "[$(date '+%Y-%m-%d %H:%M:%S')] AUGUR relaunched (PID $!)" >> "$LOG"
