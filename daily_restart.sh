#!/bin/bash
# daily_restart.sh — Project AUGUR daily clean restart at 6am
# Kills current bot, pulls latest code from GitHub, relaunches fresh.
# This means overnight fixes from Theodore are live every morning automatically.

LOG="$HOME/augur/logs/cron.log"
AUGUR_DIR="$HOME/augur"
PYTHON="python3"
SCRIPT="run_augur.py"

mkdir -p "$HOME/augur/logs"

echo "" >> "$LOG"
echo "========================================" >> "$LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] DAILY RESTART — 6am clean relaunch" >> "$LOG"
echo "========================================" >> "$LOG"

# Step 1: Kill any running bot process
if pgrep -f "run_augur.py" > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing existing AUGUR process..." >> "$LOG"
    pkill -f "run_augur.py"
    sleep 3
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] No existing process to kill" >> "$LOG"
fi

# Step 2: Pull latest code from GitHub
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running git pull..." >> "$LOG"
cd "$AUGUR_DIR" || exit 1
git pull origin master >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] git pull complete" >> "$LOG"

# Step 3: Relaunch bot fresh in live mode
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Relaunching AUGUR in live mode..." >> "$LOG"
nohup $PYTHON $SCRIPT --live >> "$LOG" 2>&1 &
echo "[$(date '+%Y-%m-%d %H:%M:%S')] AUGUR relaunched fresh (PID $!)" >> "$LOG"
echo "========================================" >> "$LOG"
