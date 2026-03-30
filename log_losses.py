"""
log_losses.py — Manual calibration entries for AUGUR's first two live bets.
Both lost. Run once to seed calibration.log.

Usage: python3 log_losses.py
"""

from calibration import log_resolution

# Loss 1: Miami — 97.4% confidence, settled NO
log_resolution(
    market_id="KXHIGHTMIA-LOSS1",
    question="Miami high temperature market (first live bet)",
    predicted_prob=0.974,
    actual_outcome=False,
    market_price=0.50,       # placeholder — update with actual yes_price
    payout_multiplier=2.0,   # placeholder — update with actual
    bet_size=5.0,            # placeholder — update with actual
    pnl=-5.0,               # placeholder — update with actual bet size
    city="Miami",
    date="2026-03-30",
)

# Loss 2: Denver — 89.7% confidence, settled NO
log_resolution(
    market_id="KXHIGHTDEN-LOSS2",
    question="Denver high temperature market (second live bet)",
    predicted_prob=0.897,
    actual_outcome=False,
    market_price=0.50,       # placeholder — update with actual yes_price
    payout_multiplier=2.0,   # placeholder — update with actual
    bet_size=5.0,            # placeholder — update with actual
    pnl=-5.0,               # placeholder — update with actual bet size
    city="Denver",
    date="2026-03-30",
)

print("Logged 2 losses to calibration.log")
print()
print("⚠️  UPDATE THE PLACEHOLDER VALUES with actual trade data:")
print("    - market_price (yes_price paid)")
print("    - payout_multiplier")
print("    - bet_size")
print("    - pnl (should be -bet_size for a loss)")
print("    - exact ticker")
print("    - exact market question")
print()
print("Brier scores:")
print(f"  Miami:  (0.974 - 0)² = {(0.974)**2:.4f}  ← terrible, model was 97% confident and WRONG")
print(f"  Denver: (0.897 - 0)² = {(0.897)**2:.4f}  ← very bad, model was 90% confident and WRONG")
print(f"  Average Brier: {((0.974**2) + (0.897**2))/2:.4f}  (0.25 = random, >0.5 = worse than coin flip)")
print()
print("This is a CALIBRATION CRISIS — the model is systematically overconfident.")
print("Likely cause: ensemble bracket probability doesn't match Kalshi's exact threshold definition.")
