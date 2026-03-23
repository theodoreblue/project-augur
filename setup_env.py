"""
setup_env.py — Creates a correctly formatted .env file for Project AUGUR.
Run this once: python3 setup_env.py
Then edit .env to fill in your KALSHI_KEY_ID value.
"""
import os

key_id = input("Paste your Kalshi Key ID (the UUID): ").strip()
key_file = os.path.expanduser("~/augur/kalshiprivatekey.pem")
bankroll = input("Starting bankroll in USD (e.g. 200.0): ").strip() or "200.0"

env_content = f"""KALSHI_KEY_ID={key_id}
KALSHI_PRIVATE_KEY_PATH={key_file}
KALSHI_USE_DEMO=false
STARTING_BANKROLL={bankroll}
"""

env_path = os.path.expanduser("~/augur/.env")
with open(env_path, "w") as f:
    f.write(env_content)

print(f"\n.env file created at {env_path}")
print("Contents:")
print(env_content)
