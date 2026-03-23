"""
setup_env.py — Creates a correctly formatted .env file for Project AUGUR.
Run this once: python3 setup_env.py
"""
import os

key_id = input("Paste your Kalshi Key ID (the UUID): ").strip()
key_file = input("Path to private key file [/home/jnyka/augur/kalshiprivatekey.pem]: ").strip()
if not key_file:
    key_file = "/home/jnyka/augur/kalshiprivatekey.pem"
bankroll = input("Starting bankroll in USD [200.0]: ").strip() or "200.0"

env_content = f"""KALSHI_KEY_ID={key_id}
KALSHI_PRIVATE_KEY_PATH={key_file}
KALSHI_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
KALSHI_DEMO_BASE_URL=https://demo-api.kalshi.co/trade-api/v2
KALSHI_USE_DEMO=false
STARTING_BANKROLL={bankroll}
"""

env_path = os.path.expanduser("~/augur/.env")
with open(env_path, "w") as f:
    f.write(env_content)

print(f"\n.env written to {env_path}")
print("Contents:")
print(env_content)
