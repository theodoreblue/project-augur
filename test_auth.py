"""
test_auth.py — Tests Kalshi RSA auth against a simple public endpoint.
Run: python3 test_auth.py
"""
import base64
import os
import time
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

load_dotenv()

BASE = "https://api.elections.kalshi.com/trade-api/v2"

def load_key():
    path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
    with open(os.path.expanduser(path), "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())

def sign(method, path, ts_ms, private_key):
    message = ts_ms + method.upper() + path
    sig = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")

def make_headers(method, path):
    key_id = os.getenv("KALSHI_KEY_ID", "")
    ts = str(int(time.time() * 1000))
    private_key = load_key()
    sig = sign(method, path, ts, private_key)
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }

# Test 1: GET /portfolio/balance
path = "/trade-api/v2/portfolio/balance"
headers = make_headers("GET", path)
print(f"Key ID: {headers['KALSHI-ACCESS-KEY']}")
print(f"Timestamp: {headers['KALSHI-ACCESS-TIMESTAMP']}")
print(f"Sig (first 20): {headers['KALSHI-ACCESS-SIGNATURE'][:20]}...")

resp = requests.get(BASE + "/portfolio/balance", headers=headers)
print(f"\nGET /portfolio/balance → {resp.status_code}")
print(resp.text[:500])

# Test 2: GET /markets (public, less auth-sensitive)
path2 = "/trade-api/v2/markets"
headers2 = make_headers("GET", path2)
resp2 = requests.get(BASE + "/markets", headers=headers2, params={"limit": 1})
print(f"\nGET /markets → {resp2.status_code}")
print(resp2.text[:300])
