"""
kalshi_auth.py — Kalshi RSA Authentication for Project AUGUR

Purpose:
    Generates signed request headers for every Kalshi API call using
    RSA-PSS cryptographic signatures. Kalshi requires this instead of
    simple Bearer tokens.

How it works:
    1. Take current timestamp (milliseconds)
    2. Build message: timestamp + HTTP_METHOD + path (no query string)
    3. Sign with RSA private key using PSS padding + SHA256
    4. Base64-encode the signature
    5. Send in headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP

Kalshi API endpoints used:
    All endpoints — this module provides headers for every request.

Regulatory constraints:
    - Private key loaded from environment variable or file path only
    - Never hardcode credentials
    - Key file should be readable only by the bot user (chmod 600)

Environment variables:
    KALSHI_KEY_ID          — your API Key ID (UUID format)
    KALSHI_PRIVATE_KEY_PATH — path to your RSA private key file
    OR
    KALSHI_PRIVATE_KEY     — the full private key content as a string
"""

import base64
import logging
import os
import time
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger(__name__)


def _load_private_key():
    """
    Load RSA private key from environment variable or file.
    Tries KALSHI_PRIVATE_KEY (raw string) first,
    then KALSHI_PRIVATE_KEY_PATH (file path).
    """
    # Option 1: key content directly in env var
    raw = os.getenv("KALSHI_PRIVATE_KEY", "")
    if raw:
        # Fix escaped newlines if stored as single line
        raw = raw.replace("\\n", "\n")
        if not raw.strip().startswith("-----"):
            raise ValueError("KALSHI_PRIVATE_KEY does not look like a valid PEM key")
        key_bytes = raw.encode()
    else:
        # Option 2: path to key file
        path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
        if not path:
            raise ValueError(
                "Set KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH in your .env file"
            )
        with open(path, "rb") as f:
            key_bytes = f.read()

    return serialization.load_pem_private_key(
        key_bytes,
        password=None,
        backend=default_backend(),
    )


def get_auth_headers(method: str, path: str) -> dict:
    """
    Generate Kalshi RSA authentication headers for one API request.

    Args:
        method: HTTP method uppercase ("GET", "POST", etc.)
        path:   API path WITHOUT query string (e.g. "/trade-api/v2/markets")

    Returns:
        Dict of headers to merge into the request.
    """
    key_id = os.getenv("KALSHI_KEY_ID", "")
    if not key_id:
        raise ValueError("KALSHI_KEY_ID not set in environment")

    # Timestamp in milliseconds
    ts_ms = str(int(time.time() * 1000))

    # Message to sign: timestamp + method + path
    message = ts_ms + method.upper() + path

    # Sign with RSA-PSS, SHA256
    private_key = _load_private_key()
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    sig_b64 = base64.b64encode(signature).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY":       key_id,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "Content-Type":            "application/json",
    }


def get_headers(method: str, path: str) -> dict:
    """Alias for get_auth_headers — shorter name for internal use."""
    return get_auth_headers(method, path)


if __name__ == "__main__":
    # Quick test — prints headers without making any API call
    try:
        headers = get_auth_headers("GET", "/trade-api/v2/markets")
        print("Auth headers generated successfully:")
        for k, v in headers.items():
            if k == "KALSHI-ACCESS-SIGNATURE":
                print(f"  {k}: {v[:20]}...{v[-10:]}")
            else:
                print(f"  {k}: {v}")
    except Exception as e:
        print(f"Auth setup error: {e}")
        print("Make sure KALSHI_KEY_ID and KALSHI_PRIVATE_KEY (or KALSHI_PRIVATE_KEY_PATH) are set in .env")
