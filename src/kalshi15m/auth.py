from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

WS_PATH = "/trade-api/ws/v2"


def ws_auth_headers() -> dict[str, str] | None:
    """Load explicitly namespaced market-data credentials from the environment."""
    key_id = os.environ.get("KALSHI15M_MARKETDATA_API_KEY_ID")
    key_path = os.environ.get("KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH")
    if not key_id or not key_path:
        return None
    private_path = Path(key_path)
    if not private_path.is_file():
        raise RuntimeError("KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH does not refer to a readable file")
    key = serialization.load_pem_private_key(private_path.read_bytes(), password=None)
    timestamp = str(int(time.time() * 1000))
    message = (timestamp + "GET" + WS_PATH).encode("utf-8")
    if isinstance(key, Ed25519PrivateKey):
        signature = key.sign(message)
    elif isinstance(key, rsa.RSAPrivateKey):
        signature = key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    else:
        raise RuntimeError("Unsupported Kalshi signing key type")
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }
