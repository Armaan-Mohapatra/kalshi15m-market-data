"""Signing characterization: throwaway generated keys only; no real credentials."""
import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from kalshi15m import auth


def pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


class SigningTests(unittest.TestCase):
    def headers_for(self, key):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "k.pem"
            path.write_bytes(pem(key))
            env = {"KALSHI15M_MARKETDATA_API_KEY_ID": "test-key-id",
                   "KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH": str(path)}
            with patch.dict(os.environ, env):
                return auth.ws_auth_headers()

    def message(self, h):
        return (h["KALSHI-ACCESS-TIMESTAMP"] + "GET" + auth.WS_PATH).encode()

    def test_rsa_pss_signature_verifies(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        h = self.headers_for(key)
        self.assertEqual(h["KALSHI-ACCESS-KEY"], "test-key-id")
        key.public_key().verify(
            base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), self.message(h),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )

    def test_ed25519_signature_verifies(self):
        key = Ed25519PrivateKey.generate()
        h = self.headers_for(key)
        key.public_key().verify(base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), self.message(h))

    def test_unsupported_key_type_is_refused(self):
        with self.assertRaises(RuntimeError):
            self.headers_for(ec.generate_private_key(ec.SECP256R1()))

    def test_missing_configuration_means_no_auth_and_bad_path_raises(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("KALSHI15M_MARKETDATA_API_KEY_ID", None)
            os.environ.pop("KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH", None)
            self.assertIsNone(auth.ws_auth_headers())
        env = {"KALSHI15M_MARKETDATA_API_KEY_ID": "x", "KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH": "/nonexistent/k.pem"}
        with patch.dict(os.environ, env), self.assertRaises(RuntimeError):
            auth.ws_auth_headers()


if __name__ == "__main__":
    unittest.main()
