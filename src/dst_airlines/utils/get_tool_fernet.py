from pathlib import Path
import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet


load_dotenv()

def _clean(s: str) -> str:
    return s.strip().strip('"').strip("'")

def _maybe_decrypt(value: str, cipher: Fernet) -> str:
    v = _clean(value)
    if v.startswith("gAAAAA"):
        return cipher.decrypt(v.encode("utf-8")).decode("utf-8")
    return v

def get_credentials(limit=100):
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("Missing ENCRYPTION_KEY in environment (.env).")

    cipher = Fernet(_clean(key).encode("utf-8"))

    host = os.getenv("API_URL_AVIONSTACK")
    token_enc = os.getenv("TOKEN_AVIONSTACK")

    if not host or not token_enc:
        raise RuntimeError("Missing API_URL_AVIONSTACK/TOKEN_AVIONSTACK in environment (.env).")

    token = _maybe_decrypt(token_enc, cipher)

    if not host.startswith(("http://", "https://")):
        raise RuntimeError(f"host does not look like a URL: {host!r}")

    return host.rstrip("/"), token, limit