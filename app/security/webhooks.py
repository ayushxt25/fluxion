import hashlib
import hmac
import time


def sign_webhook_payload(payload: bytes, timestamp: str, secret: str) -> str:
    signed = timestamp.encode() + b"." + payload
    return "v1=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


def verify_webhook_signature(
    payload: bytes,
    signature: str,
    timestamp: str,
    secret: str,
    tolerance_seconds: int = 300,
) -> bool:
    try:
        age = abs(time.time() - float(timestamp))
    except ValueError:
        return False
    return age <= tolerance_seconds and hmac.compare_digest(
        signature, sign_webhook_payload(payload, timestamp, secret)
    )
