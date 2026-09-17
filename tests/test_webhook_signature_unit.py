from app.security.webhooks import sign_webhook_payload, verify_webhook_signature


def test_webhook_signature_is_verified_with_constant_time_helper() -> None:
    payload = b'{"event":"run.succeeded"}'
    timestamp = "1760000000"
    signature = sign_webhook_payload(payload, timestamp, "secret")

    assert not verify_webhook_signature(
        payload, signature, timestamp, "wrong", 999999999
    )
    assert verify_webhook_signature(payload, signature, timestamp, "secret", 999999999)
