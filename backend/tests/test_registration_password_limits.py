"""Registration validation limits (SEC-093)."""

from backend.utils.registration_validation import MAX_PASSWORD_LEN, registration_form_error


def test_password_max_length_rejected() -> None:
    err = registration_form_error(
        "user",
        "user@example.com",
        "x" * (MAX_PASSWORD_LEN + 1),
        min_password_len=8,
    )
    assert err is not None
    assert "at most" in err
