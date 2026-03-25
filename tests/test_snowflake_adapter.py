"""
Tests for pure utility functions in SnowflakeAdapter.
No Snowflake connection is required — these functions are standalone.
"""
import base64
from datetime import datetime

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from adapters.snowflake_adapter import _format_date, _validate_identifier
from adapters.snowflake_adapter import SnowflakeAdapter


# ---------------------------------------------------------------------------
# _format_date
# ---------------------------------------------------------------------------

def test_format_date_none_returns_none():
    assert _format_date(None) is None


def test_format_date_datetime_returns_yyyy_mm_dd():
    assert _format_date(datetime(2026, 3, 25, 14, 30, 0)) == "2026-03-25"


def test_format_date_iso_string_truncated_to_date():
    assert _format_date("2026-03-25T14:30:00.000Z") == "2026-03-25"


def test_format_date_date_only_string_returned_as_is():
    assert _format_date("2026-03-25") == "2026-03-25"


def test_format_date_short_string_returned_as_is():
    assert _format_date("2026") == "2026"


# ---------------------------------------------------------------------------
# _validate_identifier
# ---------------------------------------------------------------------------

def test_validate_identifier_valid_name():
    assert _validate_identifier("MY_TABLE") == "MY_TABLE"


def test_validate_identifier_uppercases_input():
    assert _validate_identifier("my_table") == "MY_TABLE"


def test_validate_identifier_allows_dollar_sign():
    assert _validate_identifier("TABLE$1") == "TABLE$1"


def test_validate_identifier_allows_digits():
    assert _validate_identifier("TABLE1") == "TABLE1"


def test_validate_identifier_rejects_empty_string():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("")


def test_validate_identifier_rejects_space():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("MY TABLE")


def test_validate_identifier_rejects_dot():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("MY.TABLE")


def test_validate_identifier_rejects_sql_injection():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("X; DROP TABLE users")


def test_validate_identifier_rejects_dash():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("MY-TABLE")


# ---------------------------------------------------------------------------
# _decode_private_key (using a generated throwaway key — no secrets committed)
# ---------------------------------------------------------------------------

def _make_test_key_b64() -> str:
    """Generate a fresh RSA key and return it base64-encoded as PKCS8 PEM."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    return base64.b64encode(pem).decode()


def test_decode_private_key_valid_returns_der_bytes():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    key_b64 = _make_test_key_b64()
    result = adapter._decode_private_key(key_b64, None)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_decode_private_key_invalid_base64_raises():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    with pytest.raises(Exception):
        adapter._decode_private_key("not-valid-base64!!!", None)


def test_decode_private_key_wrong_passphrase_raises():
    from cryptography.hazmat.primitives.serialization import BestAvailableEncryption
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=BestAvailableEncryption(b"correct-passphrase"),
    )
    key_b64 = base64.b64encode(pem).decode()

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    with pytest.raises(Exception):
        adapter._decode_private_key(key_b64, "wrong-passphrase")
