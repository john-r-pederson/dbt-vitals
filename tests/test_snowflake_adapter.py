"""
Tests for SnowflakeAdapter utility functions and mocked query methods.
No live Snowflake connection is required.
"""
import base64
from datetime import datetime
from unittest.mock import MagicMock

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


def test_validate_identifier_allows_hyphen():
    assert _validate_identifier("MY-TABLE") == '"MY-TABLE"'


def test_validate_identifier_allows_hyphen_in_db_name():
    assert _validate_identifier("prod-db") == '"prod-db"'


def test_validate_identifier_rejects_double_quote():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier('my"table')


def test_validate_identifier_rejects_null_byte():
    with pytest.raises(ValueError, match="Invalid warehouse identifier"):
        _validate_identifier("my\x00table")


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
    with pytest.raises(ValueError, match="base64-decode SNOWFLAKE_PRIVATE_KEY"):
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
    with pytest.raises(ValueError, match="SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        adapter._decode_private_key(key_b64, "wrong-passphrase")


# ---------------------------------------------------------------------------
# _query_last_read — mocked cursor (no Snowflake connection needed)
# ---------------------------------------------------------------------------

def _make_adapter_with_cursor(fetchone_return):
    """Build a SnowflakeAdapter shell with a mocked cursor."""
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_return
    adapter.cursor = cursor
    return adapter


def test_query_last_read_returns_metrics():
    from datetime import datetime
    dt = datetime(2026, 3, 20, 10, 0, 0)
    adapter = _make_adapter_with_cursor((dt, 42, 5))
    result = adapter._query_last_read("DB", "SCH", "TBL")
    assert result["last_read"] == dt
    assert result["read_count"] == 42
    assert result["distinct_users"] == 5
    assert result["available"] is True


def test_query_last_read_returns_zeros_when_no_rows():
    adapter = _make_adapter_with_cursor((None, 0, 0))
    result = adapter._query_last_read("DB", "SCH", "TBL")
    assert result["last_read"] is None
    assert result["read_count"] == 0
    assert result["distinct_users"] == 0
    assert result["available"] is True


def test_query_last_read_marks_unavailable_on_exception():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("Insufficient privileges")
    adapter.cursor = cursor
    result = adapter._query_last_read("DB", "SCH", "TBL")
    assert result["available"] is False
    assert result["read_count"] == 0


# ---------------------------------------------------------------------------
# _query_table_metadata — mocked cursor
# ---------------------------------------------------------------------------

def test_query_table_metadata_returns_all_fields():
    from datetime import datetime
    dt = datetime(2026, 3, 24)
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    cursor = MagicMock()
    cursor.fetchone.return_value = (1073741824, dt, "BASE TABLE")  # 1 GB in bytes
    adapter.cursor = cursor
    found, size_gb, last_altered, table_type, query_error = adapter._query_table_metadata("DB", "SCH", "TBL")
    assert found is True
    assert size_gb == 1.0
    assert last_altered == dt
    assert table_type == "BASE TABLE"
    assert query_error is False


def test_query_table_metadata_returns_none_for_view():
    from datetime import datetime
    dt = datetime(2026, 3, 24)
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    cursor = MagicMock()
    cursor.fetchone.return_value = (None, dt, "VIEW")  # views have NULL bytes
    adapter.cursor = cursor
    found, size_gb, last_altered, table_type, query_error = adapter._query_table_metadata("DB", "SCH", "TBL")
    assert found is True
    assert size_gb is None
    assert table_type == "VIEW"
    assert query_error is False


def test_query_table_metadata_returns_not_found_when_no_row():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    adapter.cursor = cursor
    found, size_gb, last_altered, table_type, query_error = adapter._query_table_metadata("DB", "SCH", "TBL")
    assert found is False
    assert size_gb is None
    assert last_altered is None
    assert table_type is None
    assert query_error is False


def test_query_table_metadata_returns_error_flag_on_exception():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("permission denied")
    adapter.cursor = cursor
    found, size_gb, last_altered, table_type, query_error = adapter._query_table_metadata("DB", "SCH", "TBL")
    assert found is False
    assert size_gb is None
    assert last_altered is None
    assert table_type is None
    assert query_error is True


def test_query_table_metadata_strips_quotes_from_hyphenated_bind_params():
    """_validate_identifier quotes hyphenated names; those quotes must not reach the bind params."""
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    adapter.cursor = cursor
    adapter._query_table_metadata('"MYDB"', '"prod-schema"', '"fact-orders"')
    _, call_args = cursor.execute.call_args
    assert call_args == ("prod-schema", "fact-orders")


# ---------------------------------------------------------------------------
# get_table_stats — integration of both queries (mocked cursor)
# ---------------------------------------------------------------------------

def test_get_table_stats_populates_all_fields():
    from datetime import datetime
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90

    altered_dt = datetime(2026, 3, 24)
    read_dt = datetime(2026, 3, 25)

    cursor = MagicMock()
    # First fetchone: _query_table_metadata
    # Second fetchone: _query_last_read
    cursor.fetchone.side_effect = [
        (536870912, altered_dt, "BASE TABLE"),  # 0.5 GB
        (read_dt, 17, 3),
    ]
    adapter.cursor = cursor

    result = adapter.get_table_stats("MY_DB", "MY_SCHEMA", "MY_TABLE")
    assert result["exists"] is True
    assert result["size_gb"] == 0.5
    assert result["last_altered"] == "2026-03-24"
    assert result["last_read"] == "2026-03-25"
    assert result["read_count"] == 17
    assert result["distinct_users"] == 3
    assert result["access_history_available"] is True
    assert result["table_type"] == "BASE TABLE"
    assert result["query_error"] is False


def test_get_table_stats_returns_not_exists_when_not_found():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90
    cursor = MagicMock()
    cursor.fetchone.return_value = None  # table not found in INFORMATION_SCHEMA
    adapter.cursor = cursor

    result = adapter.get_table_stats("MY_DB", "MY_SCHEMA", "MISSING_TABLE")
    assert result["exists"] is False
    assert result["size_gb"] is None
    assert result["query_error"] is False


def test_get_table_stats_marks_query_error_on_exception():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("Insufficient privileges on INFORMATION_SCHEMA")
    adapter.cursor = cursor

    result = adapter.get_table_stats("MY_DB", "MY_SCHEMA", "RESTRICTED_TABLE")
    assert result["exists"] is False
    assert result["query_error"] is True


def test_access_history_query_uses_uppercased_full_name():
    """The ACCESS_HISTORY query must be called with the fully-qualified name uppercased.

    Snowflake stores objectName without quotes, so UPPER(objectName) = %s requires
    the bind parameter to be uppercased too — especially for hyphenated identifiers
    like 'prod-db' which are stored as 'PROD-DB'.
    """
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.lookback_days = 90
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    adapter.cursor = cursor

    adapter._query_last_read("prod-db", "my_schema", "my_table")

    _, call_args, _ = cursor.execute.mock_calls[0]
    bound_full_name = call_args[1][0]  # first positional bind param
    assert bound_full_name == "PROD-DB.MY_SCHEMA.MY_TABLE"


def test_access_history_query_returns_nonzero_for_view_domain():
    """ACCESS_HISTORY must return reads for View-domain objects (not just Table).

    Prior to the IN ('Table', 'View') fix, deleted view models always showed 0 reads
    because the query filtered objectDomain = 'Table' exclusively.
    This test verifies that a row returned for a view-domain lookup is not discarded.
    """
    from datetime import datetime
    view_read_dt = datetime(2026, 3, 20, 10, 0, 0)
    adapter = _make_adapter_with_cursor((view_read_dt, 7, 2))
    result = adapter._query_last_read("DB", "SCH", "MY_VIEW")
    assert result["read_count"] == 7
    assert result["distinct_users"] == 2
    assert result["available"] is True
