from unittest.mock import MagicMock, patch

import pytest

from adapters.factory import get_adapter


def _cfg(warehouse_type: str) -> MagicMock:
    cfg = MagicMock()
    cfg.WAREHOUSE_TYPE = warehouse_type
    return cfg


# ---------------------------------------------------------------------------
# Snowflake routing
# ---------------------------------------------------------------------------

def test_snowflake_returns_snowflake_adapter():
    """WAREHOUSE_TYPE='snowflake' must return a SnowflakeAdapter instance."""
    with patch("adapters.snowflake_adapter.SnowflakeAdapter") as MockSnowflake:
        MockSnowflake.return_value = MagicMock()
        adapter = get_adapter(_cfg("snowflake"))
    MockSnowflake.assert_called_once()
    assert adapter is MockSnowflake.return_value


def test_snowflake_routing_is_case_insensitive_upper():
    """'SNOWFLAKE' (uppercase) must route to SnowflakeAdapter."""
    with patch("adapters.snowflake_adapter.SnowflakeAdapter") as MockSnowflake:
        MockSnowflake.return_value = MagicMock()
        get_adapter(_cfg("SNOWFLAKE"))
    MockSnowflake.assert_called_once()


def test_snowflake_routing_is_case_insensitive_mixed():
    """'Snowflake' (mixed case) must route to SnowflakeAdapter."""
    with patch("adapters.snowflake_adapter.SnowflakeAdapter") as MockSnowflake:
        MockSnowflake.return_value = MagicMock()
        get_adapter(_cfg("Snowflake"))
    MockSnowflake.assert_called_once()


def test_snowflake_adapter_receives_cfg():
    """SnowflakeAdapter must be constructed with the cfg object passed to get_adapter."""
    cfg = _cfg("snowflake")
    with patch("adapters.snowflake_adapter.SnowflakeAdapter") as MockSnowflake:
        MockSnowflake.return_value = MagicMock()
        get_adapter(cfg)
    MockSnowflake.assert_called_once_with(cfg)


# ---------------------------------------------------------------------------
# Unsupported warehouse
# ---------------------------------------------------------------------------

def test_unsupported_warehouse_raises_value_error():
    """An unknown WAREHOUSE_TYPE must raise ValueError, not silently return None."""
    with pytest.raises(ValueError):
        get_adapter(_cfg("bigquery"))


def test_unsupported_warehouse_error_includes_type():
    """The ValueError message must include the unsupported type so users know what to fix."""
    with pytest.raises(ValueError, match="bigquery"):
        get_adapter(_cfg("bigquery"))


def test_unsupported_warehouse_error_lists_supported():
    """The ValueError message should mention the supported warehouse types."""
    with pytest.raises(ValueError, match="snowflake"):
        get_adapter(_cfg("redshift"))


def test_empty_warehouse_type_raises():
    """An empty WAREHOUSE_TYPE string must raise ValueError, not route to a default."""
    with pytest.raises(ValueError):
        get_adapter(_cfg(""))
