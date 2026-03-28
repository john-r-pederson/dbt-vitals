import pytest
from pydantic import ValidationError

from config import Settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FULL_SNOWFLAKE_ENV = {
    "WAREHOUSE_TYPE": "snowflake",
    "SNOWFLAKE_USER": "MY_USER",
    "SNOWFLAKE_ACCOUNT": "myorg-abc123",
    "SNOWFLAKE_WAREHOUSE": "MY_WH",
    "SNOWFLAKE_DATABASE": "MY_DB",
    "SNOWFLAKE_SCHEMA": "MY_SCHEMA",
    "SNOWFLAKE_ROLE": "MY_ROLE",
}


def _settings(monkeypatch, overrides: dict) -> Settings:
    """Build a Settings object from a controlled env, bypassing any .env file."""
    env = {**_FULL_SNOWFLAKE_ENV, **overrides}
    for key, val in env.items():
        monkeypatch.setenv(key, val)
    # Disable .env file loading so local .env does not interfere
    return Settings(_env_file=None)


# ---------------------------------------------------------------------------
# Valid config
# ---------------------------------------------------------------------------

def test_valid_snowflake_config_loads(monkeypatch):
    cfg = _settings(monkeypatch, {})
    assert cfg.SNOWFLAKE_USER == "MY_USER"
    assert cfg.SNOWFLAKE_ACCOUNT == "myorg-abc123"


def test_base_branch_defaults_to_main(monkeypatch):
    cfg = _settings(monkeypatch, {})
    assert cfg.BASE_BRANCH == "main"


def test_base_branch_can_be_overridden(monkeypatch):
    cfg = _settings(monkeypatch, {"BASE_BRANCH": "develop"})
    assert cfg.BASE_BRANCH == "develop"


# ---------------------------------------------------------------------------
# Missing Snowflake fields
# ---------------------------------------------------------------------------

def test_missing_one_snowflake_field_raises(monkeypatch):
    for key in _FULL_SNOWFLAKE_ENV:
        if key == "WAREHOUSE_TYPE":
            continue
        env = {k: v for k, v in _FULL_SNOWFLAKE_ENV.items() if k != key}
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv(key, raising=False)

        with pytest.raises((ValidationError, ValueError)) as exc_info:
            Settings(_env_file=None)

        assert key in str(exc_info.value)


def test_all_snowflake_fields_missing_raises(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_TYPE", "snowflake")
    required = [
        "SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE",
    ]
    for key in required:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises((ValidationError, ValueError)) as exc_info:
        Settings(_env_file=None)

    error_text = str(exc_info.value)
    for key in required:
        assert key in error_text


# ---------------------------------------------------------------------------
# Non-Snowflake warehouse skips Snowflake validation
# ---------------------------------------------------------------------------

def test_non_snowflake_warehouse_skips_validation(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_TYPE", "bigquery")
    for key in ["SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE"]:
        monkeypatch.delenv(key, raising=False)

    # Should not raise — Snowflake validation is skipped for other warehouse types
    cfg = Settings(_env_file=None)
    assert cfg.WAREHOUSE_TYPE == "bigquery"


# ---------------------------------------------------------------------------
# Snowflake account format validation
# ---------------------------------------------------------------------------

def test_legacy_account_locator_raises(monkeypatch):
    with pytest.raises((ValidationError, ValueError)) as exc_info:
        _settings(monkeypatch, {"SNOWFLAKE_ACCOUNT": "wdb44754"})
    assert "org-account" in str(exc_info.value)


def test_valid_account_format_accepted(monkeypatch):
    cfg = _settings(monkeypatch, {"SNOWFLAKE_ACCOUNT": "acme-abc12345"})
    assert cfg.SNOWFLAKE_ACCOUNT == "acme-abc12345"


def test_account_with_multiple_hyphens_accepted(monkeypatch):
    cfg = _settings(monkeypatch, {"SNOWFLAKE_ACCOUNT": "my-org-abc12345"})
    assert cfg.SNOWFLAKE_ACCOUNT == "my-org-abc12345"


# ---------------------------------------------------------------------------
# LOOKBACK_DAYS validation
# ---------------------------------------------------------------------------

def test_lookback_days_default_is_90(monkeypatch):
    cfg = _settings(monkeypatch, {})
    assert cfg.LOOKBACK_DAYS == 90


def test_lookback_days_valid_value_accepted(monkeypatch):
    cfg = _settings(monkeypatch, {"LOOKBACK_DAYS": "30"})
    assert cfg.LOOKBACK_DAYS == 30


def test_lookback_days_zero_raises(monkeypatch):
    with pytest.raises((ValidationError, ValueError)):
        _settings(monkeypatch, {"LOOKBACK_DAYS": "0"})


def test_lookback_days_negative_raises(monkeypatch):
    with pytest.raises((ValidationError, ValueError)):
        _settings(monkeypatch, {"LOOKBACK_DAYS": "-5"})


def test_query_timeout_default_is_60(monkeypatch):
    cfg = _settings(monkeypatch, {})
    assert cfg.QUERY_TIMEOUT_SECONDS == 60


def test_seeds_dir_default(monkeypatch):
    cfg = _settings(monkeypatch, {})
    assert cfg.SEEDS_DIR == "seeds/"


def test_query_timeout_custom_value(monkeypatch):
    cfg = _settings(monkeypatch, {"QUERY_TIMEOUT_SECONDS": "120"})
    assert cfg.QUERY_TIMEOUT_SECONDS == 120


def test_seeds_dir_custom_value(monkeypatch):
    cfg = _settings(monkeypatch, {"SEEDS_DIR": "data/seeds/"})
    assert cfg.SEEDS_DIR == "data/seeds/"


def test_query_timeout_zero_raises(monkeypatch):
    with pytest.raises((ValidationError, ValueError)):
        _settings(monkeypatch, {"QUERY_TIMEOUT_SECONDS": "0"})


def test_query_timeout_negative_raises(monkeypatch):
    with pytest.raises((ValidationError, ValueError)):
        _settings(monkeypatch, {"QUERY_TIMEOUT_SECONDS": "-10"})


# ---------------------------------------------------------------------------
# get_config() wrapper — sys.exit on misconfiguration
# ---------------------------------------------------------------------------

def test_get_config_exits_on_invalid_settings(monkeypatch):
    """get_config() must call sys.exit(1) when Settings raises, not propagate the exception."""
    from config import get_config
    monkeypatch.setenv("WAREHOUSE_TYPE", "snowflake")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "wdb44754")  # no hyphen — invalid format
    for key in ["SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
                "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE"]:
        monkeypatch.setenv(key, "x")

    with pytest.raises(SystemExit) as exc_info:
        get_config()
    assert exc_info.value.code == 1
