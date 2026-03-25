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
