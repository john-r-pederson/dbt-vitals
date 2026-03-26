import base64
import logging
import re
from typing import Any

import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

from adapters.base import BaseWarehouseAdapter

logger = logging.getLogger(__name__)

# Snowflake identifiers allow letters, digits, underscores, and dollar signs (unquoted).
_SAFE_IDENTIFIER = re.compile(r"^[A-Z0-9_$]+$")

# Quoted identifier: same as standard but also allows hyphens (common in cloud database names).
# Rejects anything else (spaces, dots, semicolons, etc.) which indicates user error.
_HYPHENATED_IDENTIFIER = re.compile(r"^[A-Za-z0-9_$-]+$")

# Kill any query that runs longer than this to prevent a hung CI job.
_QUERY_TIMEOUT_SECONDS = 60


def _format_date(value: Any) -> str | None:
    """Formats a datetime-like value to a clean YYYY-MM-DD string."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    # Fallback for string values: truncate to date portion
    return str(value)[:10]


def _validate_identifier(value: str) -> str:
    """
    Returns a SQL-safe identifier string.
    Standard names (A-Z, 0-9, _, $) are returned uppercased and unquoted.
    Hyphenated names (e.g. 'prod-db') are returned double-quoted with original
    casing preserved — quoting preserves case in Snowflake.
    Anything else (spaces, dots, semicolons, etc.) is rejected as user error.
    """
    if not value:
        raise ValueError(f"Invalid warehouse identifier: {value!r}")
    upper = value.upper()
    if _SAFE_IDENTIFIER.match(upper):
        return upper
    if _HYPHENATED_IDENTIFIER.match(value):
        return f'"{value}"'
    raise ValueError(f"Invalid warehouse identifier: {value!r}")


class SnowflakeAdapter(BaseWarehouseAdapter):
    """Warehouse adapter for Snowflake. Queries INFORMATION_SCHEMA and ACCOUNT_USAGE.ACCESS_HISTORY."""

    def __init__(self, cfg: Any) -> None:
        """Open a Snowflake connection using key-pair, password, or browser auth based on available credentials."""
        self.ctx = None
        self.cursor = None
        self.lookback_days = cfg.LOOKBACK_DAYS

        params = {
            "user": cfg.SNOWFLAKE_USER,
            "account": cfg.SNOWFLAKE_ACCOUNT,
            "warehouse": cfg.SNOWFLAKE_WAREHOUSE,
            "database": cfg.SNOWFLAKE_DATABASE,
            "schema": cfg.SNOWFLAKE_SCHEMA,
            "role": cfg.SNOWFLAKE_ROLE,
        }

        if cfg.SNOWFLAKE_HOST:
            params["host"] = cfg.SNOWFLAKE_HOST

        # Auth priority:
        # 1. Key-pair (CI/CD) — headless, no browser, no MFA prompt
        # 2. Password (basic)
        # 3. externalbrowser (local dev SSO/MFA)
        if cfg.SNOWFLAKE_PRIVATE_KEY:
            logger.info("Key-pair auth detected. Connecting headlessly...")
            params["private_key"] = self._decode_private_key(
                cfg.SNOWFLAKE_PRIVATE_KEY,
                cfg.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            )
        elif cfg.SNOWFLAKE_PASSWORD:
            params["password"] = cfg.SNOWFLAKE_PASSWORD
        else:
            logger.info("No key or password. Initiating browser authenticator for SSO/MFA...")
            params["authenticator"] = cfg.SNOWFLAKE_AUTHENTICATOR

        try:
            self.ctx = snowflake.connector.connect(**params)
            self.cursor = self.ctx.cursor()
            self.cursor.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {_QUERY_TIMEOUT_SECONDS}"
            )
            logger.info("Snowflake connection established.")
        except Exception as e:
            error_msg = str(e)
            if "Multi-factor authentication is required" in error_msg:
                raise Exception(
                    "MFA Required: Your account enforces MFA. "
                    "Set SNOWFLAKE_PRIVATE_KEY in your environment to use key-pair auth, "
                    "or remove SNOWFLAKE_PASSWORD to trigger the browser authenticator."
                )
            raise Exception(f"Failed to connect to Snowflake: {e}")

    def _decode_private_key(
        self, private_key_b64: str, passphrase: str | None
    ) -> bytes:
        """Decodes a base64-encoded PEM private key into DER bytes for the connector."""
        try:
            pem_bytes = base64.b64decode(private_key_b64)
        except Exception as e:
            raise ValueError(
                "Could not base64-decode SNOWFLAKE_PRIVATE_KEY. "
                "Ensure it was encoded with: base64 -i snowflake_key.p8 | tr -d '\\n'"
            ) from e
        pw = passphrase.encode() if passphrase else None
        try:
            private_key = load_pem_private_key(pem_bytes, password=pw)
        except Exception as e:
            raise ValueError(
                "Could not load private key from SNOWFLAKE_PRIVATE_KEY. "
                "If the key is passphrase-protected, set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE."
            ) from e
        return private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

    def get_table_stats(self, db: str, schema: str, table: str) -> dict[str, Any]:
        """Return size, last_altered, read metrics, and table type for a given warehouse table."""
        db_u = _validate_identifier(db)
        schema_u = _validate_identifier(schema)
        table_u = _validate_identifier(table)

        size_gb, last_altered, table_type, query_error = self._query_table_metadata(db_u, schema_u, table_u)
        if size_gb is None and last_altered is None:
            return {
                "exists": False,
                "size_gb": None,
                "last_altered": None,
                "last_read": None,
                "read_count": 0,
                "distinct_users": 0,
                "access_history_available": False,
                "table_type": None,
                "query_error": query_error,
            }

        # Strip surrounding quotes for the ACCESS_HISTORY bind parameter — Snowflake
        # stores object names without surrounding quotes in ACCOUNT_USAGE.
        db_plain = db_u.strip('"')
        schema_plain = schema_u.strip('"')
        table_plain = table_u.strip('"')
        access_stats = self._query_last_read(db_plain, schema_plain, table_plain)

        return {
            "exists": True,
            "size_gb": size_gb,
            "last_altered": _format_date(last_altered),
            "last_read": _format_date(access_stats["last_read"]),
            "read_count": access_stats["read_count"],
            "distinct_users": access_stats["distinct_users"],
            "access_history_available": access_stats["available"],
            "table_type": table_type,
            "query_error": False,
        }

    def _query_table_metadata(
        self, db: str, schema: str, table: str
    ) -> tuple[float | None, Any, str | None, bool]:
        """Queries INFORMATION_SCHEMA for table size, last altered timestamp, and table type."""
        query = (
            f"SELECT BYTES, LAST_ALTERED, TABLE_TYPE "
            f"FROM {db}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        )
        # Bind params are VALUES not identifiers — strip surrounding quotes if present.
        # _validate_identifier may have added them for hyphenated names (e.g. '"prod-schema"').
        schema_bind = schema.strip('"')
        table_bind = table.strip('"')
        try:
            self.cursor.execute(query, (schema_bind, table_bind))
            result = self.cursor.fetchone()
            if result:
                bytes_val, last_altered, table_type = result
                # Views have NULL BYTES — return None so report can distinguish from empty table
                size_gb = round(bytes_val / (1024**3), 4) if bytes_val else (0.0 if bytes_val == 0 else None)
                return size_gb, last_altered, table_type, False
            return None, None, None, False
        except Exception as e:
            logger.warning(f"Could not query INFORMATION_SCHEMA for {db}.{schema}.{table}: {e}")
            return None, None, None, True

    def _query_last_read(self, db: str, schema: str, table: str) -> dict[str, Any]:
        """
        Queries ACCOUNT_USAGE.ACCESS_HISTORY for usage stats within the lookback window.

        Returns:
            {
                "last_read": str | None,    — most recent query timestamp
                "read_count": int,          — distinct queries in lookback window
                "distinct_users": int,      — distinct Snowflake users
                "available": bool,          — False if ACCESS_HISTORY is inaccessible
            }

        Note: ACCESS_HISTORY has ~3 hour data latency and requires the configured
        role to have IMPORTED PRIVILEGES on the SNOWFLAKE database.
        Degrades gracefully when unavailable.
        """
        # objectName in ACCESS_HISTORY is stored without surrounding quotes.
        # Always uppercase the composed name so the UPPER() comparison matches
        # even when components contain hyphens (stored lowercase in db_plain).
        full_name = f"{db}.{schema}.{table}".upper()
        query = """
            SELECT
                MAX(query_start_time),
                COUNT(DISTINCT query_id),
                COUNT(DISTINCT user_name)
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                 LATERAL FLATTEN(input => base_objects_accessed) obj
            WHERE obj.value:objectDomain::STRING IN ('Table', 'View')
              AND UPPER(obj.value:objectName::STRING) = %s
              AND query_start_time >= DATEADD(DAY, -%s, CURRENT_TIMESTAMP())
        """
        try:
            self.cursor.execute(query, (full_name, self.lookback_days))
            result = self.cursor.fetchone()
            if result and result[0] is not None:
                return {
                    "last_read": result[0],
                    "read_count": result[1] or 0,
                    "distinct_users": result[2] or 0,
                    "available": True,
                }
            return {"last_read": None, "read_count": 0, "distinct_users": 0, "available": True}
        except Exception as e:
            logger.warning(f"Could not query ACCESS_HISTORY (check role grants): {e}")
            return {"last_read": None, "read_count": 0, "distinct_users": 0, "available": False}

    def close(self) -> None:
        """Close the Snowflake cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.ctx:
            self.ctx.close()
        logger.info("Snowflake connection closed.")
