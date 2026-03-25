import base64
import logging
import re

import snowflake.connector

logger = logging.getLogger(__name__)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

from adapters.base import BaseWarehouseAdapter

# Snowflake identifiers allow letters, digits, underscores, and dollar signs.
_SAFE_IDENTIFIER = re.compile(r"^[A-Z0-9_$]+$")

# Kill any query that runs longer than this to prevent a hung CI job.
_QUERY_TIMEOUT_SECONDS = 60


def _format_date(value) -> str | None:
    """Formats a datetime-like value to a clean YYYY-MM-DD string."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    # Fallback for string values: truncate to date portion
    return str(value)[:10]


def _validate_identifier(value: str) -> str:
    upper = value.upper()
    if not _SAFE_IDENTIFIER.match(upper):
        raise ValueError(f"Invalid warehouse identifier: {value!r}")
    return upper


class SnowflakeAdapter(BaseWarehouseAdapter):
    def __init__(self, cfg):
        self.ctx = None
        self.cursor = None

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
            logger.info("🔑 Key-pair auth detected. Connecting headlessly...")
            params["private_key"] = self._decode_private_key(
                cfg.SNOWFLAKE_PRIVATE_KEY,
                cfg.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            )
        elif cfg.SNOWFLAKE_PASSWORD:
            params["password"] = cfg.SNOWFLAKE_PASSWORD
        else:
            logger.info(
                "🔑 No key or password. Initiating browser authenticator for SSO/MFA..."
            )
            params["authenticator"] = cfg.SNOWFLAKE_AUTHENTICATOR

        try:
            self.ctx = snowflake.connector.connect(**params)
            self.cursor = self.ctx.cursor()
            self.cursor.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {_QUERY_TIMEOUT_SECONDS}"
            )
            logger.info("✅ Snowflake connection established.")
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
        pem_bytes = base64.b64decode(private_key_b64)
        pw = passphrase.encode() if passphrase else None
        private_key = load_pem_private_key(pem_bytes, password=pw)
        return private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

    def get_table_stats(self, db: str, schema: str, table: str) -> dict:
        db_u = _validate_identifier(db)
        schema_u = _validate_identifier(schema)
        table_u = _validate_identifier(table)

        size_gb, last_altered = self._query_table_metadata(db_u, schema_u, table_u)
        if size_gb is None and last_altered is None:
            return {
                "exists": False,
                "size_gb": 0,
                "last_altered": None,
                "last_read": None,
            }

        last_read = self._query_last_read(db_u, schema_u, table_u)

        return {
            "exists": True,
            "size_gb": size_gb,
            "last_altered": _format_date(last_altered),
            "last_read": _format_date(last_read),
        }

    def _query_table_metadata(self, db: str, schema: str, table: str):
        """Queries INFORMATION_SCHEMA for table size and last altered timestamp."""
        query = f"SELECT BYTES, LAST_ALTERED FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        try:
            self.cursor.execute(query, (schema, table))
            result = self.cursor.fetchone()
            if result:
                bytes_val, last_altered = result
                size_gb = round(bytes_val / (1024**3), 4) if bytes_val else 0.0
                return size_gb, last_altered
            return None, None
        except Exception as e:
            logger.warning(
                f"⚠️  Warning: Could not query INFORMATION_SCHEMA for {db}.{schema}.{table}: {e}"
            )
            return None, None

    def _query_last_read(self, db: str, schema: str, table: str) -> str | None:
        """
        Queries ACCOUNT_USAGE.ACCESS_HISTORY for the last time this table was read.

        Note: ACCESS_HISTORY has ~3 hour data latency and requires the configured
        role to have IMPORTED PRIVILEGES on the SNOWFLAKE database.
        Degrades gracefully to None if unavailable.
        """
        full_name = f"{db}.{schema}.{table}"
        query = """
            SELECT MAX(query_start_time)
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                 LATERAL FLATTEN(input => base_objects_accessed) obj
            WHERE obj.value:objectDomain::STRING = 'Table'
              AND UPPER(obj.value:objectName::STRING) = %s
        """
        try:
            self.cursor.execute(query, (full_name,))
            result = self.cursor.fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.warning(
                f"⚠️  Warning: Could not query ACCESS_HISTORY (check role grants): {e}"
            )
            return None

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.ctx:
            self.ctx.close()
        logger.info("🛡️  Snowflake connection closed.")
