import logging
import sys

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # --- Warehouse ---
    WAREHOUSE_TYPE: str = "snowflake"

    # --- Snowflake (required when WAREHOUSE_TYPE=snowflake) ---
    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_WAREHOUSE: str = ""
    SNOWFLAKE_DATABASE: str = ""
    SNOWFLAKE_SCHEMA: str = ""
    SNOWFLAKE_ROLE: str = ""

    # --- Snowflake Auth (priority: key-pair > password > browser) ---
    SNOWFLAKE_PRIVATE_KEY: str | None = None
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: str | None = None
    SNOWFLAKE_PASSWORD: str | None = None
    SNOWFLAKE_HOST: str | None = None
    SNOWFLAKE_AUTHENTICATOR: str = "externalbrowser"

    # --- GitHub (injected by GitHub Actions) ---
    GITHUB_TOKEN: str | None = None
    GITHUB_REPOSITORY: str | None = None  # "owner/repo"
    PR_NUMBER: str | None = None

    # --- dbt-vitals Behavior ---
    BASE_BRANCH: str = "main"
    MANIFEST_PATH: str | None = None
    LOOKBACK_DAYS: int = 90
    REPO_SUBDIRECTORY: str | None = None   # e.g. "dbt" for monorepos
    PR_TITLE: str | None = None            # used for [skip dbt-vitals] check
    TARGET_DIR: str = "models/"            # dbt models directory to watch
    SEEDS_DIR: str = "seeds/"             # dbt seeds directory to watch for deleted CSVs
    QUERY_TIMEOUT_SECONDS: int = 60       # per-query Snowflake timeout; increase for large ACCESS_HISTORY

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    @field_validator("LOOKBACK_DAYS")
    @classmethod
    def validate_lookback_days(cls, v: int) -> int:
        """Reject lookback values that would silently return zero rows from ACCESS_HISTORY."""
        if v < 1:
            raise ValueError("LOOKBACK_DAYS must be >= 1")
        return v

    @model_validator(mode="after")
    def check_snowflake_credentials(self) -> "Settings":
        """Validate required Snowflake fields and account format. Add per-adapter validation here as new adapters are added."""
        if self.WAREHOUSE_TYPE.lower() == "snowflake":
            missing = [
                name for name, val in {
                    "SNOWFLAKE_USER": self.SNOWFLAKE_USER,
                    "SNOWFLAKE_ACCOUNT": self.SNOWFLAKE_ACCOUNT,
                    "SNOWFLAKE_WAREHOUSE": self.SNOWFLAKE_WAREHOUSE,
                    "SNOWFLAKE_DATABASE": self.SNOWFLAKE_DATABASE,
                    "SNOWFLAKE_SCHEMA": self.SNOWFLAKE_SCHEMA,
                    "SNOWFLAKE_ROLE": self.SNOWFLAKE_ROLE,
                }.items() if not val
            ]
            if missing:
                raise ValueError(
                    f"Missing required Snowflake config: {', '.join(missing)}. "
                    "Set these in your .env file or as environment variables."
                )
            if self.SNOWFLAKE_ACCOUNT and "-" not in self.SNOWFLAKE_ACCOUNT:
                raise ValueError(
                    f"SNOWFLAKE_ACCOUNT '{self.SNOWFLAKE_ACCOUNT}' looks like a legacy account locator. "
                    "dbt-vitals requires the org-account format, e.g. 'acme-abc12345'. "
                    "Find it at: app.snowflake.com → Admin → Accounts → copy the account identifier."
                )
        return self


def get_config() -> Settings:
    """Load and validate application config from environment / .env file. Exits with error on misconfiguration."""
    try:
        return Settings()
    except Exception as e:
        logger.error(f"CONFIGURATION ERROR: {e}")
        sys.exit(1)
