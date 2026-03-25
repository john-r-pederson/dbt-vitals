import sys
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    SNOWFLAKE_PRIVATE_KEY: Optional[str] = Field(default=None)
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: Optional[str] = Field(default=None)
    SNOWFLAKE_PASSWORD: Optional[str] = Field(default=None)
    SNOWFLAKE_HOST: Optional[str] = None
    SNOWFLAKE_AUTHENTICATOR: str = "externalbrowser"

    # --- GitHub (injected by GitHub Actions) ---
    GITHUB_TOKEN: Optional[str] = None
    GITHUB_REPOSITORY: Optional[str] = None  # "owner/repo"
    PR_NUMBER: Optional[str] = None

    # --- Isotrope Behavior ---
    BASE_BRANCH: str = "main"
    MANIFEST_PATH: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    @model_validator(mode="after")
    def check_warehouse_credentials(self) -> "Settings":
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
        return self


def get_config() -> Settings:
    try:
        return Settings()
    except Exception as e:
        print(f"CONFIGURATION ERROR: {e}")
        sys.exit(1)
