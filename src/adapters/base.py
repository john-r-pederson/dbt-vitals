from abc import ABC, abstractmethod
from typing import Any


class BaseWarehouseAdapter(ABC):
    """Abstract base class for warehouse adapters. Implement get_table_stats() and close()."""

    @abstractmethod
    def get_table_stats(self, db: str, schema: str, table: str) -> dict[str, Any]:
        """
        Returns vital signs for a warehouse table.

        Return shape:
        {
            "exists": bool,
            "size_gb": float | None,           # None for views (no storage)
            "last_altered": str | None,        # YYYY-MM-DD
            "last_read": str | None,           # YYYY-MM-DD, None if no reads in lookback window
            "read_count": int,                 # distinct queries in lookback window (default 90d)
            "distinct_users": int,             # distinct Snowflake users in lookback window
            "access_history_available": bool,  # False if role lacks IMPORTED PRIVILEGES
            "table_type": str | None,          # "BASE TABLE", "VIEW", "EXTERNAL TABLE", etc.
            "query_error": bool,               # True if INFORMATION_SCHEMA query failed (permissions, not absence)
        }
        """

    @abstractmethod
    def close(self) -> None:
        """Clean up connections."""
