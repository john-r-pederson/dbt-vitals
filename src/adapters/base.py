from abc import ABC, abstractmethod


class BaseWarehouseAdapter(ABC):
    @abstractmethod
    def get_table_stats(self, db: str, schema: str, table: str) -> dict:
        """
        Returns vital signs for a warehouse table.

        Return shape:
        {
            "exists": bool,
            "size_gb": float,
            "last_altered": str | None,   # ISO 8601
            "last_read": str | None,      # ISO 8601, None if unavailable
        }
        """

    @abstractmethod
    def close(self) -> None:
        """Clean up connections."""
