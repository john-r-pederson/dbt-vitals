# src/base_adapter.py
from abc import ABC, abstractmethod


class BaseWarehouseAdapter(ABC):
    @abstractmethod
    def get_table_stats(self, db, schema, table):
        """Must return a dict with size_gb and last_altered."""
        pass
