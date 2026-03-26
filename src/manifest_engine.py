# src/manifest_engine.py
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ManifestEngine:
    """Parses a dbt manifest.json into fast lookup maps for warehouse table coordinates and reverse dependency graph."""

    def __init__(self, provided_path: str | None = None) -> None:
        """Load and parse manifest. Uses provided_path if given; otherwise autodiscovers target/manifest.json."""
        self.manifest_path = provided_path or self._discover_manifest()
        self.mapping, self.reverse_deps = self._build_mapping()

    def _discover_manifest(self) -> str:
        """
        Climbs up from the current directory looking for target/manifest.json.
        This allows dbt-vitals to work even if run from a subfolder.
        """
        current_dir = Path(os.getcwd()).resolve()

        for _ in range(5):
            potential_path = current_dir / "target" / "manifest.json"
            if potential_path.exists():
                logger.info(f"Autodiscovered manifest at: {potential_path}")
                return str(potential_path)
            current_dir = current_dir.parent

        raise FileNotFoundError(
            "Could not find 'target/manifest.json'. "
            "Run 'dbt compile' or 'dbt run' to generate it, "
            "or set MANIFEST_PATH explicitly."
        )

    def _build_mapping(self) -> tuple[dict[str, Any], dict[str, list[str]]]:
        """Build and return (mapping, reverse_deps): file_path → table metadata and node_id → downstream names."""
        with open(self.manifest_path, "r") as f:
            data = json.load(f)

        self._check_staleness(data)

        mapping = {}
        # Build reverse dep map: {node_id -> [model_names_that_depend_on_it]}
        reverse_deps = defaultdict(list)

        nodes = data.get("nodes")
        if nodes is None:
            raise ValueError(
                "manifest.json is missing the 'nodes' key. "
                "This may not be a compiled manifest — run 'dbt compile' to generate one."
            )
        for node_id, metadata in nodes.items():
            if metadata.get("resource_type") in ("model", "snapshot", "seed"):
                file_path = metadata.get("original_file_path")
                if not file_path:
                    continue
                mapping[file_path] = {
                    "database": metadata.get("database"),
                    "schema": metadata.get("schema"),
                    "name": metadata.get("alias") or metadata.get("name"),
                    "node_id": node_id,
                    "materialization": metadata.get("config", {}).get("materialized"),
                }

            # Build reverse deps for all model nodes
            if metadata.get("resource_type") == "model":
                dep_name = metadata.get("alias") or metadata.get("name")
                for dep_node_id in metadata.get("depends_on", {}).get("nodes", []):
                    reverse_deps[dep_node_id].append(dep_name)

        if not mapping:
            logger.warning(
                "Manifest loaded but contains no dbt models. "
                "Check that MANIFEST_PATH points to a compiled manifest.json "
                "(run 'dbt compile' first)."
            )

        return mapping, dict(reverse_deps)

    def _check_staleness(self, data: dict[str, Any]) -> None:
        """Warns if the manifest was generated more than 24 hours ago."""
        generated_at_str = data.get("metadata", {}).get("generated_at")
        if not generated_at_str:
            return
        try:
            generated_at = datetime.fromisoformat(
                generated_at_str.replace("Z", "+00:00")
            )
            age = datetime.now(timezone.utc) - generated_at
            if age > timedelta(hours=24):
                logger.warning(
                    f"Manifest is {age.days}d {age.seconds // 3600}h old "
                    f"(generated {generated_at_str[:10]}). Table mappings may be stale. "
                    "Run 'dbt compile' or refresh your manifest download step."
                )
        except (ValueError, TypeError):
            pass  # Unparseable timestamp — skip the check

    def get_table(self, file_path: str | None) -> dict[str, Any] | None:
        """Return warehouse coordinates for a dbt model file path, or None if not in the manifest."""
        return self.mapping.get(file_path)  # type: ignore[arg-type]

    def get_downstream_names(self, file_path: str | None) -> list[str]:
        """
        Returns the names of dbt models that directly depend on this file's model.
        Uses the reverse dependency map built from depends_on.nodes in the manifest.
        Returns an empty list if the model has no dependents or is not in the manifest.
        """
        entry = self.mapping.get(file_path)
        if not entry:
            return []
        node_id = entry.get("node_id")
        return sorted(set(self.reverse_deps.get(node_id, [])))
