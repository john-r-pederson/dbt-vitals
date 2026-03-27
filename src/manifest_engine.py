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
        self.mapping, self.reverse_deps, self.node_names = self._build_mapping()

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

    def _build_mapping(self) -> tuple[dict[str, Any], dict[str, list[str]], dict[str, str]]:
        """Build and return (mapping, reverse_deps, node_names).

        mapping:      file_path → table metadata
        reverse_deps: upstream_node_id → [downstream_node_ids]  (edges point toward consumers)
        node_names:   node_id → display name (alias or name)
        """
        with open(self.manifest_path, "r") as f:
            data = json.load(f)

        self._check_staleness(data)

        mapping: dict[str, Any] = {}
        # Edges point toward consumers: upstream_id → [child_node_ids]
        reverse_deps: defaultdict[str, list[str]] = defaultdict(list)
        # Display name for each node, used at the end of BFS traversal
        node_names: dict[str, str] = {}

        nodes = data.get("nodes")
        if nodes is None:
            raise ValueError(
                "manifest.json is missing the 'nodes' key. "
                "This may not be a compiled manifest — run 'dbt compile' to generate one."
            )
        for node_id, metadata in nodes.items():
            resource_type = metadata.get("resource_type")
            display_name = metadata.get("alias") or metadata.get("name")

            if resource_type in ("model", "snapshot", "seed"):
                file_path = metadata.get("original_file_path")
                if not file_path:
                    continue
                mapping[file_path] = {
                    "database": metadata.get("database"),
                    "schema": metadata.get("schema"),
                    "name": display_name,
                    "node_id": node_id,
                    "materialization": metadata.get("config", {}).get("materialized"),
                }
                node_names[node_id] = display_name

            # Build reverse edges for model and snapshot nodes so we can traverse
            # the full consumer graph in get_downstream_names.
            if resource_type in ("model", "snapshot"):
                for dep_node_id in metadata.get("depends_on", {}).get("nodes", []):
                    reverse_deps[dep_node_id].append(node_id)

        if not mapping:
            logger.warning(
                "Manifest loaded but contains no dbt models. "
                "Check that MANIFEST_PATH points to a compiled manifest.json "
                "(run 'dbt compile' first)."
            )

        return mapping, dict(reverse_deps), node_names

    def _check_staleness(self, data: dict[str, Any]) -> None:
        """Logs manifest schema version and warns if the manifest was generated more than 24 hours ago."""
        metadata = data.get("metadata", {})

        schema_version = metadata.get("dbt_schema_version", "")
        if schema_version:
            logger.info(f"Manifest schema version: {schema_version}")
            if "/manifest/v1" not in schema_version:
                logger.warning(
                    f"Unexpected manifest schema version: {schema_version}. "
                    "dbt-vitals was tested against v1x schemas — output may be incorrect."
                )

        generated_at_str = metadata.get("generated_at")
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
        Returns the display names of all nodes that transitively depend on this model,
        using breadth-first traversal of the reverse dependency graph.

        Includes direct dependents and all indirect consumers reachable through them.
        Returns an empty list if the model has no dependents or is not in the manifest.
        The visited set prevents infinite loops in the (theoretically impossible but
        defensively handled) case of a cycle in the graph.
        """
        entry = self.mapping.get(file_path)
        if not entry:
            return []

        start_id = entry.get("node_id")
        visited: set[str] = set()
        queue: list[str] = [start_id]
        result_names: list[str] = []

        while queue:
            current_id = queue.pop(0)
            for child_id in self.reverse_deps.get(current_id, []):
                if child_id not in visited:
                    visited.add(child_id)
                    name = self.node_names.get(child_id)
                    if name:
                        result_names.append(name)
                    queue.append(child_id)

        return sorted(set(result_names))
