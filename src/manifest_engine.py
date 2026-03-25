# src/manifest_engine.py
import json
import os
from pathlib import Path


class ManifestEngine:
    def __init__(self, provided_path=None):
        self.manifest_path = provided_path or self._discover_manifest()
        self.mapping = self._build_mapping()

    def _discover_manifest(self):
        """
        Climbs up from the current directory looking for target/manifest.json.
        This allows Isotrope to work even if run from a subfolder.
        """
        current_dir = Path(os.getcwd()).resolve()

        for _ in range(5):
            potential_path = current_dir / "target" / "manifest.json"
            if potential_path.exists():
                print(f"✨ Autodiscovered manifest at: {potential_path}")
                return str(potential_path)
            current_dir = current_dir.parent

        raise FileNotFoundError(
            "Could not find 'target/manifest.json'. "
            "Run 'dbt compile' or 'dbt run' to generate it, "
            "or set MANIFEST_PATH explicitly."
        )

    def _build_mapping(self):
        with open(self.manifest_path, "r") as f:
            data = json.load(f)

        mapping = {}
        nodes = data.get("nodes", {})
        for node_id, metadata in nodes.items():
            if metadata.get("resource_type") == "model":
                file_path = metadata.get("original_file_path")
                if not file_path:
                    continue
                mapping[file_path] = {
                    "database": metadata.get("database"),
                    "schema": metadata.get("schema"),
                    "name": metadata.get("alias") or metadata.get("name"),
                }
        return mapping

    def get_table(self, file_path):
        return self.mapping.get(file_path)
