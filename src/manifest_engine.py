import json
import os


class ManifestEngine:
    def __init__(self, manifest_path="../target/manifest.json"):
        self.manifest_path = manifest_path
        self.mapping = self._build_mapping()

    def _build_mapping(self):
        """Creates a lookup dict: {file_path: snowflake_address}"""
        if not os.path.exists(self.manifest_path):
            raise FileNotFoundError(f"Manifest not found at {self.manifest_path}")

        with open(self.manifest_path, "r") as f:
            data = json.load(f)

        mapping = {}
        nodes = data.get("nodes", {})

        for node_id, metadata in nodes.items():
            if metadata.get("resource_type") == "model":
                file_path = metadata.get("original_file_path")
                address = {
                    "database": metadata.get("database"),
                    "schema": metadata.get("schema"),
                    "name": metadata.get("alias") or metadata.get("name"),
                }
                mapping[file_path] = address

        return mapping

    def get_table(self, file_path):
        """Returns the warehouse address for a given dbt file path."""
        return self.mapping.get(file_path)


if __name__ == "__main__":
    engine = ManifestEngine()
    test_path = "models/staging/stg_users.sql"
    result = engine.get_table(test_path)

    if result:
        print(
            f"SUCCESS: {test_path} maps to {result['database']}.{result['schema']}.{result['name']}"
        )
    else:
        print(f"FAILED: Could not find mapping for {test_path}")
