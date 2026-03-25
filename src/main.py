from diff_engine import DiffEngine
from manifest_engine import ManifestEngine
from snowflake_adapter import SnowflakeAdapter  # Ensure your file is named snowflake.py


def run_isotrope():
    print("Isotrope: Commencing Symmetry Check...\n")

    # 1. Initialize Engines
    # Since your dbt repo is in a subfolder for testing, we point to it
    REPO_ROOT = "."
    DBT_ROOT = "test-dbt-repo"
    MANIFEST_PATH = f"{DBT_ROOT}/target/manifest.json"
    MODELS_DIR = f"{DBT_ROOT}/models"

    diff = DiffEngine(repo_path=REPO_ROOT)
    manifest = ManifestEngine(manifest_path=MANIFEST_PATH)

    # 2. Get Deleted Files
    # Note: We compare against 'main'. Adjust if your branch is named differently.
    deleted_paths = diff.get_deleted_models(base_branch="main", target_dir=MODELS_DIR)

    if not deleted_paths:
        print("No asymmetries detected. Warehouse and Code are uniform.")
        return

    # 3. Connect to Snowflake only if we found deletions
    print(f"🔍 Found {len(deleted_paths)} deleted models. Fetching Snowflake vitals...")
    sf = SnowflakeAdapter()

    print("\n| Model File | Snowflake Table | Size (GB) | Last Altered |")
    print("| :--- | :--- | :--- | :--- |")

    for path in deleted_paths:
        # We need to make sure the path matches what's in manifest.json
        # Manifest usually stores paths relative to the dbt project root
        manifest_path = path.replace(f"{DBT_ROOT}/", "")

        table_meta = manifest.get_table(manifest_path)

        if table_meta:
            stats = sf.get_table_stats(
                table_meta["database"], table_meta["schema"], table_meta["name"]
            )
            full_name = (
                f"{table_meta['database']}.{table_meta['schema']}.{table_meta['name']}"
            )
            print(
                f"| {path} | {full_name} | {stats['size_gb']} | {stats['last_altered']} |"
            )
        else:
            print(f"| {path} | [NOT FOUND IN MANIFEST] | - | - |")


if __name__ == "__main__":
    run_isotrope()
