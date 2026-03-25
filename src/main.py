import os
import sys

from config import get_config
from diff_engine import DiffEngine
from manifest_engine import ManifestEngine
from adapters.factory import get_adapter
from reporter import Reporter, ModelReport


def run_isotrope():
    cfg = get_config()

    try:
        diff = DiffEngine(repo_path=".")
        manifest = ManifestEngine(provided_path=cfg.MANIFEST_PATH)
        adapter = get_adapter(cfg)
    except FileNotFoundError as e:
        print(f"❌ FILESYSTEM ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ INITIALIZATION FAILED: {e}")
        sys.exit(1)

    # In GitHub Actions, HEAD is a detached merge commit — active_branch raises TypeError.
    # Fall back to GITHUB_HEAD_REF which Actions injects with the PR branch name.
    try:
        current_branch = diff.repo.active_branch.name
    except TypeError:
        current_branch = os.environ.get("GITHUB_HEAD_REF", "unknown")

    deleted_paths = diff.get_deleted_models(base_branch=cfg.BASE_BRANCH)

    if not deleted_paths:
        if current_branch == cfg.BASE_BRANCH:
            print(f"ℹ️  Active branch is '{cfg.BASE_BRANCH}'. Isotrope checks feature branches.")
        else:
            print(f"✅ No deleted models detected between '{current_branch}' and '{cfg.BASE_BRANCH}'.")
        adapter.close()
        return

    print(f"🔍 Found {len(deleted_paths)} deleted model(s). Querying warehouse...")

    reports: list[ModelReport] = []

    try:
        for path in deleted_paths:
            table_meta = manifest.get_table(path)

            if table_meta is None:
                reports.append(ModelReport(
                    file_path=path,
                    table_ref=None,
                    exists=False,
                    size_gb=0,
                    last_altered=None,
                    last_read=None,
                ))
                continue

            stats = adapter.get_table_stats(
                table_meta["database"],
                table_meta["schema"],
                table_meta["name"],
            )

            table_ref = f"{table_meta['database']}.{table_meta['schema']}.{table_meta['name']}"

            reports.append(ModelReport(
                file_path=path,
                table_ref=table_ref,
                exists=stats["exists"],
                size_gb=stats.get("size_gb", 0),
                last_altered=stats.get("last_altered"),
                last_read=stats.get("last_read"),
            ))
    finally:
        adapter.close()

    reporter = Reporter(cfg)
    reporter.publish(reports)


if __name__ == "__main__":
    run_isotrope()
