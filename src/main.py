import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

from config import get_config
from diff_engine import DiffEngine
from manifest_engine import ManifestEngine
from adapters.factory import get_adapter
from reporter import Reporter, ModelReport


def run() -> None:
    """Entry point: diffs HEAD against base branch, queries warehouse stats for each deleted model, and posts a PR comment."""
    cfg = get_config()

    # Allow users to suppress the report by adding [skip dbt-vitals] to the PR title.
    if cfg.PR_TITLE and "[skip dbt-vitals]" in cfg.PR_TITLE.lower():
        logger.info("[skip dbt-vitals] detected in PR title. Skipping warehouse check.")
        return

    try:
        diff = DiffEngine(repo_path=".")
        manifest = ManifestEngine(provided_path=cfg.MANIFEST_PATH)
        adapter = get_adapter(cfg)
    except FileNotFoundError as e:
        logger.error(f"FILESYSTEM ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"INITIALIZATION FAILED: {e}")
        sys.exit(1)

    # In GitHub Actions, HEAD is a detached merge commit — active_branch raises TypeError.
    # Fall back to GITHUB_HEAD_REF which Actions injects with the PR branch name.
    try:
        current_branch = diff.repo.active_branch.name
    except TypeError:
        current_branch = os.environ.get("GITHUB_HEAD_REF", "unknown")

    target_dir = cfg.TARGET_DIR
    changes = diff.get_deleted_models(base_branch=cfg.BASE_BRANCH, target_dir=target_dir)

    if not changes:
        if current_branch == cfg.BASE_BRANCH:
            logger.info(f"Active branch is '{cfg.BASE_BRANCH}'. dbt-vitals checks feature branches.")
        else:
            logger.info(f"No deleted models detected between '{current_branch}' and '{cfg.BASE_BRANCH}'.")
        adapter.close()
        return

    logger.info(f"Found {len(changes)} deleted/renamed model(s). Querying warehouse...")

    reports: list[ModelReport] = []

    try:
        for change in changes:
            try:
                # Monorepo support: strip a repo subdirectory prefix before manifest lookup
                lookup_path = change.old_path
                if cfg.REPO_SUBDIRECTORY:
                    prefix = cfg.REPO_SUBDIRECTORY.rstrip("/") + "/"
                    lookup_path = lookup_path.removeprefix(prefix)

                table_meta = manifest.get_table(lookup_path)
                downstream_names = manifest.get_downstream_names(lookup_path)

                if table_meta is None:
                    reports.append(ModelReport(
                        file_path=change.old_path,
                        new_path=change.new_path,
                        table_ref=None,
                        exists=False,
                        table_type=None,
                        materialization=None,
                        size_gb=None,
                        last_altered=None,
                        last_read=None,
                        downstream_names=downstream_names,
                    ))
                    continue

                table_ref = f"{table_meta['database']}.{table_meta['schema']}.{table_meta['name']}"

                stats = adapter.get_table_stats(
                    table_meta["database"],
                    table_meta["schema"],
                    table_meta["name"],
                )

                if not stats["exists"]:
                    logger.warning(
                        f"{table_ref} not found in INFORMATION_SCHEMA. "
                        "This may mean the table doesn't exist yet, or that the configured role "
                        "lacks REFERENCES on this table/database."
                    )

                reports.append(ModelReport(
                    file_path=change.old_path,
                    new_path=change.new_path,
                    table_ref=table_ref,
                    exists=stats["exists"],
                    table_type=stats.get("table_type"),
                    materialization=table_meta.get("materialization"),
                    size_gb=stats.get("size_gb"),
                    last_altered=stats.get("last_altered"),
                    last_read=stats.get("last_read"),
                    read_count=stats.get("read_count", 0),
                    distinct_users=stats.get("distinct_users", 0),
                    access_history_available=stats.get("access_history_available", True),
                    downstream_names=downstream_names,
                    query_error=stats.get("query_error", False),
                ))
            except Exception as e:
                logger.error(f"Failed to process {change.old_path}: {e}")
                reports.append(ModelReport(
                    file_path=change.old_path,
                    new_path=change.new_path,
                    table_ref=None,
                    exists=False,
                    table_type=None,
                    materialization=None,
                    size_gb=None,
                    last_altered=None,
                    last_read=None,
                    read_count=0,
                    distinct_users=0,
                    access_history_available=False,
                    downstream_names=[],
                    query_error=True,
                ))
    finally:
        adapter.close()

    reporter = Reporter(cfg)
    reporter.publish(reports)


if __name__ == "__main__":
    run()
