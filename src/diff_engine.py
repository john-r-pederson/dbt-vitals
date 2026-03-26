import logging
import os
from dataclasses import dataclass

import git

logger = logging.getLogger(__name__)


@dataclass
class ModelChange:
    """Represents a deleted or renamed dbt model file."""
    old_path: str
    new_path: str | None  # None = pure deletion; set = renamed to this path
    lookup_path: str | None = None  # if set, use for manifest lookup instead of old_path
                                    # (populated for YAML-only deletions whose .sql still exists)


class DiffEngine:
    """Detects deleted and renamed dbt model files between HEAD and a base branch using gitpython."""

    def __init__(self, repo_path: str = ".") -> None:
        """Open the git repo at repo_path. Raises Exception if not a valid git repository."""
        try:
            self.repo = git.Repo(repo_path, search_parent_directories=True)
        except git.InvalidGitRepositoryError:
            raise Exception(f"Directory {repo_path} is not a valid git repository.")

    def get_deleted_models(
        self,
        base_branch: str = "main",
        target_dir: str | list[str] = "models/",
        seeds_dir: str = "seeds/",
    ) -> list[ModelChange]:
        """
        Compares HEAD against base_branch and returns ModelChange objects for files
        that were deleted (D) or renamed (R) within target_dir (.sql, .yml/.yaml) or
        seeds_dir (.csv).

        target_dir may be a comma-separated string or a list to watch multiple
        directories simultaneously (e.g. "models/,snapshots/" catches both).

        YAML (.yml/.yaml) deletions are reported only when the corresponding .sql file
        was NOT also deleted in the same PR — meaning the model still exists in the
        warehouse but its schema config was removed.  When both files change together
        (co-rename or co-delete), the .sql change is the primary signal and the YAML
        is suppressed to avoid duplicate rows.

        For renames, ModelChange.new_path contains the destination path.
        For pure deletions, ModelChange.new_path is None.
        For YAML-only changes, ModelChange.lookup_path holds the derived .sql path
        used for manifest lookup (manifests map .sql paths, not .yml paths).

        In GitHub Actions, the base branch is available via GITHUB_BASE_REF.
        That env var takes priority over the base_branch argument so the Action
        always diffs against the correct PR target without extra config.
        """
        effective_base = os.environ.get("GITHUB_BASE_REF") or base_branch

        # Normalise target_dir to a list of slash-terminated directory prefixes.
        # Accepts a comma-separated string ("models/,snapshots/") or a Python list.
        if isinstance(target_dir, str):
            dirs = [d.strip() for d in target_dir.split(",") if d.strip()]
        else:
            dirs = list(target_dir)
        dirs = [d if d.endswith("/") else d + "/" for d in dirs]

        if seeds_dir and not seeds_dir.endswith("/"):
            seeds_dir += "/"

        # R=True reverses the diff direction to "base → HEAD", so a file
        # deleted in this branch (missing in HEAD, present in base) appears
        # as change_type "D" rather than "A".
        #
        # In GitHub Actions, checkout@v4 does not create a local branch for
        # the base ref — only origin/<base> exists. Try the bare name first;
        # if git cannot resolve it, fall back to origin/<base>.
        try:
            head = self.repo.head.commit
        except ValueError:
            logger.warning("Repository has no commits — nothing to diff.")
            return []

        try:
            diff_index = head.diff(effective_base, R=True)
        except git.GitCommandError:
            try:
                diff_index = head.diff(f"origin/{effective_base}", R=True)
            except git.GitCommandError:
                raise Exception(
                    f"Base branch '{effective_base}' not found locally or as "
                    f"'origin/{effective_base}'. "
                    "Verify BASE_BRANCH or GITHUB_BASE_REF is set correctly."
                )

        def _in_sql_dirs(path: str) -> bool:
            return any(path.startswith(d) for d in dirs)

        changes: list[ModelChange] = []
        # SQL paths covered by a ModelChange in pass 1; used in pass 2 to suppress
        # YAML entries whose paired .sql was already accounted for.
        deleted_sql_paths: set[str] = set()

        # ── Pass 1: .sql models/snapshots and .csv seeds ────────────────────
        for diff in diff_index:
            if diff.change_type not in ("D", "R"):
                continue
            old_path = diff.a_path
            is_sql = old_path.endswith(".sql") and _in_sql_dirs(old_path)
            is_seed = bool(seeds_dir) and old_path.endswith(".csv") and old_path.startswith(seeds_dir)
            if not (is_sql or is_seed):
                continue

            if diff.change_type == "R":
                b = diff.b_path or ""
                new_path = b if (b.endswith(".sql") or b.endswith(".csv")) else None
                changes.append(ModelChange(old_path=old_path, new_path=new_path))
            else:
                changes.append(ModelChange(old_path=old_path, new_path=None))

            if is_sql:
                deleted_sql_paths.add(old_path)

        # ── Pass 2: .yml/.yaml schema files (standalone changes only) ───────
        # A YAML deletion is only worth reporting when the paired .sql model still
        # exists — it means the schema config was removed but the table is still live.
        renamed_sql_destinations: set[str] = {
            c.new_path for c in changes if c.new_path and c.new_path.endswith(".sql")
        }
        for diff in diff_index:
            if diff.change_type not in ("D", "R"):
                continue
            old_path = diff.a_path
            if not (old_path.endswith(".yml") or old_path.endswith(".yaml")):
                continue
            if not _in_sql_dirs(old_path):
                continue

            base, _ = os.path.splitext(old_path)
            sql_path = base + ".sql"

            # The corresponding .sql was deleted/renamed in this PR — the SQL
            # change is the primary signal; skip to avoid a duplicate row.
            if sql_path in deleted_sql_paths:
                continue

            if diff.change_type == "R":
                b = diff.b_path or ""
                # If the YAML is being renamed and the same-stem .sql is also being
                # renamed (co-rename), the SQL rename already covers this model.
                new_base, _ = os.path.splitext(b)
                if (new_base + ".sql") in renamed_sql_destinations:
                    continue
                new_path: str | None = b if (b.endswith(".yml") or b.endswith(".yaml")) else None
            else:
                new_path = None

            # YAML changed while the SQL model still exists — report on the table.
            # lookup_path carries the .sql path so ManifestEngine can find the entry.
            changes.append(ModelChange(old_path=old_path, new_path=new_path, lookup_path=sql_path))

        return changes
