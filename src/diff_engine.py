import os
from dataclasses import dataclass

import git


@dataclass
class ModelChange:
    """Represents a deleted or renamed dbt model file."""
    old_path: str
    new_path: str | None  # None = pure deletion; set = renamed to this path


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
        target_dir: str = "models/",
        seeds_dir: str = "seeds/",
    ) -> list[ModelChange]:
        """
        Compares HEAD against base_branch and returns ModelChange objects for files
        that were deleted (D) or renamed (R) within target_dir (.sql) or seeds_dir (.csv).

        For renames, ModelChange.new_path contains the destination path.
        For pure deletions, ModelChange.new_path is None.

        In GitHub Actions, the base branch is available via GITHUB_BASE_REF.
        That env var takes priority over the base_branch argument so the Action
        always diffs against the correct PR target without extra config.
        """
        effective_base = os.environ.get("GITHUB_BASE_REF") or base_branch

        if not target_dir.endswith("/"):
            target_dir += "/"
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
            diff_index = self.repo.head.commit.diff(effective_base, R=True)
        except git.GitCommandError:
            diff_index = self.repo.head.commit.diff(f"origin/{effective_base}", R=True)

        deleted_files = []
        for diff in diff_index:
            if diff.change_type in ("D", "R"):
                old_path = diff.a_path
                is_sql_model = old_path.endswith(".sql") and old_path.startswith(target_dir)
                is_seed = seeds_dir and old_path.endswith(".csv") and old_path.startswith(seeds_dir)
                if not (is_sql_model or is_seed):
                    continue

                if diff.change_type == "R":
                    # b_path is the new file path after the rename
                    b = diff.b_path or ""
                    new_path = b if (b.endswith(".sql") or b.endswith(".csv")) else None
                    deleted_files.append(ModelChange(old_path=old_path, new_path=new_path))
                else:
                    deleted_files.append(ModelChange(old_path=old_path, new_path=None))

        return deleted_files
