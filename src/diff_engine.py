import os

import git


class DiffEngine:
    def __init__(self, repo_path="."):
        try:
            self.repo = git.Repo(repo_path, search_parent_directories=True)
        except git.InvalidGitRepositoryError:
            raise Exception(f"Directory {repo_path} is not a valid git repository.")

    def get_deleted_models(self, base_branch: str = "main", target_dir: str = "models/") -> list[str]:
        """
        Compares HEAD against base_branch and returns file paths that were
        deleted (D) or renamed (R) within target_dir.

        In GitHub Actions, the base branch is available via GITHUB_BASE_REF.
        That env var takes priority over the base_branch argument so the Action
        always diffs against the correct PR target without extra config.
        """
        effective_base = os.environ.get("GITHUB_BASE_REF") or base_branch

        if not target_dir.endswith("/"):
            target_dir += "/"

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
                if old_path.endswith(".sql") and old_path.startswith(target_dir):
                    deleted_files.append(old_path)

        return deleted_files
