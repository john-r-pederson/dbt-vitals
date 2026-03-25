import git


class DiffEngine:
    def __init__(self, repo_path="."):
        """
        repo_path should be the root of your git repository.
        """
        try:
            self.repo = git.Repo(repo_path, search_parent_directories=True)
        except git.InvalidGitRepositoryError:
            raise Exception(f"Directory {repo_path} is not a valid git repository.")

    def get_deleted_models(self, base_branch="main", target_dir="models/"):
        """
        Compares the current branch (HEAD) against the base_branch (e.g., 'main').
        Returns a list of file paths that were Deleted (D) or Renamed (R).
        """
        # Ensure target_dir has a trailing slash for matching
        if not target_dir.endswith("/"):
            target_dir += "/"

        # Compare current HEAD to the base branch
        # This shows us what changed IN our branch relative to main
        diff_index = self.repo.head.commit.diff(base_branch)

        deleted_files = []

        for diff in diff_index:
            # Change types: 'D' = Deleted, 'R' = Renamed
            if diff.change_type in ("D", "R"):
                # 'a_path' is the path of the file in the base branch (the old file)
                old_path = diff.a_path

                # Check if it's a .sql file inside our target directory
                # Note: dbt-test-repo/models/... might need adjustment depending on where you run this
                if old_path.endswith(".sql") and target_dir in old_path:
                    deleted_files.append(old_path)

        return deleted_files


# --- Local Test ---
if __name__ == "__main__":
    engine = DiffEngine()
    # Change 'main' to your actual default branch name if different
    orphans = engine.get_deleted_models(base_branch="main")
    print(f"Found {len(orphans)} potential orphans:")
    for path in orphans:
        print(f" - {path}")
