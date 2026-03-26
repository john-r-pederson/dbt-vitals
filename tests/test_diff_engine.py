import git
import pytest

from diff_engine import DiffEngine


def _paths(changes):
    """Helper: extract old_path strings from a list of ModelChange objects."""
    return [c.old_path for c in changes]


def _init_repo(tmp_path):
    """Create a git repo with a 'main' branch. Returns the Repo object."""
    repo = git.Repo.init(tmp_path)
    repo.config_writer().set_value("user", "name", "Test").release()
    repo.config_writer().set_value("user", "email", "test@test.com").release()
    # git init defaults to "master" on many systems — rename to "main"
    repo.active_branch.rename("main")
    return repo


def _make_repo(tmp_path):
    """
    Creates a git repo on 'main' with two model files and a non-SQL script,
    then checks out a feature branch and deletes one model.

    main:
        models/staging/stg_users.sql
        models/marts/fct_orders.sql
        scripts/not_a_model.py

    feature/remove-users:
        models/staging/stg_users.sql  -> DELETED
    """
    repo = _init_repo(tmp_path)

    (tmp_path / "models" / "staging").mkdir(parents=True)
    (tmp_path / "models" / "marts").mkdir(parents=True)
    (tmp_path / "scripts").mkdir()

    (tmp_path / "models" / "staging" / "stg_users.sql").write_text("select 1")
    (tmp_path / "models" / "marts" / "fct_orders.sql").write_text("select 2")
    (tmp_path / "scripts" / "not_a_model.py").write_text("print('hi')")

    repo.index.add([
        "models/staging/stg_users.sql",
        "models/marts/fct_orders.sql",
        "scripts/not_a_model.py",
    ])
    repo.index.commit("initial commit")

    feature = repo.create_head("feature/remove-users")
    feature.checkout()

    (tmp_path / "models" / "staging" / "stg_users.sql").unlink()
    repo.index.remove(["models/staging/stg_users.sql"])
    repo.index.commit("remove stg_users model")

    return repo


@pytest.fixture
def repo(tmp_path):
    return _make_repo(tmp_path)


@pytest.fixture
def engine(repo):
    return DiffEngine(repo_path=str(repo.working_dir))


def test_deleted_sql_model_is_detected(engine):
    changes = engine.get_deleted_models(base_branch="main")
    assert "models/staging/stg_users.sql" in _paths(changes)


def test_deleted_model_has_no_new_path(engine):
    changes = engine.get_deleted_models(base_branch="main")
    change = next(c for c in changes if c.old_path == "models/staging/stg_users.sql")
    assert change.new_path is None


def test_unchanged_model_is_not_returned(engine):
    changes = engine.get_deleted_models(base_branch="main")
    assert "models/marts/fct_orders.sql" not in _paths(changes)


def test_non_sql_file_is_excluded(engine):
    changes = engine.get_deleted_models(base_branch="main")
    assert not any(c.old_path.endswith(".py") for c in changes)


def test_file_outside_models_dir_is_excluded(tmp_path):
    """A deleted SQL file in seeds/ must not appear — only models/ is watched."""
    repo = _init_repo(tmp_path)

    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "seed.sql").write_text("select 1")
    # Also add a models/ file to confirm that dir is unaffected
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "kept.sql").write_text("select 2")

    repo.index.add(["seeds/seed.sql", "models/kept.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-seed")
    branch.checkout()
    (tmp_path / "seeds" / "seed.sql").unlink()
    repo.index.remove(["seeds/seed.sql"])
    repo.index.commit("remove seed")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="models/"
    )
    assert changes == []


def test_similarly_named_dir_is_not_matched(tmp_path):
    """'foo_models/bar.sql' must not match target_dir='models/' — startswith check."""
    repo = _init_repo(tmp_path)

    (tmp_path / "foo_models").mkdir()
    (tmp_path / "foo_models" / "bar.sql").write_text("select 1")
    repo.index.add(["foo_models/bar.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-bar")
    branch.checkout()
    (tmp_path / "foo_models" / "bar.sql").unlink()
    repo.index.remove(["foo_models/bar.sql"])
    repo.index.commit("remove bar")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="models/"
    )
    assert changes == []


def test_renamed_model_is_detected(tmp_path):
    """A renamed .sql file in models/ should appear as a deletion of the old path."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "old_name.sql").write_text("select 1")
    repo.index.add(["models/old_name.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/rename-model")
    branch.checkout()
    (tmp_path / "models" / "old_name.sql").rename(tmp_path / "models" / "new_name.sql")
    repo.index.remove(["models/old_name.sql"])
    repo.index.add(["models/new_name.sql"])
    repo.index.commit("rename model")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert "models/old_name.sql" in _paths(changes)


def test_renamed_model_returns_new_path(tmp_path):
    """A renamed model should expose the destination path via new_path."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "old_name.sql").write_text("select 1")
    repo.index.add(["models/old_name.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/rename-model-new-path")
    branch.checkout()
    (tmp_path / "models" / "old_name.sql").rename(tmp_path / "models" / "new_name.sql")
    repo.index.remove(["models/old_name.sql"])
    repo.index.add(["models/new_name.sql"])
    repo.index.commit("rename model")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    change = next(c for c in changes if c.old_path == "models/old_name.sql")
    assert change.new_path == "models/new_name.sql"


def test_github_base_ref_env_var_takes_priority(engine, monkeypatch):
    """GITHUB_BASE_REF must override the base_branch argument."""
    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    changes = engine.get_deleted_models(base_branch="nonexistent-branch-xyz")
    assert "models/staging/stg_users.sql" in _paths(changes)


def test_base_branch_used_when_env_var_absent(engine, monkeypatch):
    """Without GITHUB_BASE_REF, the explicit base_branch argument is used."""
    monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
    changes = engine.get_deleted_models(base_branch="main")
    assert "models/staging/stg_users.sql" in _paths(changes)


def test_no_deletions_returns_empty_list(tmp_path):
    """A branch with only additions should return an empty list."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "existing.sql").write_text("select 1")
    repo.index.add(["models/existing.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/add-model")
    branch.checkout()
    (tmp_path / "models" / "new_model.sql").write_text("select 2")
    repo.index.add(["models/new_model.sql"])
    repo.index.commit("add model")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert changes == []


def test_deleted_seed_csv_is_detected(tmp_path):
    """A deleted .csv file in seeds/ should be detected alongside .sql models."""
    repo = _init_repo(tmp_path)

    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "ref_countries.csv").write_text("id,name\n1,US\n")
    repo.index.add(["seeds/ref_countries.csv"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-seed")
    branch.checkout()
    (tmp_path / "seeds" / "ref_countries.csv").unlink()
    repo.index.remove(["seeds/ref_countries.csv"])
    repo.index.commit("remove seed")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert "seeds/ref_countries.csv" in _paths(changes)


def test_seed_outside_seeds_dir_not_detected(tmp_path):
    """A .csv in a non-seeds directory must not be detected."""
    repo = _init_repo(tmp_path)

    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "extra.csv").write_text("x,y\n")
    repo.index.add(["data/extra.csv"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-csv")
    branch.checkout()
    (tmp_path / "data" / "extra.csv").unlink()
    repo.index.remove(["data/extra.csv"])
    repo.index.commit("remove csv")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert changes == []


def test_invalid_repo_raises(tmp_path):
    with pytest.raises(Exception, match="not a valid git repository"):
        DiffEngine(repo_path=str(tmp_path))


def test_empty_repo_returns_empty_list(tmp_path):
    """A repo with no commits should return [] gracefully rather than raising."""
    _init_repo(tmp_path)  # initialises the repo but makes no commits
    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert changes == []


def test_missing_base_branch_raises(tmp_path):
    """When neither the bare branch nor origin/<branch> exist, raise a descriptive error."""
    _make_repo(tmp_path)  # local-only repo — no remote, no origin/
    engine = DiffEngine(repo_path=str(tmp_path))
    with pytest.raises(Exception, match="not found"):
        engine.get_deleted_models(base_branch="nonexistent-branch-xyz")


def test_falls_back_to_origin_prefix_when_local_branch_missing(tmp_path):
    """
    In GitHub Actions, checkout@v4 does not create a local branch for the base
    ref — only origin/<base> exists.  DiffEngine must retry with origin/<base>
    when the plain branch name raises GitCommandError.

    Setup: clone directly from an upstream repo (no bare remote needed).
    The clone has origin/main but the local main branch is deleted to
    simulate the GitHub Actions checkout state.
    """
    # Build the upstream repo with a model on main
    upstream_path = tmp_path / "upstream"
    upstream_path.mkdir()
    upstream_repo = git.Repo.init(upstream_path)
    upstream_repo.config_writer().set_value("user", "name", "Test").release()
    upstream_repo.config_writer().set_value("user", "email", "test@test.com").release()
    upstream_repo.active_branch.rename("main")

    (upstream_path / "models").mkdir()
    (upstream_path / "models" / "stg_users.sql").write_text("select 1")
    upstream_repo.index.add(["models/stg_users.sql"])
    upstream_repo.index.commit("initial")

    # Clone directly from the upstream (origin/main is automatically created)
    clone_path = tmp_path / "clone"
    clone_repo = git.Repo.clone_from(str(upstream_path), str(clone_path))
    clone_repo.config_writer().set_value("user", "name", "Test").release()
    clone_repo.config_writer().set_value("user", "email", "test@test.com").release()

    # Create a feature branch that deletes the model
    clone_repo.git.checkout("-b", "feature/remove-users")
    (clone_path / "models" / "stg_users.sql").unlink()
    clone_repo.index.remove(["models/stg_users.sql"])
    clone_repo.index.commit("remove stg_users")

    # Delete the local main branch to simulate the CI checkout@v4 state
    clone_repo.delete_head("main", force=True)

    # Verify setup: no local main, but origin/main exists
    assert "main" not in [h.name for h in clone_repo.heads]
    assert any(r.name == "origin/main" for r in clone_repo.remotes["origin"].refs)

    changes = DiffEngine(repo_path=str(clone_path)).get_deleted_models(base_branch="main")
    assert "models/stg_users.sql" in _paths(changes)


# ---------------------------------------------------------------------------
# YAML / schema file deletions
# ---------------------------------------------------------------------------

def test_yaml_only_deletion_is_detected(tmp_path):
    """Deleting a .yml without deleting the paired .sql means the model still exists — report it."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "stg_users.sql").write_text("select 1")
    (tmp_path / "models" / "stg_users.yml").write_text("version: 2")
    repo.index.add(["models/stg_users.sql", "models/stg_users.yml"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-yaml")
    branch.checkout()
    (tmp_path / "models" / "stg_users.yml").unlink()
    repo.index.remove(["models/stg_users.yml"])
    repo.index.commit("remove yaml")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert "models/stg_users.yml" in _paths(changes)


def test_yaml_only_deletion_sets_lookup_path(tmp_path):
    """A YAML-only deletion must set lookup_path to the paired .sql path for manifest lookup."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "stg_users.sql").write_text("select 1")
    (tmp_path / "models" / "stg_users.yml").write_text("version: 2")
    repo.index.add(["models/stg_users.sql", "models/stg_users.yml"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-yaml-lookup")
    branch.checkout()
    (tmp_path / "models" / "stg_users.yml").unlink()
    repo.index.remove(["models/stg_users.yml"])
    repo.index.commit("remove yaml")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    change = next(c for c in changes if c.old_path == "models/stg_users.yml")
    assert change.lookup_path == "models/stg_users.sql"


def test_yaml_deleted_with_sql_is_not_duplicated(tmp_path):
    """When both .yml and .sql are deleted in the same PR, only the .sql change is reported."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "stg_users.sql").write_text("select 1")
    (tmp_path / "models" / "stg_users.yml").write_text("version: 2")
    repo.index.add(["models/stg_users.sql", "models/stg_users.yml"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-both")
    branch.checkout()
    (tmp_path / "models" / "stg_users.sql").unlink()
    (tmp_path / "models" / "stg_users.yml").unlink()
    repo.index.remove(["models/stg_users.sql", "models/stg_users.yml"])
    repo.index.commit("remove model and yaml")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    paths = _paths(changes)
    assert "models/stg_users.sql" in paths
    assert "models/stg_users.yml" not in paths


def test_yaml_outside_target_dir_not_detected(tmp_path):
    """A .yml deletion outside the watched directory must not be reported."""
    repo = _init_repo(tmp_path)

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "profiles.yml").write_text("version: 2")
    repo.index.add(["config/profiles.yml"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-config")
    branch.checkout()
    (tmp_path / "config" / "profiles.yml").unlink()
    repo.index.remove(["config/profiles.yml"])
    repo.index.commit("remove config yaml")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert changes == []


# ---------------------------------------------------------------------------
# Multi-path target_dir
# ---------------------------------------------------------------------------

def test_multi_path_target_dir_comma_separated(tmp_path):
    """target_dir='models/,snapshots/' detects deletions in both directories."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "models" / "stg_users.sql").write_text("select 1")
    (tmp_path / "snapshots" / "snap_orders.sql").write_text("select 2")
    repo.index.add(["models/stg_users.sql", "snapshots/snap_orders.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-both-dirs")
    branch.checkout()
    (tmp_path / "models" / "stg_users.sql").unlink()
    (tmp_path / "snapshots" / "snap_orders.sql").unlink()
    repo.index.remove(["models/stg_users.sql", "snapshots/snap_orders.sql"])
    repo.index.commit("remove model and snapshot")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="models/,snapshots/"
    )
    paths = _paths(changes)
    assert "models/stg_users.sql" in paths
    assert "snapshots/snap_orders.sql" in paths


def test_multi_path_target_dir_as_list(tmp_path):
    """target_dir accepts a Python list as well as a comma-separated string."""
    repo = _init_repo(tmp_path)

    (tmp_path / "models").mkdir()
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "models" / "stg.sql").write_text("select 1")
    (tmp_path / "snapshots" / "snap.sql").write_text("select 2")
    repo.index.add(["models/stg.sql", "snapshots/snap.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/multi-list")
    branch.checkout()
    (tmp_path / "snapshots" / "snap.sql").unlink()
    repo.index.remove(["snapshots/snap.sql"])
    repo.index.commit("remove snapshot")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir=["models/", "snapshots/"]
    )
    assert "snapshots/snap.sql" in _paths(changes)
    assert "models/stg.sql" not in _paths(changes)


def test_snapshot_dir_detected_with_target_dir(tmp_path):
    """Snapshot deletions are detected when snapshots/ is in target_dir."""
    repo = _init_repo(tmp_path)

    (tmp_path / "snapshots").mkdir()
    (tmp_path / "snapshots" / "snap_users.sql").write_text("select 1")
    repo.index.add(["snapshots/snap_users.sql"])
    repo.index.commit("initial")

    branch = repo.create_head("feature/remove-snapshot")
    branch.checkout()
    (tmp_path / "snapshots" / "snap_users.sql").unlink()
    repo.index.remove(["snapshots/snap_users.sql"])
    repo.index.commit("remove snapshot")

    changes = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="snapshots/"
    )
    assert "snapshots/snap_users.sql" in _paths(changes)
