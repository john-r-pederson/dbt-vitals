import pytest
import git
from diff_engine import DiffEngine


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
    deleted = engine.get_deleted_models(base_branch="main")
    assert "models/staging/stg_users.sql" in deleted


def test_unchanged_model_is_not_returned(engine):
    deleted = engine.get_deleted_models(base_branch="main")
    assert "models/marts/fct_orders.sql" not in deleted


def test_non_sql_file_is_excluded(engine):
    deleted = engine.get_deleted_models(base_branch="main")
    assert not any(p.endswith(".py") for p in deleted)


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

    deleted = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="models/"
    )
    assert deleted == []


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

    deleted = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(
        base_branch="main", target_dir="models/"
    )
    assert deleted == []


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

    deleted = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert "models/old_name.sql" in deleted


def test_github_base_ref_env_var_takes_priority(engine, monkeypatch):
    """GITHUB_BASE_REF must override the base_branch argument."""
    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    deleted = engine.get_deleted_models(base_branch="nonexistent-branch-xyz")
    assert "models/staging/stg_users.sql" in deleted


def test_base_branch_used_when_env_var_absent(engine, monkeypatch):
    """Without GITHUB_BASE_REF, the explicit base_branch argument is used."""
    monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
    deleted = engine.get_deleted_models(base_branch="main")
    assert "models/staging/stg_users.sql" in deleted


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

    deleted = DiffEngine(repo_path=str(tmp_path)).get_deleted_models(base_branch="main")
    assert deleted == []


def test_invalid_repo_raises(tmp_path):
    with pytest.raises(Exception, match="not a valid git repository"):
        DiffEngine(repo_path=str(tmp_path))
