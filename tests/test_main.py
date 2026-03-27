from unittest.mock import MagicMock, patch

import pytest

import main
from diff_engine import ModelChange

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cfg(monkeypatch):
    """Minimal Settings-like object sufficient for run()."""
    cfg = MagicMock()
    cfg.BASE_BRANCH = "main"
    cfg.MANIFEST_PATH = None
    cfg.GITHUB_TOKEN = None
    cfg.GITHUB_REPOSITORY = None
    cfg.PR_NUMBER = None
    cfg.PR_TITLE = None
    cfg.REPO_SUBDIRECTORY = None
    cfg.TARGET_DIR = "models/"
    return cfg


def _deleted(path):
    """Helper: a pure deletion ModelChange."""
    return ModelChange(old_path=path, new_path=None)


def _renamed(old, new):
    """Helper: a rename ModelChange."""
    return ModelChange(old_path=old, new_path=new)


# ---------------------------------------------------------------------------
# Skip label
# ---------------------------------------------------------------------------

def test_skip_label_in_pr_title_exits_early(mock_cfg):
    mock_cfg.PR_TITLE = "refactor: remove stg_users [skip dbt-vitals]"

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.get_adapter") as MockAdapter:

        main.run()

    MockDiff.assert_not_called()
    MockAdapter.assert_not_called()


def test_skip_label_case_insensitive(mock_cfg):
    mock_cfg.PR_TITLE = "refactor: cleanup [SKIP DBT-VITALS]"

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.get_adapter") as MockAdapter:

        main.run()

    MockDiff.assert_not_called()
    MockAdapter.assert_not_called()


def test_no_skip_label_proceeds_normally(mock_cfg):
    mock_cfg.PR_TITLE = "refactor: remove stg_users"
    mock_adapter = MagicMock()
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine"), \
         patch("main.get_adapter", return_value=mock_adapter) as MockGetAdapter, \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = []
        MockDiff.return_value.repo.active_branch.name = "feature/test"

        main.run()

    # No models deleted — adapter should never be initialized
    MockGetAdapter.assert_not_called()


# ---------------------------------------------------------------------------
# No deleted models
# ---------------------------------------------------------------------------

def test_no_deleted_models_skips_adapter_and_publish(mock_cfg):
    mock_adapter = MagicMock()
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine"), \
         patch("main.get_adapter", return_value=mock_adapter) as MockGetAdapter, \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = []
        MockDiff.return_value.repo.active_branch.name = "feature/test"

        main.run()

    # Adapter must not be initialized — no Snowflake connection for PRs with no deletions
    MockGetAdapter.assert_not_called()
    mock_reporter.publish.assert_not_called()


# ---------------------------------------------------------------------------
# Deleted model not in manifest
# ---------------------------------------------------------------------------

def test_deleted_model_not_in_manifest_reports_null_table_ref(mock_cfg):
    mock_adapter = MagicMock()
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [_deleted("models/stg_users.sql")]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = None
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    assert len(reports) == 1
    assert reports[0].file_path == "models/stg_users.sql"
    assert reports[0].table_ref is None
    mock_adapter.close.assert_called_once()


# ---------------------------------------------------------------------------
# Deleted model in manifest but not in warehouse
# ---------------------------------------------------------------------------

def test_deleted_model_not_in_warehouse(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": False, "size_gb": None, "last_altered": None, "last_read": None,
        "read_count": 0, "distinct_users": 0, "access_history_available": False,
        "table_type": None,
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [_deleted("models/stg_users.sql")]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    assert reports[0].exists is False
    assert reports[0].table_ref == "PROD_DB.STAGING.STG_USERS"


# ---------------------------------------------------------------------------
# Full happy path
# ---------------------------------------------------------------------------

def test_full_happy_path_populates_all_fields(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True,
        "size_gb": 1.234,
        "last_altered": "2026-03-24",
        "last_read": "2026-03-25",
        "read_count": 57,
        "distinct_users": 4,
        "access_history_available": True,
        "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [_deleted("models/stg_users.sql")]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = ["fct_orders"]

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    r = reports[0]
    assert r.exists is True
    assert r.size_gb == 1.234
    assert r.last_altered == "2026-03-24"
    assert r.last_read == "2026-03-25"
    assert r.read_count == 57
    assert r.distinct_users == 4
    assert r.access_history_available is True
    assert r.downstream_names == ["fct_orders"]


# ---------------------------------------------------------------------------
# Rename: new_path flows through to ModelReport
# ---------------------------------------------------------------------------

def test_renamed_model_new_path_in_report(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None,
        "read_count": 0, "distinct_users": 0, "access_history_available": True, "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [
            _renamed("models/old.sql", "models/new.sql")
        ]
        MockDiff.return_value.repo.active_branch.name = "feature/rename"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "OLD", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    assert reports[0].file_path == "models/old.sql"
    assert reports[0].new_path == "models/new.sql"


# ---------------------------------------------------------------------------
# Monorepo prefix stripping
# ---------------------------------------------------------------------------

def test_monorepo_prefix_stripped_before_manifest_lookup(mock_cfg):
    mock_cfg.REPO_SUBDIRECTORY = "dbt"
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 1.0, "last_altered": "2026-01-01", "last_read": None,
        "read_count": 0, "distinct_users": 0, "access_history_available": True, "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        # Git diff returns dbt-prefixed path
        MockDiff.return_value.get_deleted_models.return_value = [
            _deleted("dbt/models/stg.sql")
        ]
        MockDiff.return_value.repo.active_branch.name = "feature/mono"
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    # Manifest should have been called with the stripped path
    MockManifest.return_value.get_table.assert_called_once_with("models/stg.sql")


# ---------------------------------------------------------------------------
# adapter.close() called even when query raises; per-model error isolation
# ---------------------------------------------------------------------------

def test_adapter_close_called_on_exception(mock_cfg):
    """Even when a model's query fails, adapter.close() must be called."""
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.side_effect = RuntimeError("warehouse unavailable")
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [_deleted("models/stg_users.sql")]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    mock_adapter.close.assert_called_once()


def test_one_bad_model_does_not_abort_others(mock_cfg):
    """If one model's Snowflake query raises, the other models still appear in the report."""
    mock_adapter = MagicMock()
    # First call raises; second succeeds
    mock_adapter.get_table_stats.side_effect = [
        RuntimeError("warehouse unavailable"),
        {
            "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None,
            "read_count": 0, "distinct_users": 0, "access_history_available": True,
            "table_type": "BASE TABLE", "query_error": False,
        },
    ]
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [
            _deleted("models/bad.sql"),
            _deleted("models/good.sql"),
        ]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "TBL", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    assert len(reports) == 2
    file_paths = {r.file_path for r in reports}
    assert "models/bad.sql" in file_paths
    assert "models/good.sql" in file_paths
    # The bad model should be marked as a query error
    bad = next(r for r in reports if r.file_path == "models/bad.sql")
    assert bad.query_error is True


# ---------------------------------------------------------------------------
# Multiple deleted models
# ---------------------------------------------------------------------------

def test_multiple_deleted_models_all_reported(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None,
        "read_count": 5, "distinct_users": 1, "access_history_available": True, "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    paths = ["models/a.sql", "models/b.sql", "models/c.sql"]

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [_deleted(p) for p in paths]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "TBL", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    reports = mock_reporter.publish.call_args[0][0]
    assert len(reports) == 3
    assert {r.file_path for r in reports} == set(paths)


# ---------------------------------------------------------------------------
# YAML-only deletion: lookup_path used for manifest lookup
# ---------------------------------------------------------------------------

def test_yaml_deletion_uses_lookup_path_for_manifest(mock_cfg):
    """A YAML-only change must look up the manifest via lookup_path (.sql), not old_path (.yml)."""
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None,
        "read_count": 0, "distinct_users": 0, "access_history_available": True, "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    yaml_change = ModelChange(
        old_path="models/staging/stg_users.yml",
        new_path=None,
        lookup_path="models/staging/stg_users.sql",
    )

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [yaml_change]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "STG_USERS", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    # Manifest must be called with the .sql path, not the .yml path
    MockManifest.return_value.get_table.assert_called_once_with("models/staging/stg_users.sql")


def test_yaml_deletion_with_repo_subdirectory_prefix_stripped(mock_cfg):
    """YAML-only change in a monorepo: lookup_path must have the subdir prefix stripped."""
    mock_cfg.REPO_SUBDIRECTORY = "dbt"
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None,
        "read_count": 0, "distinct_users": 0, "access_history_available": True, "table_type": "BASE TABLE",
    }
    mock_reporter = MagicMock()

    yaml_change = ModelChange(
        old_path="dbt/models/stg_users.yml",
        new_path=None,
        lookup_path="dbt/models/stg_users.sql",
    )

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = [yaml_change]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "STG_USERS", "materialization": "table"
        }
        MockManifest.return_value.get_downstream_names.return_value = []

        main.run()

    # Prefix must be stripped: manifest sees "models/stg_users.sql", not "dbt/models/stg_users.sql"
    MockManifest.return_value.get_table.assert_called_once_with("models/stg_users.sql")
