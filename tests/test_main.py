from unittest.mock import MagicMock, patch, call

import pytest

import main
from reporter import ModelReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cfg(monkeypatch):
    """Minimal Settings-like object sufficient for run_isotrope()."""
    cfg = MagicMock()
    cfg.BASE_BRANCH = "main"
    cfg.MANIFEST_PATH = None
    cfg.GITHUB_TOKEN = None
    cfg.GITHUB_REPOSITORY = None
    cfg.PR_NUMBER = None
    return cfg


def _patch_all(mock_cfg, deleted_paths, manifest_side_effect=None, stats=None):
    """
    Returns a context manager that patches all external dependencies of
    run_isotrope() and returns (mock_adapter, mock_reporter) for assertions.
    """
    mock_adapter = MagicMock()
    if stats is not None:
        mock_adapter.get_table_stats.return_value = stats

    mock_reporter = MagicMock()

    patches = [
        patch("main.get_config", return_value=mock_cfg),
        patch("main.DiffEngine"),
        patch("main.ManifestEngine"),
        patch("main.get_adapter", return_value=mock_adapter),
        patch("main.Reporter", return_value=mock_reporter),
    ]
    return patches, mock_adapter, mock_reporter


# ---------------------------------------------------------------------------
# No deleted models
# ---------------------------------------------------------------------------

def test_no_deleted_models_closes_adapter_and_skips_publish(mock_cfg):
    mock_adapter = MagicMock()
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine"), \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = []
        MockDiff.return_value.repo.active_branch.name = "feature/test"

        main.run_isotrope()

    mock_adapter.close.assert_called_once()
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

        MockDiff.return_value.get_deleted_models.return_value = ["models/stg_users.sql"]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = None

        main.run_isotrope()

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
        "exists": False, "size_gb": 0, "last_altered": None, "last_read": None
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = ["models/stg_users.sql"]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS"
        }

        main.run_isotrope()

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
    }
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = ["models/stg_users.sql"]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS"
        }

        main.run_isotrope()

    reports = mock_reporter.publish.call_args[0][0]
    r = reports[0]
    assert r.exists is True
    assert r.size_gb == 1.234
    assert r.last_altered == "2026-03-24"
    assert r.last_read == "2026-03-25"


# ---------------------------------------------------------------------------
# adapter.close() called even when query raises
# ---------------------------------------------------------------------------

def test_adapter_close_called_on_exception(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.side_effect = RuntimeError("warehouse unavailable")
    mock_reporter = MagicMock()

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = ["models/stg_users.sql"]
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "PROD_DB", "schema": "STAGING", "name": "STG_USERS"
        }

        with pytest.raises(RuntimeError):
            main.run_isotrope()

    mock_adapter.close.assert_called_once()


# ---------------------------------------------------------------------------
# Multiple deleted models
# ---------------------------------------------------------------------------

def test_multiple_deleted_models_all_reported(mock_cfg):
    mock_adapter = MagicMock()
    mock_adapter.get_table_stats.return_value = {
        "exists": True, "size_gb": 0.5, "last_altered": "2026-01-01", "last_read": None
    }
    mock_reporter = MagicMock()

    paths = ["models/a.sql", "models/b.sql", "models/c.sql"]

    with patch("main.get_config", return_value=mock_cfg), \
         patch("main.DiffEngine") as MockDiff, \
         patch("main.ManifestEngine") as MockManifest, \
         patch("main.get_adapter", return_value=mock_adapter), \
         patch("main.Reporter", return_value=mock_reporter):

        MockDiff.return_value.get_deleted_models.return_value = paths
        MockDiff.return_value.repo.active_branch.name = "feature/test"
        MockManifest.return_value.get_table.return_value = {
            "database": "DB", "schema": "SCH", "name": "TBL"
        }

        main.run_isotrope()

    reports = mock_reporter.publish.call_args[0][0]
    assert len(reports) == 3
    assert {r.file_path for r in reports} == set(paths)
