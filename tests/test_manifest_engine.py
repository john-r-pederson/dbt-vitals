import json
import logging
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from manifest_engine import ManifestEngine

FIXTURE = Path(__file__).parent / "fixtures" / "manifest.json"


@pytest.fixture
def engine():
    return ManifestEngine(provided_path=str(FIXTURE))


# ---------------------------------------------------------------------------
# Basic table lookup
# ---------------------------------------------------------------------------

def test_known_path_returns_table_metadata(engine):
    result = engine.get_table("models/staging/stg_users.sql")
    assert result["database"] == "PROD_DB"
    assert result["schema"] == "STAGING"
    assert result["name"] == "STG_USERS"


def test_uses_alias_over_name(engine):
    result = engine.get_table("models/core/aliased_model.sql")
    assert result["name"] == "CUSTOM_ALIAS"


def test_falls_back_to_name_when_alias_is_null(engine):
    result = engine.get_table("models/marts/fct_orders.sql")
    assert result["name"] == "fct_orders"


def test_unknown_path_returns_none(engine):
    assert engine.get_table("models/does_not_exist.sql") is None


def test_non_model_nodes_are_excluded(engine):
    # The fixture has a "test" resource_type at the same path as stg_users.
    # Only resource_type in ("model", "snapshot") entries should be indexed.
    result = engine.get_table("models/staging/stg_users.sql")
    assert result is not None
    assert result["name"] == "STG_USERS"


# ---------------------------------------------------------------------------
# Materialization
# ---------------------------------------------------------------------------

def test_materialization_extracted(engine):
    result = engine.get_table("models/staging/stg_users.sql")
    assert result["materialization"] == "table"


def test_incremental_materialization(engine):
    result = engine.get_table("models/marts/fct_orders.sql")
    assert result["materialization"] == "incremental"


def test_view_materialization(engine):
    result = engine.get_table("models/core/aliased_model.sql")
    assert result["materialization"] == "view"


# ---------------------------------------------------------------------------
# Snapshot support
# ---------------------------------------------------------------------------

def test_snapshot_resource_type_included(engine):
    result = engine.get_table("snapshots/orders_snapshot.sql")
    assert result is not None
    assert result["name"] == "ORDERS_SNAPSHOT"
    assert result["materialization"] == "snapshot"


def test_seed_resource_type_included(engine):
    result = engine.get_table("seeds/ref_countries.csv")
    assert result is not None
    assert result["name"] == "REF_COUNTRIES"
    assert result["materialization"] == "seed"


# ---------------------------------------------------------------------------
# Downstream lineage
# ---------------------------------------------------------------------------

def test_downstream_names_returns_dependents(engine):
    # fct_orders and aliased_model both depend on stg_users
    deps = engine.get_downstream_names("models/staging/stg_users.sql")
    assert "CUSTOM_ALIAS" in deps
    assert "fct_orders" in deps


def test_downstream_names_returns_empty_for_leaf(engine):
    # fct_orders has no dependents in the fixture
    deps = engine.get_downstream_names("models/marts/fct_orders.sql")
    assert deps == []


def test_downstream_names_returns_empty_for_unknown_path(engine):
    deps = engine.get_downstream_names("models/nonexistent.sql")
    assert deps == []


def test_downstream_names_sorted(engine):
    deps = engine.get_downstream_names("models/staging/stg_users.sql")
    assert deps == sorted(deps)


def test_downstream_names_no_duplicates(tmp_path):
    """Two nodes with the same name depending on the same upstream must not produce duplicates."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.upstream": {
                "resource_type": "model",
                "original_file_path": "models/upstream.sql",
                "database": "DB", "schema": "SCH", "name": "upstream", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
            "model.p.downstream_a": {
                "resource_type": "model",
                "original_file_path": "models/downstream_a.sql",
                "database": "DB", "schema": "SCH", "name": "same_name", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.upstream"]},
            },
            "model.p.downstream_b": {
                "resource_type": "model",
                "original_file_path": "models/downstream_b.sql",
                "database": "DB", "schema": "SCH", "name": "same_name", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.upstream"]},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    deps = eng.get_downstream_names("models/upstream.sql")
    assert deps == ["same_name"]  # deduplicated, not ["same_name", "same_name"]


# ---------------------------------------------------------------------------
# Null file path / edge cases
# ---------------------------------------------------------------------------

def test_node_with_null_file_path_is_skipped(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.null_path": {
                "resource_type": "model",
                "original_file_path": None,
                "database": "DB", "schema": "SCH", "name": "m", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
            "model.p.valid": {
                "resource_type": "model",
                "original_file_path": "models/valid.sql",
                "database": "DB", "schema": "SCH", "name": "valid", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table(None) is None
    assert eng.get_table("models/valid.sql") is not None


# ---------------------------------------------------------------------------
# Manifest staleness warning
# ---------------------------------------------------------------------------

def test_stale_manifest_logs_warning(tmp_path, caplog):
    old_date = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "metadata": {"generated_at": old_date},
        "nodes": {}
    }))
    with caplog.at_level(logging.WARNING):
        ManifestEngine(provided_path=str(manifest))
    assert "stale" in caplog.text.lower() or "old" in caplog.text.lower()


def test_fresh_manifest_no_staleness_warning(tmp_path, caplog):
    fresh_date = datetime.now(timezone.utc).isoformat()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "metadata": {"generated_at": fresh_date},
        "nodes": {}
    }))
    with caplog.at_level(logging.WARNING):
        ManifestEngine(provided_path=str(manifest))
    assert "stale" not in caplog.text.lower()


def test_missing_generated_at_no_warning(tmp_path, caplog):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {}}))
    with caplog.at_level(logging.WARNING):
        ManifestEngine(provided_path=str(manifest))
    assert "stale" not in caplog.text.lower()


# ---------------------------------------------------------------------------
# Autodiscovery
# ---------------------------------------------------------------------------

def test_autodiscovery_raises_when_no_manifest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="manifest.json"):
        ManifestEngine()


def test_autodiscovery_finds_manifest_in_parent(tmp_path, monkeypatch):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps({
        "nodes": {
            "model.p.m": {
                "resource_type": "model",
                "original_file_path": "models/m.sql",
                "database": "DB", "schema": "SCH", "name": "m", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            }
        }
    }))
    subdir = tmp_path / "some" / "subdir"
    subdir.mkdir(parents=True)
    monkeypatch.chdir(subdir)
    eng = ManifestEngine()
    assert eng.get_table("models/m.sql") is not None


def test_explicit_path_overrides_autodiscovery(tmp_path):
    custom = tmp_path / "custom_manifest.json"
    custom.write_text(json.dumps({
        "nodes": {
            "model.p.m": {
                "resource_type": "model",
                "original_file_path": "models/m.sql",
                "database": "DB", "schema": "SCH", "name": "m", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            }
        }
    }))
    eng = ManifestEngine(provided_path=str(custom))
    assert eng.get_table("models/m.sql") is not None
