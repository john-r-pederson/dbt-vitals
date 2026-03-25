import json
import pytest
from pathlib import Path
from manifest_engine import ManifestEngine

FIXTURE = Path(__file__).parent / "fixtures" / "manifest.json"


@pytest.fixture
def engine():
    return ManifestEngine(provided_path=str(FIXTURE))


def test_known_path_returns_table_metadata(engine):
    result = engine.get_table("models/staging/stg_users.sql")
    assert result == {
        "database": "PROD_DB",
        "schema": "STAGING",
        "name": "STG_USERS",
    }


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
    # Only resource_type == "model" entries should be indexed.
    result = engine.get_table("models/staging/stg_users.sql")
    assert result is not None
    assert result["name"] == "STG_USERS"


def test_node_with_null_file_path_is_skipped(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.null_path": {
                "resource_type": "model",
                "original_file_path": None,
                "database": "DB", "schema": "SCH", "name": "m", "alias": None,
            },
            "model.p.valid": {
                "resource_type": "model",
                "original_file_path": "models/valid.sql",
                "database": "DB", "schema": "SCH", "name": "valid", "alias": None,
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table(None) is None
    assert eng.get_table("models/valid.sql") is not None


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
            }
        }
    }))
    # Run from a subdirectory — autodiscovery should walk up to tmp_path
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
            }
        }
    }))
    eng = ManifestEngine(provided_path=str(custom))
    assert eng.get_table("models/m.sql") is not None
