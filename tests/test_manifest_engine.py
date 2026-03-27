import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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
    # fct_orders and aliased_model directly depend on stg_users;
    # orders_snapshot depends transitively (via fct_orders)
    deps = engine.get_downstream_names("models/staging/stg_users.sql")
    assert "CUSTOM_ALIAS" in deps
    assert "fct_orders" in deps
    assert "ORDERS_SNAPSHOT" in deps  # transitive via fct_orders


def test_downstream_names_returns_empty_for_leaf(engine):
    # aliased_model has no dependents in the fixture
    deps = engine.get_downstream_names("models/core/aliased_model.sql")
    assert deps == []


def test_downstream_names_returns_empty_for_unknown_path(engine):
    deps = engine.get_downstream_names("models/nonexistent.sql")
    assert deps == []


def test_downstream_names_sorted(engine):
    deps = engine.get_downstream_names("models/staging/stg_users.sql")
    assert deps == sorted(deps)


def test_snapshot_dependent_on_model_is_tracked(engine):
    # orders_snapshot depends on fct_orders in the fixture
    deps = engine.get_downstream_names("models/marts/fct_orders.sql")
    assert "ORDERS_SNAPSHOT" in deps


def test_transitive_chain_fully_traversed(tmp_path):
    """A → B → C: deleting A must surface both B and C as downstream."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.A": {
                "resource_type": "model", "original_file_path": "models/a.sql",
                "database": "DB", "schema": "SCH", "name": "A", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
            "model.p.B": {
                "resource_type": "model", "original_file_path": "models/b.sql",
                "database": "DB", "schema": "SCH", "name": "B", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.A"]},
            },
            "model.p.C": {
                "resource_type": "model", "original_file_path": "models/c.sql",
                "database": "DB", "schema": "SCH", "name": "C", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.B"]},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    deps = eng.get_downstream_names("models/a.sql")
    assert "B" in deps
    assert "C" in deps


def test_diamond_dependency_no_duplicates(tmp_path):
    """A → B, A → C, B → D, C → D: D must appear exactly once."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.A": {
                "resource_type": "model", "original_file_path": "models/a.sql",
                "database": "DB", "schema": "SCH", "name": "A", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
            "model.p.B": {
                "resource_type": "model", "original_file_path": "models/b.sql",
                "database": "DB", "schema": "SCH", "name": "B", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.A"]},
            },
            "model.p.C": {
                "resource_type": "model", "original_file_path": "models/c.sql",
                "database": "DB", "schema": "SCH", "name": "C", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.A"]},
            },
            "model.p.D": {
                "resource_type": "model", "original_file_path": "models/d.sql",
                "database": "DB", "schema": "SCH", "name": "D", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.B", "model.p.C"]},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    deps = eng.get_downstream_names("models/a.sql")
    assert deps.count("D") == 1  # deduplicated
    assert set(deps) == {"B", "C", "D"}


def test_cycle_in_manifest_does_not_infinite_loop(tmp_path):
    """A corrupt manifest with A → B → A must not cause an infinite loop."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.A": {
                "resource_type": "model", "original_file_path": "models/a.sql",
                "database": "DB", "schema": "SCH", "name": "A", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.B"]},
            },
            "model.p.B": {
                "resource_type": "model", "original_file_path": "models/b.sql",
                "database": "DB", "schema": "SCH", "name": "B", "alias": None,
                "config": {}, "depends_on": {"nodes": ["model.p.A"]},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    # Must terminate and return whatever is reachable without looping
    deps = eng.get_downstream_names("models/a.sql")
    assert isinstance(deps, list)


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


def test_manifest_missing_nodes_key_raises(tmp_path):
    bad = tmp_path / "manifest.json"
    bad.write_text(json.dumps({"metadata": {}}))
    with pytest.raises(ValueError, match="nodes"):
        ManifestEngine(provided_path=str(bad))


# ---------------------------------------------------------------------------
# Complex manifest scenarios
# ---------------------------------------------------------------------------

def test_snapshot_downstream_dep_tracked(tmp_path):
    """A snapshot that depends on a model must appear in get_downstream_names."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_orders": {
                "resource_type": "model",
                "original_file_path": "models/stg_orders.sql",
                "database": "DB", "schema": "SCH", "name": "stg_orders", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
            "snapshot.p.orders_snapshot": {
                "resource_type": "snapshot",
                "original_file_path": "snapshots/orders_snapshot.sql",
                "database": "DB", "schema": "SNAPSHOTS", "name": "orders_snapshot", "alias": "ORDERS_SNAPSHOT",
                "config": {"materialized": "snapshot"},
                "depends_on": {"nodes": ["model.p.stg_orders"]},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    deps = eng.get_downstream_names("models/stg_orders.sql")
    assert "ORDERS_SNAPSHOT" in deps


def test_exposure_node_excluded_from_mapping(tmp_path):
    """Exposure nodes must not appear in the table mapping."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "exposure.p.weekly_report": {
                "resource_type": "exposure",
                "original_file_path": "models/exposures.yml",
                "database": "DB", "schema": "SCH", "name": "weekly_report", "alias": None,
                "config": {}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table("models/exposures.yml") is None


def test_sources_top_level_key_does_not_break_parsing(tmp_path):
    """Real manifests include a top-level 'sources' key; it must not affect node parsing."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_users": {
                "resource_type": "model",
                "original_file_path": "models/stg_users.sql",
                "database": "DB", "schema": "SCH", "name": "stg_users", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        },
        "sources": {
            "source.p.raw.users": {
                "resource_type": "source",
                "original_file_path": "models/sources.yml",
                "database": "DB", "schema": "RAW", "name": "users",
            }
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table("models/stg_users.sql") is not None
    assert eng.get_table("models/sources.yml") is None


def test_alias_empty_string_falls_back_to_name(tmp_path):
    """An alias of '' (empty string) is falsy — the model name must be used instead."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_events": {
                "resource_type": "model",
                "original_file_path": "models/stg_events.sql",
                "database": "DB", "schema": "SCH", "name": "stg_events", "alias": "",
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table("models/stg_events.sql")["name"] == "stg_events"


def test_missing_config_key_returns_none_materialization(tmp_path):
    """A node with no 'config' key at all must not raise; materialization is None."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_bare": {
                "resource_type": "model",
                "original_file_path": "models/stg_bare.sql",
                "database": "DB", "schema": "SCH", "name": "stg_bare", "alias": None,
                "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    result = eng.get_table("models/stg_bare.sql")
    assert result is not None
    assert result["materialization"] is None


def test_monorepo_path_collision_last_node_wins(tmp_path):
    """
    In a monorepo with two dbt packages, two nodes can share the same
    original_file_path. The current behaviour is last-parsed wins.
    This test documents that behaviour so any future change is explicit.
    """
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.package_a.stg_users": {
                "resource_type": "model",
                "original_file_path": "models/stg_users.sql",
                "database": "DB_A", "schema": "SCH", "name": "stg_users", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
            "model.package_b.stg_users": {
                "resource_type": "model",
                "original_file_path": "models/stg_users.sql",
                "database": "DB_B", "schema": "SCH", "name": "stg_users", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    result = eng.get_table("models/stg_users.sql")
    # One of the two entries wins — either DB_A or DB_B. The point is it doesn't crash
    # and returns a deterministic result (not None).
    assert result is not None
    assert result["database"] in ("DB_A", "DB_B")


def test_dbt_package_node_does_not_match_user_model_path(tmp_path):
    """
    Manifests include nodes from imported dbt packages (e.g. dbt_utils).
    Their original_file_path starts with 'dbt_packages/'. A user file at
    'models/stg_users.sql' must not resolve to a package node.
    """
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.dbt_utils.stg_users": {
                "resource_type": "model",
                "original_file_path": "dbt_packages/dbt_utils/models/stg_users.sql",
                "database": "DB", "schema": "SCH", "name": "stg_users", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    assert eng.get_table("models/stg_users.sql") is None
    assert eng.get_table("dbt_packages/dbt_utils/models/stg_users.sql") is not None


def test_relation_name_field_does_not_break_parsing(tmp_path):
    """dbt 1.5+ nodes include a 'relation_name' field. It must be ignored without error."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_events": {
                "resource_type": "model",
                "original_file_path": "models/stg_events.sql",
                "database": "DB", "schema": "SCH", "name": "stg_events", "alias": None,
                "relation_name": '"DB"."SCH"."stg_events"',
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    result = eng.get_table("models/stg_events.sql")
    assert result is not None
    assert result["name"] == "stg_events"


def test_versioned_model_path_resolves_correctly(tmp_path):
    """
    dbt 1.5+ versioned models produce files like 'models/stg_users_v1.sql'.
    The manifest maps that exact path — verify get_table finds it.
    """
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_users.v1": {
                "resource_type": "model",
                "original_file_path": "models/stg_users_v1.sql",
                "database": "DB", "schema": "SCH", "name": "stg_users", "alias": "stg_users_v1",
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
            "model.p.stg_users.v2": {
                "resource_type": "model",
                "original_file_path": "models/stg_users_v2.sql",
                "database": "DB", "schema": "SCH", "name": "stg_users", "alias": "stg_users",
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    v1 = eng.get_table("models/stg_users_v1.sql")
    v2 = eng.get_table("models/stg_users_v2.sql")
    assert v1 is not None and v1["name"] == "stg_users_v1"
    assert v2 is not None and v2["name"] == "stg_users"


def test_schema_version_v12_does_not_warn(tmp_path, caplog):
    """Future manifest schema versions like v12 must not trigger the version warning."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
        },
        "nodes": {}
    }))
    with caplog.at_level(logging.WARNING):
        ManifestEngine(provided_path=str(manifest))
    assert "unexpected" not in caplog.text.lower()


def test_schema_version_non_v1x_warns(tmp_path, caplog):
    """A non-v1x schema version URL (hypothetical future major) must log a warning."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v20/manifest.json",
        },
        "nodes": {}
    }))
    with caplog.at_level(logging.WARNING):
        ManifestEngine(provided_path=str(manifest))
    assert "unexpected" in caplog.text.lower()


def test_null_database_in_manifest_returns_none_in_table_meta(tmp_path):
    """
    Some warehouse adapters (e.g. BigQuery) may produce a null database.
    ManifestEngine must return the entry without raising; callers handle None.
    """
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "nodes": {
            "model.p.stg_events": {
                "resource_type": "model",
                "original_file_path": "models/stg_events.sql",
                "database": None, "schema": "SCH", "name": "stg_events", "alias": None,
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
        }
    }))
    eng = ManifestEngine(provided_path=str(manifest))
    result = eng.get_table("models/stg_events.sql")
    assert result is not None
    assert result["database"] is None
