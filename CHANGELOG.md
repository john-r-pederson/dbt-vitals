# Changelog

All notable changes will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.2.0] — 2026-03-27

### Added

- **Full transitive dependency traversal** — the dbt Dependents column now shows the complete downstream lineage via breadth-first search of the dbt DAG, not just direct 1-hop dependents. Includes cycle protection and diamond-deduplication.

### Changed

- Dependents column footer note updated from "direct downstream models only" to "full transitive lineage"

---

## [0.1.2] — 2026-03-26

### Fixed

- **Snapshot downstream deps** — snapshots that depend on a deleted model were missing from the dependents column; fixed by including `snapshot` resource type in the reverse-dependency map alongside `model`
- **Report footer URL** — pointed to old internal repo; corrected to `github.com/john-r-pederson/dbt-vitals`

---

## [0.1.1] — 2026-03-26

### Fixed

- **`GITHUB_BASE_REF` env var isolation in tests** — test for missing base branch was leaking the env var and passing for the wrong reason

### Changed

- E2E workflow now runs against the published Docker image instead of a local `uv run`, making it a true integration test of what users actually get
- Dependency bumps: `actions/checkout` → v6.0.2, `docker/login-action` → v4.0.0, `docker/build-push-action` → v7.0.0, `astral-sh/setup-uv` → v7.6.0

---

## [0.1.0] — 2026-03-25

### Added

- **Warehouse Impact Report** — 7-column Markdown table posted as a GitHub PR comment when dbt models are deleted or renamed
- **Snowflake adapter** — queries `INFORMATION_SCHEMA.TABLES` (size, type, last altered) and `ACCOUNT_USAGE.ACCESS_HISTORY` (read count, distinct users, 90-day lookback)
- **Risk indicators** — 🔴 (actively read + has dbt dependents) and 🟡 (one of the two) for instant visual triage
- **dbt downstream lineage** — 1-hop reverse dependency map from `manifest.json`; dependents shown in the final column
- **Snapshot and seed support** — `snapshots/**/*.sql` and `seeds/**/*.csv` watched alongside `models/**/*.sql`; use `target-dir: models/,snapshots/` to track both simultaneously
- **YAML/schema file deletion detection** — standalone `.yml`/`.yaml` deletions reported when the paired `.sql` model still exists (schema config removed while the table is live); co-deletions and co-renames suppressed to avoid duplicate rows
- **Rename detection** — shows `old.sql → new.sql` for moved models; warehouse lookup uses the old path
- **Distinct user count** — reads cell shows `N (M users)` when `ACCESS_HISTORY` is available
- **Materialization type** — "table", "view", "incremental", "snapshot", "seed" from the manifest; falls back to `TABLE_TYPE` from the warehouse
- **`[skip dbt-vitals]`** — add anywhere in the PR title (case-insensitive) to suppress the warehouse check
- **Monorepo support** — `repo-subdirectory` input strips a path prefix before manifest lookup
- **Manifest staleness warning** — logged when `manifest.json` is older than 24 hours
- **Query error disambiguation** — "_(query error — check role grants)_" vs "_(not in warehouse)_" in the report
- **Access history grant check** — "_(no ACCESS_HISTORY grant)_" shown when `ACCOUNT_USAGE` is inaccessible
- **Key-pair RSA authentication** — PKCS8 PEM, base64-encoded; no MFA prompt in CI
- **Configurable lookback** — `lookback-days` input (default: 90)
- **Configurable query timeout** — `query-timeout-seconds` input (default: 60); increase for orgs with large `ACCESS_HISTORY`
- **Per-model error isolation** — a failure on one model logs an error and continues; the PR comment is always posted
- **Adapter init guard** — Snowflake connection is deferred until after the no-changes check; add-only PRs never open a warehouse connection
- **Dynamic header** — `Reads (Nd)` reflects the configured `lookback-days`
- **Report truncation** — oversized reports are truncated with a notice and the full list logged to stdout
