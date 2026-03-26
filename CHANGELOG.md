# Changelog

All notable changes will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.1.0] — 2026-03-25

### Added

- **Warehouse Impact Report** — 7-column Markdown table posted as a GitHub PR comment when dbt models are deleted or renamed
- **Snowflake adapter** — queries `INFORMATION_SCHEMA.TABLES` (size, type, last altered) and `ACCOUNT_USAGE.ACCESS_HISTORY` (read count, distinct users, 90-day lookback)
- **Risk indicators** — 🔴 (actively read + has dbt dependents) and 🟡 (one of the two) for instant visual triage
- **dbt downstream lineage** — 1-hop reverse dependency map from `manifest.json`; dependents shown in the final column
- **Snapshot and seed support** — `snapshots/**/*.sql` and `seeds/**/*.csv` are watched in addition to `models/**/*.sql`
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
- **Per-model error isolation** — a failure on one model logs an error and continues; the PR comment is always posted
- **Dynamic header** — `Reads (Nd)` reflects the configured `lookback-days`
