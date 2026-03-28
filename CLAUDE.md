# dbt-vitals — CLAUDE.md

GitHub Action that surfaces the warehouse impact of dbt model changes before they merge. When a PR deletes, renames, or removes the schema config for a dbt model, snapshot, or seed, dbt-vitals maps the file to its production warehouse table via `manifest.json` and posts a Warehouse Impact Report (size, last altered, read counts, full transitive downstream lineage) as a PR comment. Snowflake is the only supported warehouse today; the adapter pattern is designed for BigQuery/Redshift/Databricks additions.

---

## Stack

- **Python 3.13+**, package manager: `uv`
- **Pydantic-settings** for config (reads `.env` + env vars)
- **gitpython** for diff detection
- **snowflake-connector-python** for warehouse queries
- **pytest** for tests
- Deployed as a **Docker-based GitHub Action**

---

## Running

```bash
# Install deps
uv sync

# Run locally (must be on a feature branch with deleted models)
uv run python src/main.py

# Run tests
uv run pytest tests/

# Run tests with output
uv run pytest tests/ -v
```

---

## Project Structure

```
src/
  main.py                   # Orchestrator — thin, delegates to engines/adapters
  config.py                 # Pydantic-settings; all env var config lives here
  diff_engine.py            # gitpython: HEAD vs base branch → deleted/renamed .sql, .yml, .csv paths
  manifest_engine.py        # Parses target/manifest.json → warehouse object names
  reporter.py               # Builds Markdown table; posts/updates GitHub PR comment
  adapters/
    base.py                 # BaseWarehouseAdapter ABC
    factory.py              # Routes WAREHOUSE_TYPE to the correct adapter
    snowflake_adapter.py    # Snowflake implementation (key-pair + browser auth)
tests/
  fixtures/manifest.json    # Minimal manifest for unit tests
  test_config.py
  test_diff_engine.py
  test_factory.py
  test_main.py
  test_manifest_engine.py
  test_reporter.py
  test_snowflake_adapter.py
models/
  stg_users.sql             # Local test fixture — simulates a dbt model file
test-dbt-repo/
  target/manifest.json      # Test manifest mapping models/stg_users.sql → Snowflake
```

---

## Architecture Rules

**Adding a new warehouse adapter:**
1. Create `src/adapters/<warehouse>_adapter.py` implementing `BaseWarehouseAdapter`
2. `get_table_stats()` must return:
   `{"exists": bool, "size_gb": float|None, "last_altered": str|None, "last_read": str|None, "read_count": int, "distinct_users": int, "access_history_available": bool, "table_type": str|None, "query_error": bool}`
3. Add a branch to `src/adapters/factory.py`
4. Add warehouse-specific fields to `src/config.py`

**Never:**
- Add logic to `main.py` beyond orchestration
- Use f-strings for SQL `WHERE` clause values — always use `%s` parameterized binds
- Interpolate object identifiers without running them through `_validate_identifier()` first
- Commit `.env`, private key files, or any secrets

---

## Config (`.env`)

```bash
WAREHOUSE_TYPE=snowflake

SNOWFLAKE_USER=...
SNOWFLAKE_ACCOUNT=...        # org-account format: myorg-abc12345 (NOT the account locator alone)
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
SNOWFLAKE_ROLE=...

# Auth priority: key-pair > password > externalbrowser
SNOWFLAKE_PRIVATE_KEY=...    # base64-encoded PKCS8 PEM — required for CI
# SNOWFLAKE_PASSWORD=...     # MFA-enforced accounts will reject this
# (leave both unset to use browser SSO for local dev)

MANIFEST_PATH=./test-dbt-repo/target/manifest.json   # override autodiscovery locally
```

**Snowflake account format:** always `{org}-{account}` (e.g. `myorg-abc12345`). The account locator alone causes 404s.

**`SNOWFLAKE_HOST` must not be set** unless on PrivateLink. Setting it to the web UI URL (`app.snowflake.com`) causes 404s.

---

## Authentication

| Context | Method | How |
| :--- | :--- | :--- |
| GitHub Actions (CI) | Key-pair RSA | `SNOWFLAKE_PRIVATE_KEY` = base64 PEM in GitHub Secret |
| Local dev (SSO/MFA) | `externalbrowser` | Leave `SNOWFLAKE_PRIVATE_KEY` and `SNOWFLAKE_PASSWORD` unset |
| Service account | Password | `SNOWFLAKE_PASSWORD` — fails if MFA enforced |

Generate key pair:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
base64 -i snowflake_key.p8 | tr -d '\n'   # paste into SNOWFLAKE_PRIVATE_KEY
```
Assign in Snowflake: `ALTER USER <user> SET RSA_PUBLIC_KEY='<pub key body>';`
Delete key files from filesystem immediately after — never commit them.

---

## Local End-to-End Test

```bash
# Comprehensive E2E test branch — covers 8 scenarios
git checkout test/e2e-scenarios
uv run python src/main.py
```

Uses `./test-dbt-repo/target/manifest.json`. Scenarios covered:

| Scenario | Model / file |
| :--- | :--- |
| Table exists in Snowflake | `models/stg_users.sql` → `DBT_VITALS_DB.DBT_VITALS_STAGING.STG_USERS` |
| Table not in warehouse | `models/stg_ghost.sql` → `STG_GHOST_MODEL` (never created) |
| Model not in manifest | `models/stg_orphan.sql` (no manifest entry) |
| Rename | `models/stg_customers.sql` → `models/staging/stg_customers.sql` |
| Seed (.csv) | `seeds/ref_countries.csv` |
| Downstream dep tracking | `fct_orders` depends on `stg_users` in manifest |
| Risk indicator | `stg_users` shows 🟡/🔴 due to downstream dep |
| Transitive dep tracking | `orders_snapshot` (snapshot) depends on `fct_orders` → appears under `stg_users` |

**Known E2E gap:** access history unavailable (`_(no ACCESS_HISTORY grant)_`) requires a separate Snowflake role without `IMPORTED PRIVILEGES`. Covered by unit tests in `tests/test_snowflake_adapter.py`.

---

## Snowflake Role Setup

Minimum grants for `DBT_VITALS_ROLE`:

```sql
GRANT USAGE ON WAREHOUSE <wh> TO ROLE DBT_VITALS_ROLE;
GRANT USAGE ON DATABASE <db> TO ROLE DBT_VITALS_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <db> TO ROLE DBT_VITALS_ROLE;
GRANT REFERENCES ON ALL TABLES IN DATABASE <db> TO ROLE DBT_VITALS_ROLE;
GRANT REFERENCES ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE DBT_VITALS_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DBT_VITALS_ROLE;  -- for ACCESS_HISTORY
```

---

## Git Conventions

Branch naming: `feat/`, `fix/`, `test/`, `chore/`

Always branch off `main`, merge back with `--no-ff`:
```bash
git checkout -b feat/my-feature
# ... work ...
git checkout main
git merge --no-ff feat/my-feature -m "Merge feat/my-feature"
git branch -d feat/my-feature
```

Commit messages: `type: description` — types are `feat`, `fix`, `test`, `chore`, `docs`.

---

## GitHub Action

Triggered on `pull_request` when `models/**/*.sql` changes. Requires these GitHub Secrets:
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`

`GITHUB_TOKEN` is provided automatically — do not add as a secret.

Workflow file: `.github/workflows/dbt-vitals.yml`
Action definition: `action.yml`
Container: `Dockerfile`

**`fetch-depth: 0` is mandatory** in the checkout step — shallow clones break the git diff.

**GitHub Actions Docker compatibility — two known gotchas:**
1. `git config --system safe.directory /github/workspace` is set in the Dockerfile. Must use `--system` (not `--global`) because Actions overrides `$HOME` inside the container, so `/root/.gitconfig` is never read.
2. `checkout@v4` does not create a local branch for the PR base ref — only `origin/<base>` is available. `DiffEngine` handles this by catching `GitCommandError` and retrying with the `origin/` prefix.
