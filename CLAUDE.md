# Isotrope — CLAUDE.md

Warehouse-agnostic dbt linter. Runs as a GitHub Action. When a PR deletes or renames a dbt model, Isotrope maps it to the production warehouse table via `manifest.json` and posts a "Vital Signs" report (size, last altered, last read) as a PR comment — before the table is gone.

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
  diff_engine.py            # gitpython: HEAD vs base branch → deleted .sql paths
  manifest_engine.py        # Parses target/manifest.json → warehouse object names
  reporter.py               # Builds Markdown table; posts/updates GitHub PR comment
  adapters/
    base.py                 # BaseWarehouseAdapter ABC
    factory.py              # Routes WAREHOUSE_TYPE to the correct adapter
    snowflake_adapter.py    # Snowflake implementation (key-pair + browser auth)
tests/
  fixtures/manifest.json    # Minimal manifest for unit tests
  test_diff_engine.py
  test_manifest_engine.py
  test_reporter.py
models/
  stg_users.sql             # Local test fixture — simulates a dbt model file
test-dbt-repo/
  target/manifest.json      # Test manifest mapping models/stg_users.sql → Snowflake
```

---

## Architecture Rules

**Adding a new warehouse adapter:**
1. Create `src/adapters/<warehouse>_adapter.py` implementing `BaseWarehouseAdapter`
2. `get_table_stats()` must return `{"exists": bool, "size_gb": float, "last_altered": str|None, "last_read": str|None}`
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
SNOWFLAKE_ACCOUNT=...        # org-account format: myorg-abc12345 (NOT wdb44754 alone)
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
# Test branch that deletes models/stg_users.sql exists at: test/delete-stg-users
git checkout test/delete-stg-users
uv run python src/main.py
```

Expects manifest at `./test-dbt-repo/target/manifest.json` mapping `models/stg_users.sql` → `ISOTROPE_DB.ISOTROPE_STAGING.STG_USERS`.

---

## Snowflake Role Setup

Minimum grants for `ISOTROPE_ROLE`:

```sql
GRANT USAGE ON WAREHOUSE <wh> TO ROLE ISOTROPE_ROLE;
GRANT USAGE ON DATABASE <db> TO ROLE ISOTROPE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <db> TO ROLE ISOTROPE_ROLE;
GRANT REFERENCES ON ALL TABLES IN DATABASE <db> TO ROLE ISOTROPE_ROLE;
GRANT REFERENCES ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE ISOTROPE_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ISOTROPE_ROLE;  -- for ACCESS_HISTORY
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

Workflow file: `.github/workflows/isotrope.yml`
Action definition: `action.yml`
Container: `Dockerfile`

**`fetch-depth: 0` is mandatory** in the checkout step — shallow clones break the git diff.
