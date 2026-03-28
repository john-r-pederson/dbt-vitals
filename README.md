# dbt-vitals

[![CI](https://github.com/john-r-pederson/dbt-vitals/actions/workflows/ci.yml/badge.svg)](https://github.com/john-r-pederson/dbt-vitals/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**dbt-vitals** is a GitHub Action that tells you the warehouse impact of a dbt change before you merge it.

When a PR deletes, renames, or removes schema config for a dbt model, snapshot, or seed, dbt-vitals cross-references your `manifest.json` against the live warehouse and posts a **Warehouse Impact Report** as a PR comment: table size, last altered timestamp, read count, distinct users, and full transitive downstream lineage. See exactly what you're about to lose — and whether anything still depends on it — before the table is gone.

**What triggers a report row:**

| Change | File types | Default | Notes |
| :--- | :--- | :---: | :--- |
| Model deleted or renamed | `.sql` in `models/` | ✅ | All subdirectories |
| Seed deleted or renamed | `.csv` in `seeds/` | ✅ | All subdirectories |
| Snapshot deleted or renamed | `.sql` in `snapshots/` | ❌ | Set `target-dir: models/,snapshots/` |
| Schema config removed | `.yml`/`.yaml` in `models/` | ✅ | Only when the paired `.sql` still exists — table is live but schema config is gone |

```markdown
## 🔍 dbt-vitals: Warehouse Impact Report

> **3 model(s) deleted or renamed in this PR.** Review before merging.

| Model | Warehouse Table | Type | Size | Last Altered | Reads (90d) | dbt Dependents |
| :--- | :--- | :--- | ---: | :--- | ---: | :--- |
| 🔴 `models/stg_users.sql` | `PROD.STAGING.STG_USERS` | table | 42.1 GB | 2026-03-24 | 318 (12 users) | `fct_orders`, `rpt_revenue`, `rpt_users` |
| `models/stg_sessions.sql` _(→ `models/archive/stg_sessions.sql`)_ | `PROD.STAGING.STG_SESSIONS` | view | — | 2026-03-20 | 0 | — |
| `seeds/ref_countries.csv` | `PROD.SEEDS.REF_COUNTRIES` | table | 0 bytes | 2026-01-15 | 4 (1 user) | — |

> ⚠️ Tables with recent reads or dbt dependents may have active consumers outside this PR.
```

**Risk indicators:** 🔴 = actively read AND has dbt dependents · 🟡 = one of the two · (none) = likely safe

---

## Requirements

- **GitHub Actions** — dbt-vitals runs as a Docker-based Action on GitHub-hosted or self-hosted runners
- **Snowflake** — the only supported warehouse today (BigQuery, Redshift, Databricks: [planned](#additional-adapters))
- **Service account** with key-pair RSA authentication (see [Authentication](#authentication))
- **A compiled `manifest.json`** from your production dbt project (see [Manifest setup](#manifest-setup))
- **dbt manifest schema v11** (dbt-core 1.6+) — earlier versions may work but are untested

---

## How it works

1. On every PR that touches `models/` (`.sql`, `.yml`, `.yaml`), `snapshots/`, or `seeds/`, dbt-vitals runs inside a Docker container on GitHub-hosted runners. The trigger paths in your workflow control which file types fire the Action — see [Quickstart](#quickstart).
2. It diffs HEAD against your base branch to find deleted or renamed files.
3. It looks up each file in your dbt `manifest.json` to get the fully-qualified warehouse table name.
4. It queries `INFORMATION_SCHEMA.TABLES` for size, type, and last-altered timestamp, and `ACCOUNT_USAGE.ACCESS_HISTORY` for read counts and distinct users.
5. It posts (or updates) a comment on the PR with a Markdown table of results.

---

## Quickstart

### 1. Add the workflow

Create `.github/workflows/dbt-vitals.yml` in your dbt repo:

```yaml
name: dbt-vitals

on:
  pull_request:
    types: [opened, synchronize, reopened]
    paths:
      - 'models/**/*.sql'
      - 'models/**/*.yml'    # Required for YAML-only schema file deletion detection
      - 'models/**/*.yaml'   # Required for YAML-only schema file deletion detection
      - 'snapshots/**/*.sql'
      - 'seeds/**/*.csv'

jobs:
  dbt-vitals:
    name: Warehouse Impact Report
    runs-on: ubuntu-latest
    permissions:
      pull-requests: write
      contents: read

    steps:
      - name: Checkout
        uses: actions/checkout@v4  # Pin to a SHA in security-sensitive environments
        with:
          fetch-depth: 0   # Full history required for the git diff

      # Download your production manifest.json before this step.
      # See "Manifest setup" below.

      - name: Run dbt-vitals
        uses: john-r-pederson/dbt-vitals@v0
        with:
          warehouse-type: snowflake
          snowflake-account: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          snowflake-user: ${{ secrets.SNOWFLAKE_USER }}
          snowflake-private-key: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
          snowflake-warehouse: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
          snowflake-database: ${{ secrets.SNOWFLAKE_DATABASE }}
          snowflake-schema: ${{ secrets.SNOWFLAKE_SCHEMA }}
          snowflake-role: ${{ secrets.SNOWFLAKE_ROLE }}
          github-token: ${{ secrets.GITHUB_TOKEN }}
          pr-number: ${{ github.event.pull_request.number }}
          pr-title: ${{ github.event.pull_request.title }}
```

> `fetch-depth: 0` is **required**. Shallow clones break the git diff.

> If you use dbt snapshots, add `target-dir: models/,snapshots/` to the step inputs and `snapshots/**/*.sql` to the workflow `paths` filter. Snapshots are not watched by default.

### 2. Add GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
| :--- | :--- |
| `SNOWFLAKE_ACCOUNT` | `org-account` format, e.g. `myorg-abc12345` |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PRIVATE_KEY` | Base64-encoded RSA private key (see Auth below) |
| `SNOWFLAKE_WAREHOUSE` | Virtual warehouse name |
| `SNOWFLAKE_DATABASE` | Default database |
| `SNOWFLAKE_SCHEMA` | Default schema |
| `SNOWFLAKE_ROLE` | Role with access to `INFORMATION_SCHEMA` and `ACCOUNT_USAGE` |

`GITHUB_TOKEN` is provided automatically — do not add it as a secret.

### 3. Grant the Snowflake role

```sql
GRANT USAGE ON WAREHOUSE <wh>               TO ROLE DBT_VITALS_ROLE;
GRANT USAGE ON DATABASE <db>                TO ROLE DBT_VITALS_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <db> TO ROLE DBT_VITALS_ROLE;
GRANT REFERENCES ON ALL TABLES IN DATABASE <db> TO ROLE DBT_VITALS_ROLE;
GRANT REFERENCES ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE DBT_VITALS_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DBT_VITALS_ROLE; -- for read-count data
```

---

## Manifest setup

dbt-vitals needs your **production** `manifest.json` to map deleted model files to their warehouse tables. The manifest format is identical whether you use dbt Core or dbt Cloud — the difference is just how you get it into the workflow.

Add a step **before** "Run dbt-vitals" to make it available. Pick the option that matches your setup:

#### Option A — dbt Core: run `dbt compile` in CI

```yaml
- name: Compile dbt project
  run: dbt compile --profiles-dir . --target prod
# manifest is now at ./target/manifest.json — no manifest-path input needed
```

> This requires a `profiles.yml` and production credentials available in the runner environment. Most teams prefer Option B or C to avoid exposing production credentials on PR workflows.

#### Option B — dbt Cloud: download from the artifacts API

```yaml
- name: Download manifest from dbt Cloud
  run: |
    mkdir -p target
    curl -s -H "Authorization: Token ${{ secrets.DBT_CLOUD_API_TOKEN }}" \
      "https://cloud.getdbt.com/api/v2/accounts/${{ secrets.DBT_ACCOUNT_ID }}/jobs/${{ secrets.DBT_CLOUD_JOB_ID }}/artifacts/manifest.json" \
      -o ./target/manifest.json
```

> Find your `DBT_ACCOUNT_ID` and `DBT_CLOUD_JOB_ID` in the dbt Cloud URL:
> `https://cloud.getdbt.com/deploy/{account_id}/projects/{project_id}/jobs/{job_id}`

#### Option C — S3 / GCS artifact store

```yaml
- name: Download manifest from S3
  run: aws s3 cp s3://your-bucket/dbt-artifacts/manifest.json ./target/manifest.json
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_REGION: us-east-1
```

#### Option D — GitHub Actions artifact from a prior job

```yaml
- uses: actions/download-artifact@v4
  with:
    name: dbt-manifest
# manifest is now at ./manifest.json — pass manifest-path: ./manifest.json
```

**Option E — Committed to repo** (not recommended for production)
No step needed. dbt-vitals auto-discovers `target/manifest.json` from the repo root.

---

## Escape hatch

Add `[skip dbt-vitals]` anywhere in your PR title to suppress the warehouse check:

```text
refactor: remove deprecated models [skip dbt-vitals]
```

dbt-vitals will exit cleanly without connecting to Snowflake or posting a comment. Case-insensitive.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| :--- | :--- | :--- |
| "Could not find 'target/manifest.json'" | Manifest not generated | Add a manifest download step (see above) or set `manifest-path` explicitly |
| "Manifest loaded but contains no dbt models" | Wrong file or stale manifest | Check `manifest-path` points to a **compiled** manifest.json, not an empty or partial one |
| No PR comment posted; report printed to stdout | GitHub context missing | Confirm `github-token`, `pr-number` inputs are set in the workflow step |
| Snowflake 404 or hostname resolution error | Wrong account format | Use `org-account` format, e.g. `myorg-abc12345` (see Snowflake account format below) |
| "SNOWFLAKE_ACCOUNT looks like a legacy account locator" | Legacy locator used | Use the org-account format from app.snowflake.com → Admin → Accounts |
| "Multi-factor authentication is required" | Password auth with MFA enforced | Set `snowflake-private-key` for key-pair auth instead |
| "Could not base64-decode SNOWFLAKE_PRIVATE_KEY" | Key encoded incorrectly | Re-encode with `base64 -i snowflake_key.p8 \| tr -d '\n'` |
| `_(query error — check role grants)_` in report | Role lacks REFERENCES privilege | Run the GRANT statements above |
| `_(no ACCESS_HISTORY grant)_` in Reads column | Role lacks IMPORTED PRIVILEGES | `GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ...` |
| `fetch-depth: 0` missing from checkout step | Shallow clone, diff fails | Add `fetch-depth: 0` to the `actions/checkout@v4` step |
| Manifest staleness warning in logs | manifest.json is >24h old | Re-run `dbt compile` or refresh your manifest download step |
| Action doesn't run on YAML-only schema file deletions | Workflow `paths` filter doesn't include `.yml` | Add `models/**/*.yml` and `models/**/*.yaml` to the `paths` filter |

---

## Authentication

dbt-vitals uses key-pair RSA authentication for headless CI — no MFA prompt, no browser.

**Generate a key pair:**

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
base64 -i snowflake_key.p8 | tr -d '\n'   # paste this into SNOWFLAKE_PRIVATE_KEY secret
```

**Assign the public key in Snowflake:**

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of snowflake_key.pub, header/footer excluded>';
```

Delete both key files from your filesystem immediately after — never commit them.

| Context | Method | How |
| :--- | :--- | :--- |
| GitHub Actions (CI) | Key-pair RSA | `SNOWFLAKE_PRIVATE_KEY` secret |
| Local dev (SSO/MFA) | `externalbrowser` | Leave `SNOWFLAKE_PRIVATE_KEY` and `SNOWFLAKE_PASSWORD` unset |
| Service account | Password | Set `SNOWFLAKE_PASSWORD` — fails if MFA is enforced |

---

## All inputs

Inputs marked **✓** are always required. Inputs marked **S** are required when `warehouse-type: snowflake` (the only supported warehouse today).

| Input | Required | Default | Description |
| :--- | :---: | :--- | :--- |
| `warehouse-type` | | `snowflake` | Warehouse type. Currently supported: `snowflake` |
| `snowflake-account` | S | | Account in `org-account` format, e.g. `myorg-abc12345`. Find it at app.snowflake.com → Admin → Accounts. |
| `snowflake-user` | S | | Snowflake username |
| `snowflake-private-key` | S | | Base64-encoded PKCS8 PEM private key |
| `snowflake-private-key-passphrase` | | | Passphrase for the private key (leave blank if unencrypted) |
| `snowflake-warehouse` | S | | Virtual warehouse name |
| `snowflake-database` | S | | Default database |
| `snowflake-schema` | S | | Default schema |
| `snowflake-role` | | `DBT_VITALS_ROLE` | Role for warehouse queries |
| `manifest-path` | | | Explicit path to `manifest.json`. Auto-discovered at `target/manifest.json` if not set. |
| `base-branch` | | `main` | Branch to diff against. Defaults to `GITHUB_BASE_REF` (the PR target). |
| `target-dir` | | `models/` | Directory (or comma-separated list) to watch for deleted/renamed dbt models. E.g. `models/,snapshots/` watches both. |
| `seeds-dir` | | `seeds/` | Directory to watch for deleted/renamed seed CSVs. |
| `lookback-days` | | `90` | Days to look back in `ACCESS_HISTORY` for read counts. |
| `query-timeout-seconds` | | `60` | Per-query Snowflake timeout in seconds. Increase for orgs with large `ACCESS_HISTORY`. |
| `repo-subdirectory` | | | Subdirectory where dbt lives in a monorepo (e.g. `dbt`). Strips this prefix from git diff paths before manifest lookup. |
| `pr-title` | | | PR title. Used to detect `[skip dbt-vitals]` label. Pass `github.event.pull_request.title`. |
| `github-token` | ✓ | | Use `secrets.GITHUB_TOKEN` |
| `pr-number` | | | Pass `github.event.pull_request.number`. If omitted, no PR comment is posted — the report is printed to the Action log only. |

---

## Snowflake account format

Always use `org-account` format. Find it in your Snowflake URL:

```text
https://app.snowflake.com/myorg/abc12345/
                          ^^^^^ ^^^^^^^^
                          org   account
→ SNOWFLAKE_ACCOUNT = myorg-abc12345
```

Using the account locator alone (e.g. `abc12345`) causes 404 connection errors.

---

## Local development

```bash
# Install deps
uv sync

# Run against the comprehensive E2E test branch
# (7 scenarios: deletion, not-in-warehouse, not-in-manifest, rename, seed, downstream deps, risk indicator)
git checkout test/e2e-scenarios
uv run python src/main.py

# Run tests
uv run pytest tests/ -v
```

Requires a `.env` file with Snowflake credentials. Copy `.env.example` and fill in your values.

---

## Roadmap

### Transitive dependencies

The **dbt Dependents** column shows the **full transitive lineage** — not just direct dependents. If `stg_users → fct_orders → rpt_revenue`, deleting `stg_users` will show both `fct_orders` and `rpt_revenue`. The traversal is a breadth-first walk of the dbt DAG with cycle protection.

dbt tests, metrics, and exposures are not included in the lineage column.

### Non-dbt consumers

dbt-vitals cannot see consumers outside the dbt graph. Tableau workbooks, Looker explores, Jupyter notebooks, Airflow DAGs, reverse ETL pipelines (Census, Hightouch), and ad-hoc analyst SQL are all invisible to it.

The `ACCESS_HISTORY` read count is the closest proxy. If a table shows 318 reads from 12 distinct users over 90 days, something is consuming it — even if you can't identify what. That's the signal: don't drop this without investigating.

### Additional adapters

| Warehouse | Status |
| :--- | :--- |
| Snowflake | ✅ Supported |
| BigQuery | Planned |
| Redshift | Planned |
| Databricks | Planned |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the adapter interface and contribution guide. If you want to track or vote on a specific adapter, open or upvote an issue in [GitHub Issues](https://github.com/john-r-pederson/dbt-vitals/issues).

### Manifest schema version compatibility

The manifest version check currently logs a warning when an unexpected `dbt_schema_version` is encountered but does not block execution. A future improvement would document the tested version matrix and surface a clearer error message (or hard stop) if a structurally incompatible version is detected.

---

## Contributing

Contributions are welcome. The highest-impact way to contribute is adding a new warehouse adapter — see [CONTRIBUTING.md](CONTRIBUTING.md) for the adapter interface and step-by-step guide.

For bugs and feature requests, open an issue. For questions, use GitHub Discussions.

---

## Scope & limitations

### What dbt-vitals detects

dbt-vitals tracks **deletions and renames** in your configured directories. Adding or modifying a file never triggers a report row.

| File type | Detected by default | Notes |
| :--- | :---: | :--- |
| `.sql` files in `models/` | ✅ | Includes all subdirectories |
| `.csv` files in `seeds/` | ✅ | Includes all subdirectories |
| `.sql` files in `snapshots/` | ❌ | Set `target-dir: models/,snapshots/` to watch both simultaneously |
| `.yml`/`.yaml` schema files | ⚠️ | Reported when the paired `.sql` still exists — schema config was removed while the table is live. Co-deleted or co-renamed pairs report the `.sql` change only. Add `models/**/*.yml` to the workflow `paths` filter to catch YAML-only deletions. |

### Downstream dependents

The **dbt Dependents** column shows the **full transitive lineage**. If `stg_users → fct_orders → rpt_revenue`, deleting `stg_users` shows both `fct_orders` and `rpt_revenue`. The traversal is a breadth-first walk of the dbt DAG. dbt tests, metrics, and exposures are not included.

### Warehouse visibility (Snowflake)

| Scenario | Behavior |
| :--- | :--- |
| Table exists, role has `REFERENCES` | Size, type, and last-altered timestamp reported |
| Table exists, role lacks `REFERENCES` | _(query error — check role grants)_ |
| Table exists, role lacks `IMPORTED PRIVILEGES` | Size and last-altered reported; Reads column shows _(no ACCESS_HISTORY grant)_ |
| Table not found | _(not in warehouse)_ — already dropped, or in a different database than the manifest specifies |
| External tables | May not appear in `INFORMATION_SCHEMA.TABLES` on some Snowflake account configurations |

### What "Last Altered" means

The **Last Altered** column reflects the last DDL change — when the table was created, a column was added, a clustering key changed, etc. It does **not** reflect when data was last loaded. A table loaded daily with no schema changes will show its original creation date. Use the **Reads (90d)** column as the better signal for active consumption.

### What "Reads (90d)" counts

`ACCESS_HISTORY` records every query that touched the table — including dbt pipeline runs, dbt tests, BI tool scheduled refreshes, and ad-hoc SQL. A table showing 318 reads from 12 users could be entirely automated dbt runs with no human consumers. High read counts mean *something* is using the table; they don't tell you what.

### Read count freshness

Snowflake's `ACCOUNT_USAGE.ACCESS_HISTORY` has **approximately 3 hours of latency**. Read counts reflect queries run up to ~3 hours before the report was generated. A table showing `0` recent reads may still have been read within the last few hours.

### Manifest freshness

dbt-vitals logs a warning if `manifest.json` is more than 24 hours old. A stale manifest may map deleted files to incorrect warehouse tables if schemas, aliases, or databases have changed since the last `dbt compile`.

### Report size

If a PR deletes a very large number of models simultaneously, the report is truncated to stay within GitHub's 65,536-character comment limit. The Action logs always contain the full list.
