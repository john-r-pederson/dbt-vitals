# dbt-vitals

**dbt-vitals** is a GitHub Action that protects your data warehouse from silent table drops.

When a pull request deletes or renames a dbt model, dbt-vitals maps the file to its production warehouse table via `manifest.json`, queries live metadata (size, last altered, read count, distinct users), and posts a **Vital Signs** report as a PR comment — before the table is gone.

```
## 🔍 dbt-vitals: Warehouse Impact Report

> **2 model(s) deleted or renamed in this PR.** Review before merging.

| Model | Warehouse Table | Type | Size | Last Altered | Reads (90d) | dbt Dependents |
| :--- | :--- | :--- | ---: | :--- | ---: | :--- |
| 🔴 `models/stg_users.sql` | `PROD.STAGING.STG_USERS` | table | 42.1 GB | 2026-03-24 | 318 (12 users) | `fct_orders`, `rpt_users` |
| `models/stg_sessions.sql` | `PROD.STAGING.STG_SESSIONS` | view | — | 2026-03-20 | 0 | — |

> ⚠️ Tables with recent reads or dbt dependents may have active consumers outside this PR.
```

**Risk indicators:** 🔴 = actively read AND has dbt dependents · 🟡 = one of the two · (none) = likely safe

---

## How it works

1. On every PR that touches `models/**/*.sql`, `snapshots/**/*.sql`, or `seeds/**/*.csv`, dbt-vitals runs inside a Docker container on GitHub-hosted runners.
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
        uses: actions/checkout@v4
        with:
          fetch-depth: 0   # Full history required for the git diff

      # Download your production manifest.json before this step.
      # See "Manifest setup" below.

      - name: Run dbt-vitals
        uses: Laskr/dbt-vitals@v0.1.0
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
GRANT USAGE ON WAREHOUSE <wh>               TO ROLE dbt-vitals_ROLE;
GRANT USAGE ON DATABASE <db>                TO ROLE dbt-vitals_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <db> TO ROLE dbt-vitals_ROLE;
GRANT REFERENCES ON ALL TABLES IN DATABASE <db> TO ROLE dbt-vitals_ROLE;
GRANT REFERENCES ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE dbt-vitals_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE dbt-vitals_ROLE; -- for read-count data
```

---

## Manifest setup

dbt-vitals needs your **production** `manifest.json` to map deleted model files to their warehouse tables. The manifest format is identical whether you use dbt Core or dbt Cloud — the difference is just how you get it into the workflow.

Add a step **before** "Run dbt-vitals" to make it available. Pick the option that matches your setup:

**Option A — dbt Core: run `dbt compile` in CI**
```yaml
- name: Compile dbt project
  run: dbt compile --profiles-dir . --target prod
# manifest is now at ./target/manifest.json — no manifest-path input needed
```

**Option B — dbt Cloud: download from the artifacts API**
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

**Option C — S3 / GCS artifact store**
```yaml
- name: Download manifest from S3
  run: aws s3 cp s3://your-bucket/dbt-artifacts/manifest.json ./target/manifest.json
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_REGION: us-east-1
```

**Option D — GitHub Actions artifact from a prior job**
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

```
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

| Input | Required | Default | Description |
| :--- | :---: | :--- | :--- |
| `warehouse-type` | | `snowflake` | Warehouse type. Currently supported: `snowflake` |
| `snowflake-account` | | | Account in `org-account` format, e.g. `myorg-abc12345`. Find it at app.snowflake.com → Admin → Accounts. |
| `snowflake-user` | | | Snowflake username |
| `snowflake-private-key` | | | Base64-encoded PKCS8 PEM private key |
| `snowflake-private-key-passphrase` | | | Passphrase for the private key (leave blank if unencrypted) |
| `snowflake-warehouse` | | | Virtual warehouse name |
| `snowflake-database` | | | Default database |
| `snowflake-schema` | | | Default schema |
| `snowflake-role` | | `dbt-vitals_ROLE` | Role for warehouse queries |
| `manifest-path` | | | Explicit path to `manifest.json`. Auto-discovered at `target/manifest.json` if not set. |
| `base-branch` | | `main` | Branch to diff against. Defaults to `GITHUB_BASE_REF` (the PR target). |
| `target-dir` | | `models/` | Directory to watch for deleted/renamed dbt models. |
| `lookback-days` | | `90` | Days to look back in `ACCESS_HISTORY` for read counts. |
| `repo-subdirectory` | | | Subdirectory where dbt lives in a monorepo (e.g. `dbt`). Strips this prefix from git diff paths before manifest lookup. |
| `pr-title` | | | PR title. Used to detect `[skip dbt-vitals]` label. Pass `github.event.pull_request.title`. |
| `github-token` | ✓ | | Use `secrets.GITHUB_TOKEN` |
| `pr-number` | | | Pass `github.event.pull_request.number` |

---

## Snowflake account format

Always use `org-account` format. Find it in your Snowflake URL:

```
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

# Run against a branch that deletes a model
git checkout test/delete-stg-users
uv run python src/main.py

# Run tests
uv run pytest tests/ -v
```

Requires a `.env` file with Snowflake credentials. See `CLAUDE.md` for the full variable list.

---

## Supported warehouses

| Warehouse | Status |
| :--- | :--- |
| Snowflake | ✅ Supported |
| BigQuery | Planned |
| Redshift | Planned |
| Databricks | Planned |

Contributions welcome. See `src/adapters/base.py` for the adapter interface.
