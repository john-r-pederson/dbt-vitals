# Isotrope

**Isotrope** is a GitHub Action that protects your data warehouse from silent table drops.

When a pull request deletes or renames a dbt model, Isotrope maps the file to its production warehouse table via `manifest.json`, queries live metadata (size, last altered, last read), and posts a **Vital Signs** report as a PR comment — before the table is gone.

```
## 🔍 Isotrope: Warehouse Impact Report

> **1 model(s) deleted or renamed in this PR.** Review before merging.

| Model | Warehouse Table | Size | Last Altered | Last Read |
| :--- | :--- | ---: | :--- | :--- |
| `models/stg_users.sql` | `ISOTROPE_DB.ISOTROPE_STAGING.STG_USERS` | 0.0007 GB | 2026-03-24 | _(unavailable)_ |

> ⚠️ Tables with recent reads may have active downstream consumers outside dbt.
```

---

## How it works

1. On every PR that touches `models/**/*.sql`, Isotrope runs inside a Docker container on GitHub-hosted runners.
2. It diffs HEAD against your base branch to find deleted or renamed `.sql` files.
3. It looks up each file in your dbt `manifest.json` to get the fully-qualified warehouse table name.
4. It queries `INFORMATION_SCHEMA.TABLES` for size and last-altered timestamp, and `ACCOUNT_USAGE.ACCESS_HISTORY` for the last read.
5. It posts (or updates) a comment on the PR with a Markdown table of results.

---

## Quickstart

### 1. Add the workflow

Create `.github/workflows/isotrope.yml` in your dbt repo:

```yaml
name: Isotrope

on:
  pull_request:
    types: [opened, synchronize, reopened]
    paths:
      - 'models/**/*.sql'

jobs:
  isotrope:
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

      - name: Run Isotrope
        uses: isotrope-ai/isotrope@v0.1.0
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
| `SNOWFLAKE_ROLE` | Role with access to INFORMATION\_SCHEMA and ACCOUNT\_USAGE |

`GITHUB_TOKEN` is provided automatically — do not add it as a secret.

### 3. Grant the Snowflake role

```sql
GRANT USAGE ON WAREHOUSE <wh>               TO ROLE ISOTROPE_ROLE;
GRANT USAGE ON DATABASE <db>                TO ROLE ISOTROPE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <db> TO ROLE ISOTROPE_ROLE;
GRANT REFERENCES ON ALL TABLES IN DATABASE <db> TO ROLE ISOTROPE_ROLE;
GRANT REFERENCES ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE ISOTROPE_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ISOTROPE_ROLE; -- for last-read data
```

---

## Manifest setup

Isotrope needs your production `manifest.json` to map model files to warehouse tables. Add a step before "Run Isotrope" to download it. Pick the option that matches your setup:

**Option A — S3 (most common for dbt Core)**
```yaml
- name: Download manifest
  run: aws s3 cp s3://your-bucket/dbt-artifacts/manifest.json ./target/manifest.json
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_REGION: us-east-1
```

**Option B — dbt Cloud artifacts API**
```yaml
- name: Download manifest
  run: |
    mkdir -p target
    curl -s -H "Authorization: Token ${{ secrets.DBT_CLOUD_API_TOKEN }}" \
      "https://cloud.getdbt.com/api/v2/accounts/${{ secrets.DBT_ACCOUNT_ID }}/runs/latest/artifacts/manifest.json" \
      -o ./target/manifest.json
```

**Option C — Committed to repo**
No step needed. Isotrope auto-discovers `target/manifest.json` from the repo root.

---

## Authentication

Isotrope uses key-pair RSA authentication for headless CI — no MFA prompt, no browser.

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
| `snowflake-account` | | | Account in `org-account` format, e.g. `myorg-abc12345` |
| `snowflake-user` | | | Snowflake username |
| `snowflake-private-key` | | | Base64-encoded PKCS8 PEM private key |
| `snowflake-private-key-passphrase` | | | Passphrase for the private key (leave blank if unencrypted) |
| `snowflake-warehouse` | | | Virtual warehouse name |
| `snowflake-database` | | | Default database |
| `snowflake-schema` | | | Default schema |
| `snowflake-role` | | `ISOTROPE_ROLE` | Role for warehouse queries |
| `manifest-path` | | | Explicit path to `manifest.json`. Auto-discovered at `target/manifest.json` if not set. |
| `base-branch` | | `main` | Branch to diff against. Defaults to `GITHUB_BASE_REF` (the PR target). |
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
| Snowflake | Supported |
| BigQuery | Planned |
| Redshift | Planned |
| Databricks | Planned |

Contributions welcome. See `src/adapters/base.py` for the adapter interface.
