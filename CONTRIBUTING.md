# Contributing to dbt-vitals

Thanks for your interest in contributing. This document covers dev setup, the adapter architecture, and how to submit a PR.

---

## Dev setup

```bash
git clone https://github.com/Laskr/dbt-vitals.git
cd dbt-vitals
uv sync
uv run pytest tests/ -v
```

Copy `.env.example` (or see `CLAUDE.md`) to `.env` and fill in Snowflake credentials if you want to run the full local end-to-end test.

---

## Architecture

```
src/
  main.py               Thin orchestrator — do not add logic here
  config.py             All config lives here (pydantic-settings)
  diff_engine.py        Git diff → deleted model paths
  manifest_engine.py    manifest.json → warehouse table names
  reporter.py           Markdown report + GitHub PR comment
  adapters/
    base.py             BaseWarehouseAdapter ABC
    factory.py          Routes WAREHOUSE_TYPE → adapter
    snowflake_adapter.py
```

**Rules:**
- `main.py` orchestrates only — no business logic
- SQL `WHERE` values always use `%s` parameterized binds, never f-strings
- Warehouse identifiers must pass through `_validate_identifier()` before interpolation
- All output via `logging`, not `print()`

---

## Adding a new warehouse adapter

1. Create `src/adapters/<warehouse>_adapter.py` implementing `BaseWarehouseAdapter`:

```python
from adapters.base import BaseWarehouseAdapter

class MyWarehouseAdapter(BaseWarehouseAdapter):
    def __init__(self, cfg):
        # connect using cfg fields
        ...

    def get_table_stats(self, db: str, schema: str, table: str) -> dict:
        # Must return:
        # {
        #     "exists": bool,
        #     "size_gb": float | None,           # None for views (no storage)
        #     "last_altered": str | None,        # YYYY-MM-DD
        #     "last_read": str | None,           # YYYY-MM-DD, None if no reads in lookback
        #     "read_count": int,
        #     "distinct_users": int,
        #     "access_history_available": bool,  # False if role lacks IMPORTED PRIVILEGES
        #     "table_type": str | None,          # "BASE TABLE", "VIEW", etc.
        #     "query_error": bool,               # True = permissions error, not absence
        # }
        ...

    def close(self) -> None:
        ...
```

2. Add a branch to `src/adapters/factory.py`
3. Add warehouse-specific fields to `src/config.py` (follow the Snowflake pattern)
4. Add tests in `tests/test_<warehouse>_adapter.py`
5. Update the README inputs table and supported warehouses list

---

## Testing

All PRs must include tests. Run the suite with:

```bash
uv run pytest tests/ -v
```

Tests use real git repos (`tmp_path` fixtures) and mocks — no live warehouse connection needed for unit tests.

---

## PR conventions

Branch naming: `feat/`, `fix/`, `test/`, `chore/`, `docs/`

Always branch off `main`:
```bash
git checkout -b feat/my-feature
# work...
git checkout main
git merge --no-ff feat/my-feature -m "Merge feat/my-feature"
git branch -d feat/my-feature
```

Commit format: `type: description` — types: `feat`, `fix`, `test`, `chore`, `docs`

A good PR:
- Has a clear description of the problem it solves
- Includes tests for new behavior
- Keeps `main.py` thin
- Doesn't introduce `print()` — use `logging`
- Doesn't commit `.env` or key files
