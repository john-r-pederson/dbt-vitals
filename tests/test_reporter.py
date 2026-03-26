import json
import pytest
from unittest.mock import MagicMock, patch
from io import BytesIO
from http.client import HTTPMessage

from reporter import Reporter, ModelReport, _COMMENT_TAG, _parse_next_link, _risk_indicator, _format_size, _TOOL_URL, _escape_md, _GITHUB_COMMENT_MAX_CHARS


def _make_cfg(github_token=None, github_repository=None, pr_number=None, lookback_days=90):
    cfg = MagicMock()
    cfg.GITHUB_TOKEN = github_token
    cfg.GITHUB_REPOSITORY = github_repository
    cfg.PR_NUMBER = pr_number
    cfg.LOOKBACK_DAYS = lookback_days
    return cfg


def _reporter(**kwargs):
    return Reporter(_make_cfg(**kwargs))


def _report(**kwargs):
    """Build a ModelReport with sensible defaults — only override what the test cares about."""
    defaults = dict(
        file_path="models/stg_users.sql",
        new_path=None,
        table_ref="PROD.STAGING.STG_USERS",
        exists=True,
        table_type="BASE TABLE",
        materialization="table",
        size_gb=1.0,
        last_altered="2026-03-01",
        last_read="2026-03-05",
        read_count=42,
        distinct_users=3,
        access_history_available=True,
        downstream_names=[],
    )
    defaults.update(kwargs)
    return ModelReport(**defaults)


# ---------------------------------------------------------------------------
# ModelReport invariants
# ---------------------------------------------------------------------------

def test_model_report_rejects_exists_without_table_ref():
    with pytest.raises(ValueError, match="table_ref"):
        ModelReport(
            file_path="models/stg_users.sql",
            new_path=None,
            table_ref=None,
            exists=True,
            table_type=None,
            materialization=None,
            size_gb=None,
            last_altered=None,
            last_read=None,
        )


# ---------------------------------------------------------------------------
# build_markdown — structural
# ---------------------------------------------------------------------------

def test_comment_tag_is_present():
    assert _COMMENT_TAG in _reporter().build_markdown([])


def test_header_is_present():
    md = _reporter().build_markdown([])
    assert "dbt-vitals" in md
    assert "Warehouse Impact Report" in md


def test_report_count_in_header():
    reports = [_report(), _report(file_path="models/b.sql"), _report(file_path="models/c.sql")]
    assert "3 model(s)" in _reporter().build_markdown(reports)


def test_seven_column_headers_present():
    md = _reporter().build_markdown([])
    assert "Warehouse Table" in md
    assert "Type" in md
    assert "Reads (90d)" in md
    assert "dbt Dependents" in md


# ---------------------------------------------------------------------------
# Existing table — full stats
# ---------------------------------------------------------------------------

def test_existing_table_with_full_stats():
    r = _report(
        file_path="models/core/orders.sql",
        table_ref="PROD.CORE.ORDERS",
        size_gb=42.3,
        last_altered="2026-03-01",
        last_read="2026-02-28",
        read_count=100,
    )
    md = _reporter().build_markdown([r])
    assert "`models/core/orders.sql`" in md
    assert "`PROD.CORE.ORDERS`" in md
    assert "42.3 GB" in md
    assert "2026-03-01" in md
    assert "100" in md


def test_sub_gb_size_shows_mb():
    r = _report(size_gb=0.5)
    md = _reporter().build_markdown([r])
    assert "MB" in md
    assert "GB" not in md.split("|")[3]  # size column


def test_tiny_table_shows_kb():
    r = _report(size_gb=0.0007)
    md = _reporter().build_markdown([r])
    assert "KB" in md


def test_zero_size_table_renders_correctly():
    r = _report(size_gb=0.0)
    assert "0 bytes" in _reporter().build_markdown([r])


def test_view_shows_dash_for_size():
    r = _report(size_gb=None, materialization="view", table_type="VIEW")
    md = _reporter().build_markdown([r])
    assert "— |" in md  # size column shows "—"


# ---------------------------------------------------------------------------
# Materialization type column
# ---------------------------------------------------------------------------

def test_materialization_shown_in_type_column():
    r = _report(materialization="incremental", table_type="BASE TABLE")
    md = _reporter().build_markdown([r])
    assert "incremental" in md


def test_table_type_used_when_no_materialization():
    r = _report(materialization=None, table_type="EXTERNAL TABLE")
    md = _reporter().build_markdown([r])
    assert "external table" in md.lower()


# ---------------------------------------------------------------------------
# Read count / ACCESS_HISTORY availability
# ---------------------------------------------------------------------------

def test_read_count_shown():
    r = _report(read_count=73, access_history_available=True)
    assert "73" in _reporter().build_markdown([r])


def test_zero_reads_shows_zero_not_unavailable():
    r = _report(read_count=0, distinct_users=0, access_history_available=True, last_read=None)
    md = _reporter().build_markdown([r])
    assert "0" in md
    assert "unavailable" not in md.lower()


def test_access_history_unavailable_shows_grant_message():
    r = _report(access_history_available=False, read_count=0)
    md = _reporter().build_markdown([r])
    assert "ACCESS_HISTORY" in md


def test_read_count_shows_user_count_when_nonzero():
    r = _report(read_count=57, distinct_users=4)
    md = _reporter().build_markdown([r])
    assert "57" in md
    assert "(4 users)" in md


def test_zero_reads_no_user_count():
    r = _report(read_count=0, distinct_users=0, access_history_available=True)
    md = _reporter().build_markdown([r])
    assert "0" in md
    assert " users)" not in md  # "N users" suffix should not appear when count is 0


# ---------------------------------------------------------------------------
# Downstream dbt dependencies
# ---------------------------------------------------------------------------

def test_downstream_names_shown():
    r = _report(downstream_names=["fct_orders", "rpt_users"])
    md = _reporter().build_markdown([r])
    assert "`fct_orders`" in md
    assert "`rpt_users`" in md


def test_no_downstream_names_shows_dash():
    r = _report(downstream_names=[])
    md = _reporter().build_markdown([r])
    # Last column should be "—"
    assert "| — |" in md


# ---------------------------------------------------------------------------
# Rename display
# ---------------------------------------------------------------------------

def test_rename_shows_arrow_and_new_path():
    r = _report(file_path="models/old.sql", new_path="models/new.sql")
    md = _reporter().build_markdown([r])
    assert "`models/old.sql`" in md
    assert "`models/new.sql`" in md
    assert "→" in md


def test_pure_deletion_has_no_arrow():
    r = _report(file_path="models/gone.sql", new_path=None)
    md = _reporter().build_markdown([r])
    assert "→" not in md


# ---------------------------------------------------------------------------
# Not-in-manifest and not-in-warehouse rows
# ---------------------------------------------------------------------------

def test_table_not_in_warehouse():
    r = _report(exists=False, size_gb=None, last_altered=None)
    md = _reporter().build_markdown([r])
    assert "_(not in warehouse)_" in md


def test_query_error_shows_distinct_message():
    r = _report(exists=False, size_gb=None, last_altered=None, query_error=True)
    md = _reporter().build_markdown([r])
    assert "_(query error — check role grants)_" in md
    assert "_(not in warehouse)_" not in md


def test_file_not_in_manifest():
    r = _report(table_ref=None, exists=False, size_gb=None)
    md = _reporter().build_markdown([r])
    assert "_(not in manifest)_" in md


def test_multiple_reports_all_appear():
    reports = [
        _report(file_path="models/a.sql", table_ref="DB.S.A"),
        _report(file_path="models/b.sql", table_ref=None, exists=False, size_gb=None),
    ]
    md = _reporter().build_markdown(reports)
    assert "`models/a.sql`" in md
    assert "`models/b.sql`" in md


# ---------------------------------------------------------------------------
# publish routing
# ---------------------------------------------------------------------------

def test_publish_prints_to_stdout_when_no_github_config(capsys):
    _reporter().publish([_report(table_ref=None, exists=False, size_gb=None)])
    assert _COMMENT_TAG in capsys.readouterr().out


def test_publish_uses_stdout_when_token_missing(capsys):
    r = _reporter(github_repository="owner/repo", pr_number="1")  # no token
    r.publish([_report(table_ref=None, exists=False, size_gb=None)])
    assert _COMMENT_TAG in capsys.readouterr().out


def test_publish_calls_github_api_when_fully_configured(monkeypatch):
    r = _reporter(github_token="ghp_fake", github_repository="owner/repo", pr_number="42")
    calls = []
    monkeypatch.setattr(r, "_post_or_update_pr_comment", lambda body: calls.append(body))
    r.publish([_report(table_ref=None, exists=False, size_gb=None)])
    assert len(calls) == 1
    assert _COMMENT_TAG in calls[0]


# ---------------------------------------------------------------------------
# _find_existing_comment — pagination
# ---------------------------------------------------------------------------

def _mock_response(data, status=200, link_header=None):
    """Creates a mock urllib response object."""
    body = json.dumps(data).encode()
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    headers = HTTPMessage()
    if link_header:
        headers["Link"] = link_header
    resp.headers = headers
    return resp


def test_find_existing_comment_returns_id_when_found():
    r = _reporter(github_token="tok", github_repository="o/r", pr_number="1")
    comments = [
        {"id": 100, "body": "unrelated comment"},
        {"id": 200, "body": f"before {_COMMENT_TAG} after"},
    ]
    with patch("urllib.request.urlopen", return_value=_mock_response(comments)):
        result = r._find_existing_comment("o/r", "1", {})
    assert result == 200


def test_find_existing_comment_returns_none_when_not_found():
    r = _reporter(github_token="tok", github_repository="o/r", pr_number="1")
    comments = [{"id": 1, "body": "no tag here"}]
    with patch("urllib.request.urlopen", return_value=_mock_response(comments)):
        result = r._find_existing_comment("o/r", "1", {})
    assert result is None


def test_find_existing_comment_paginates():
    """Comment is on page 2 — must follow the next link."""
    r = _reporter(github_token="tok", github_repository="o/r", pr_number="1")
    page1 = [{"id": 1, "body": "unrelated"}]
    page2 = [{"id": 99, "body": f"report {_COMMENT_TAG}"}]

    resp1 = _mock_response(page1, link_header='<https://api.github.com/page2>; rel="next"')
    resp2 = _mock_response(page2)

    with patch("urllib.request.urlopen", side_effect=[resp1, resp2]):
        result = r._find_existing_comment("o/r", "1", {})
    assert result == 99


# ---------------------------------------------------------------------------
# _parse_next_link
# ---------------------------------------------------------------------------

def test_parse_next_link_returns_url():
    header = '<https://api.github.com/repos/o/r/issues/1/comments?page=2>; rel="next", <https://api.github.com/repos/o/r/issues/1/comments?page=5>; rel="last"'
    assert _parse_next_link(header) == "https://api.github.com/repos/o/r/issues/1/comments?page=2"


def test_parse_next_link_returns_none_when_no_next():
    header = '<https://api.github.com/repos/o/r/issues/1/comments?page=1>; rel="prev"'
    assert _parse_next_link(header) is None


def test_parse_next_link_returns_none_for_empty_header():
    assert _parse_next_link("") is None
    assert _parse_next_link(None) is None


# ---------------------------------------------------------------------------
# _format_size
# ---------------------------------------------------------------------------

def test_format_size_none_returns_dash():
    assert _format_size(None) == "—"


def test_format_size_zero_returns_bytes():
    assert _format_size(0.0) == "0 bytes"


def test_format_size_large_shows_gb():
    assert _format_size(42.3) == "42.3 GB"
    assert _format_size(1.0) == "1.0 GB"


def test_format_size_medium_shows_mb():
    assert "MB" in _format_size(0.5)
    assert "MB" in _format_size(0.1)


def test_format_size_small_shows_kb():
    assert "KB" in _format_size(0.0007)
    assert "KB" in _format_size(0.0001)


# ---------------------------------------------------------------------------
# _escape_md
# ---------------------------------------------------------------------------

def test_escape_md_replaces_pipe():
    assert _escape_md("a|b") == "a\\|b"


def test_escape_md_no_pipe_unchanged():
    assert _escape_md("models/stg_users.sql") == "models/stg_users.sql"


def test_pipe_in_file_path_is_escaped():
    r = _report(file_path="models/pipe|table.sql")
    md = _reporter().build_markdown([r])
    assert "\\|" in md
    # The raw unescaped pipe in the path should not create an extra column
    assert "pipe|table" not in md


def test_pipe_in_table_ref_is_escaped():
    r = _report(table_ref="DB|BAD.SCH.TBL")
    md = _reporter().build_markdown([r])
    assert "DB\\|BAD" in md


# ---------------------------------------------------------------------------
# Footer URL
# ---------------------------------------------------------------------------

def test_footer_links_to_isotrope_tool_not_user_repo():
    """Footer should always point to the dbt-vitals project, not the user's dbt repo."""
    r = _reporter(github_repository="owner/my-dbt-repo")
    md = r.build_markdown([])
    assert _TOOL_URL in md
    assert "owner/my-dbt-repo" not in md


def test_footer_contains_generated_timestamp():
    md = _reporter().build_markdown([])
    assert "UTC" in md


# ---------------------------------------------------------------------------
# Dynamic lookback header
# ---------------------------------------------------------------------------

def test_lookback_days_shown_in_header():
    r = _reporter(lookback_days=30)
    md = r.build_markdown([])
    assert "Reads (30d)" in md


def test_default_lookback_is_90():
    md = _reporter().build_markdown([])
    assert "Reads (90d)" in md


# ---------------------------------------------------------------------------
# Risk indicator
# ---------------------------------------------------------------------------

def test_risk_indicator_red_when_reads_and_deps():
    r = _report(read_count=42, distinct_users=3, downstream_names=["fct_orders"])
    assert _risk_indicator(r) == "🔴 "


def test_risk_indicator_yellow_when_reads_only():
    r = _report(read_count=10, distinct_users=1, downstream_names=[])
    assert _risk_indicator(r) == "🟡 "


def test_risk_indicator_yellow_when_deps_only():
    r = _report(read_count=0, distinct_users=0, access_history_available=True, downstream_names=["fct_orders"])
    assert _risk_indicator(r) == "🟡 "


def test_risk_indicator_absent_when_safe():
    r = _report(read_count=0, distinct_users=0, access_history_available=True, downstream_names=[])
    assert _risk_indicator(r) == ""


def test_risk_indicator_absent_when_access_history_unavailable():
    r = _report(read_count=0, distinct_users=0, access_history_available=False, downstream_names=[])
    assert _risk_indicator(r) == ""


def test_risk_indicator_in_report_markdown():
    r = _report(read_count=5, distinct_users=2, downstream_names=["fct_orders"])
    md = _reporter().build_markdown([r])
    assert "🔴" in md


# ---------------------------------------------------------------------------
# Transitive deps note
# ---------------------------------------------------------------------------

def test_transitive_deps_note_shown_when_dependents_present():
    """When any report has downstream dependents, a note about direct-only scope is added."""
    r = _report(downstream_names=["fct_orders"])
    md = _reporter().build_markdown([r])
    assert "direct downstream" in md.lower()


def test_transitive_deps_note_absent_when_no_dependents():
    """When no report has downstream dependents, the transitive deps note is omitted."""
    r = _report(downstream_names=[])
    md = _reporter().build_markdown([r])
    assert "direct downstream" not in md.lower()


def test_transitive_deps_note_uses_actual_model_name():
    """The dbt ls hint in the transitive deps note uses the stem of the first model with dependents."""
    r = _report(file_path="models/staging/stg_orders.sql", downstream_names=["fct_orders"])
    md = _reporter().build_markdown([r])
    assert "stg_orders+" in md


# ---------------------------------------------------------------------------
# Report truncation
# ---------------------------------------------------------------------------

def test_report_truncated_when_exceeds_github_limit():
    """Reports exceeding the GitHub comment limit are truncated with a notice."""
    long_name = "a_very_long_model_name_that_takes_up_space_in_the_report"
    reports = [
        _report(
            file_path=f"models/staging/finance/reporting/{long_name}_{i:04d}.sql",
            table_ref=f"PRODUCTION_DATABASE.FINANCE_REPORTING_SCHEMA.{long_name.upper()}_{i:04d}",
            downstream_names=[f"dep_model_{j}" for j in range(10)],
        )
        for i in range(500)
    ]
    md = _reporter().build_markdown(reports)
    assert len(md) <= _GITHUB_COMMENT_MAX_CHARS
    assert "truncated" in md.lower()
    assert "omitted" in md.lower()


def test_report_under_limit_is_not_truncated():
    """A small report that fits within the limit must not contain a truncation notice."""
    reports = [_report(), _report(file_path="models/b.sql")]
    md = _reporter().build_markdown(reports)
    assert "truncated" not in md.lower()


# ---------------------------------------------------------------------------
# adapter factory
# ---------------------------------------------------------------------------

def test_factory_raises_for_unknown_warehouse():
    from adapters.factory import get_adapter
    cfg = MagicMock()
    cfg.WAREHOUSE_TYPE = "oracle"
    with pytest.raises(ValueError, match="Unsupported WAREHOUSE_TYPE"):
        get_adapter(cfg)
