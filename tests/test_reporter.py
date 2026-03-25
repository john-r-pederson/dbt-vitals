import json
import pytest
from unittest.mock import MagicMock, patch
from io import BytesIO
from http.client import HTTPMessage

from reporter import Reporter, ModelReport, _COMMENT_TAG, _parse_next_link


def _make_cfg(github_token=None, github_repository=None, pr_number=None):
    cfg = MagicMock()
    cfg.GITHUB_TOKEN = github_token
    cfg.GITHUB_REPOSITORY = github_repository
    cfg.PR_NUMBER = pr_number
    return cfg


def _reporter(**kwargs):
    return Reporter(_make_cfg(**kwargs))


# ---------------------------------------------------------------------------
# build_markdown
# ---------------------------------------------------------------------------

def test_comment_tag_is_present():
    assert _COMMENT_TAG in _reporter().build_markdown([])


def test_header_is_present():
    md = _reporter().build_markdown([])
    assert "Isotrope" in md
    assert "Warehouse Impact Report" in md


def test_report_count_in_header():
    reports = [
        ModelReport("a.sql", "DB.S.A", True, 1.0, "2026-01-01", None),
        ModelReport("b.sql", "DB.S.B", True, 2.0, "2026-01-02", None),
        ModelReport("c.sql", None, False, 0, None, None),
    ]
    assert "3 model(s)" in _reporter().build_markdown(reports)


def test_existing_table_with_full_stats():
    r = ModelReport("models/core/orders.sql", "PROD.CORE.ORDERS", True, 42.3, "2026-03-01", "2026-02-28")
    md = _reporter().build_markdown([r])
    assert "`models/core/orders.sql`" in md
    assert "`PROD.CORE.ORDERS`" in md
    assert "42.3 GB" in md
    assert "2026-03-01" in md
    assert "2026-02-28" in md


def test_existing_table_without_last_read():
    r = ModelReport("models/core/orders.sql", "PROD.CORE.ORDERS", True, 1.0, "2026-01-01", None)
    assert "_(unavailable)_" in _reporter().build_markdown([r])


def test_table_not_in_warehouse():
    r = ModelReport("models/staging/stg_old.sql", "PROD.STAGING.STG_OLD", False, 0, None, None)
    md = _reporter().build_markdown([r])
    assert "_(not in warehouse)_" in md
    assert "`PROD.STAGING.STG_OLD`" in md


def test_file_not_in_manifest():
    r = ModelReport("models/legacy/ghost.sql", None, False, 0, None, None)
    md = _reporter().build_markdown([r])
    assert "_(not in manifest)_" in md


def test_zero_size_table_renders_correctly():
    r = ModelReport("models/empty.sql", "DB.SCH.EMPTY", True, 0.0, "2026-01-01", None)
    md = _reporter().build_markdown([r])
    assert "0.0 GB" in md


def test_multiple_reports_all_appear():
    reports = [
        ModelReport("models/a.sql", "DB.S.A", True, 1.0, "2026-01-01", "2026-01-05"),
        ModelReport("models/b.sql", None, False, 0, None, None),
    ]
    md = _reporter().build_markdown(reports)
    assert "`models/a.sql`" in md
    assert "`models/b.sql`" in md


# ---------------------------------------------------------------------------
# publish routing
# ---------------------------------------------------------------------------

def test_publish_prints_to_stdout_when_no_github_config(capsys):
    _reporter().publish([ModelReport("models/a.sql", None, False, 0, None, None)])
    assert _COMMENT_TAG in capsys.readouterr().out


def test_publish_uses_stdout_when_token_missing(capsys):
    r = _reporter(github_repository="owner/repo", pr_number="1")  # no token
    r.publish([ModelReport("models/a.sql", None, False, 0, None, None)])
    assert _COMMENT_TAG in capsys.readouterr().out


def test_publish_calls_github_api_when_fully_configured(monkeypatch):
    r = _reporter(github_token="ghp_fake", github_repository="owner/repo", pr_number="42")
    calls = []
    monkeypatch.setattr(r, "_post_or_update_pr_comment", lambda body: calls.append(body))
    r.publish([ModelReport("models/a.sql", None, False, 0, None, None)])
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
# _validate_identifier (adapter)
# ---------------------------------------------------------------------------

def test_validate_identifier_accepts_valid():
    from adapters.snowflake_adapter import _validate_identifier
    assert _validate_identifier("my_table") == "MY_TABLE"
    assert _validate_identifier("REVENUE$USD") == "REVENUE$USD"
    assert _validate_identifier("TABLE123") == "TABLE123"


def test_validate_identifier_rejects_invalid():
    from adapters.snowflake_adapter import _validate_identifier
    with pytest.raises(ValueError):
        _validate_identifier("table; DROP TABLE users--")
    with pytest.raises(ValueError):
        _validate_identifier("table name")
    with pytest.raises(ValueError):
        _validate_identifier("table.name")


# ---------------------------------------------------------------------------
# adapter factory
# ---------------------------------------------------------------------------

def test_factory_raises_for_unknown_warehouse():
    from adapters.factory import get_adapter
    cfg = MagicMock()
    cfg.WAREHOUSE_TYPE = "oracle"
    with pytest.raises(ValueError, match="Unsupported WAREHOUSE_TYPE"):
        get_adapter(cfg)
