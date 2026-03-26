import json
import logging
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Hidden tag used to find and update an existing dbt-vitals comment on re-runs
_COMMENT_TAG = "<!-- dbt-vitals-report -->"

# GitHub PR comment hard limit is 65,536 chars; leave a small buffer
_GITHUB_COMMENT_MAX_CHARS = 65_000

# GitHub Actions injects GITHUB_API_URL; defaults to github.com API.
# GitHub Enterprise Server users get the correct URL automatically.
_GITHUB_API_BASE = os.environ.get("GITHUB_API_URL", "https://api.github.com")

# Canonical URL for the dbt-vitals project — always used in the report footer,
# independent of the user's GITHUB_REPOSITORY.
_TOOL_URL = "https://github.com/Laskr/dbt-vitals"


@dataclass
class ModelReport:
    file_path: str
    new_path: str | None           # set for renames; None for pure deletions
    table_ref: str | None          # "DB.SCHEMA.TABLE" or None if not in manifest
    exists: bool
    table_type: str | None         # "BASE TABLE", "VIEW", "EXTERNAL TABLE", etc.
    materialization: str | None    # from manifest: "table", "view", "incremental", etc.
    size_gb: float | None          # None for views (no storage)
    last_altered: str | None
    last_read: str | None
    read_count: int = 0
    distinct_users: int = 0
    access_history_available: bool = True
    downstream_names: list[str] = field(default_factory=list)
    query_error: bool = False    # True = INFORMATION_SCHEMA query failed (permissions); False = genuinely absent

    def __post_init__(self) -> None:
        if self.exists and self.table_ref is None:
            raise ValueError("ModelReport cannot have exists=True with table_ref=None")


class Reporter:
    """Builds and publishes the Warehouse Impact Report as a GitHub PR comment or stdout."""

    def __init__(self, cfg: Any) -> None:
        """Store GitHub context and lookback period from cfg."""
        self.github_token = cfg.GITHUB_TOKEN
        self.github_repository = cfg.GITHUB_REPOSITORY
        self.pr_number = cfg.PR_NUMBER
        self.lookback_days = cfg.LOOKBACK_DAYS

    def build_markdown(self, reports: list[ModelReport]) -> str:
        """Render a list of ModelReports into the full Markdown PR comment body."""
        header = [
            _COMMENT_TAG,
            "## 🔍 dbt-vitals: Warehouse Impact Report",
            "",
            f"> **{len(reports)} model(s) deleted or renamed in this PR.** Review before merging.",
            "",
            f"| Model | Warehouse Table | Type | Size | Last Altered | Reads ({self.lookback_days}d) | dbt Dependents |",
            "| :--- | :--- | :--- | ---: | :--- | ---: | :--- |",
        ]

        rows = []
        for r in reports:
            # Risk indicator — quick visual triage signal
            risk = _risk_indicator(r)

            # Model cell — show rename destination if applicable; escape pipes in paths
            fp = _escape_md(r.file_path)
            if r.new_path:
                np = _escape_md(r.new_path)
                model = f"{risk}`{fp}` _(→ `{np}`)_"
            else:
                model = f"{risk}`{fp}`"

            if r.table_ref is None:
                rows.append(f"| {model} | _(not in manifest)_ | — | — | — | — | — |")
                continue

            table = f"`{_escape_md(r.table_ref)}`"

            if not r.exists:
                if r.query_error:
                    not_found_msg = "_(query error — check role grants)_"
                else:
                    not_found_msg = "_(not in warehouse)_"
                rows.append(f"| {model} | {table} | — | {not_found_msg} | — | — | — |")
                continue

            # Type — prefer manifest materialization, fall back to warehouse TABLE_TYPE
            mat = r.materialization or ""
            wh_type = (r.table_type or "").replace("BASE TABLE", "table").lower()
            type_cell = mat or wh_type or "—"

            # Size — views have no storage; use human-readable units
            size = _format_size(r.size_gb)

            altered = r.last_altered or "—"

            # Reads — distinguish unavailable from zero; surface distinct user count
            if not r.access_history_available:
                reads = "_(no ACCESS_HISTORY grant)_"
            else:
                reads = str(r.read_count)
                if r.distinct_users > 0:
                    reads += f" ({r.distinct_users} users)"

            # dbt downstream dependents
            if r.downstream_names:
                deps = ", ".join(f"`{n}`" for n in r.downstream_names)
            else:
                deps = "—"

            rows.append(f"| {model} | {table} | {type_cell} | {size} | {altered} | {reads} | {deps} |")

        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        has_dependents = any(r.downstream_names for r in reports)

        footer = [
            "",
            "> ⚠️ Tables with recent reads or dbt dependents may have active consumers outside this PR.",
        ]
        if has_dependents:
            example_model = next(
                (os.path.splitext(os.path.basename(r.file_path))[0] for r in reports if r.downstream_names),
                "<model>",
            )
            footer.append(
                "> ℹ️ **dbt Dependents** shows direct downstream models only — "
                f"run `dbt ls --select {example_model}+` for the full lineage."
            )
        footer += [
            "",
            "---",
            f"_Generated by [dbt-vitals]({_TOOL_URL}) · {generated_at}_",
        ]

        # Truncate rows if the report would exceed GitHub's comment size limit
        full = "\n".join(header + rows + footer)
        if len(full) > _GITHUB_COMMENT_MAX_CHARS:
            included = list(rows)
            while included:
                included.pop()
                omitted = len(rows) - len(included)
                notice = [
                    f"> ⚠️ Report truncated — {omitted} model(s) omitted to fit "
                    "GitHub's comment limit. See Action logs for full output.",
                ]
                if len("\n".join(header + included + notice + footer)) <= _GITHUB_COMMENT_MAX_CHARS:
                    omitted_paths = [r.file_path for r in reports[len(included):]]
                    logger.warning(
                        f"Report truncated: {omitted} model(s) omitted from PR comment: "
                        + ", ".join(omitted_paths)
                    )
                    return "\n".join(header + included + notice + footer)
            omitted = len(rows)
            notice = [
                f"> ⚠️ Report truncated — {omitted} model(s) omitted to fit "
                "GitHub's comment limit. See Action logs for full output.",
            ]
            omitted_paths = [r.file_path for r in reports]
            logger.warning(
                f"Report truncated: {omitted} model(s) omitted from PR comment: "
                + ", ".join(omitted_paths)
            )
            return "\n".join(header + notice + footer)

        return full

    def publish(self, reports: list[ModelReport]) -> None:
        """Post the report as a PR comment if GitHub context is available, otherwise print to stdout."""
        body = self.build_markdown(reports)

        if self.github_token and self.github_repository and self.pr_number:
            self._post_or_update_pr_comment(body)
        else:
            logger.warning(
                "PR comment context missing (GITHUB_TOKEN / GITHUB_REPOSITORY / PR_NUMBER). "
                "Printing report to stdout instead. This is expected in local dev."
            )
            print(body)

    def _post_or_update_pr_comment(self, body: str) -> None:
        """Create a new PR comment or update the existing dbt-vitals comment (identified by hidden tag)."""
        repo = self.github_repository
        pr = self.pr_number
        headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        }

        existing_id = self._find_existing_comment(repo, pr, headers)

        if existing_id:
            url = f"{_GITHUB_API_BASE}/repos/{repo}/issues/comments/{existing_id}"
            method = "PATCH"
            logger.info(f"Updating existing dbt-vitals comment #{existing_id}...")
        else:
            url = f"{_GITHUB_API_BASE}/repos/{repo}/issues/{pr}/comments"
            method = "POST"
            logger.info("Posting new dbt-vitals comment...")

        payload = json.dumps({"body": body}).encode()
        req = urllib.request.Request(url, data=payload, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status in (200, 201):
                    logger.info("PR comment published.")
                else:
                    logger.warning(f"Unexpected response status: {resp.status}")
        except urllib.error.HTTPError as e:
            logger.error(f"Failed to post PR comment: {e.code} {e.reason}")
            sys.exit(1)

    def _find_existing_comment(self, repo: str, pr: str, headers: dict[str, str]) -> int | None:
        """
        Returns the comment ID of an existing dbt-vitals comment, or None.
        Paginates through all pages so PRs with >100 comments are handled correctly.
        """
        url = f"{_GITHUB_API_BASE}/repos/{repo}/issues/{pr}/comments?per_page=100"

        while url:
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    comments = json.loads(resp.read())
                    for comment in comments:
                        if _COMMENT_TAG in comment.get("body", ""):
                            return comment["id"]
                    # Follow pagination via Link header
                    url = _parse_next_link(resp.headers.get("Link", ""))
            except Exception as e:
                logger.warning(f"Could not fetch existing comments: {e}")
                return None

        return None


def _escape_md(text: str) -> str:
    """Escapes pipe characters so they don't corrupt Markdown table structure."""
    return text.replace("|", "\\|")


def _format_size(size_gb: float | None) -> str:
    """Returns a human-readable file size string (KB / MB / GB) from a GB float."""
    if size_gb is None:
        return "—"
    if size_gb == 0.0:
        return "0 bytes"
    if size_gb >= 1:
        return f"{size_gb:.1f} GB"
    mb = size_gb * 1024
    if mb >= 1:
        return f"{mb:.1f} MB"
    kb = mb * 1024
    return f"{kb:.0f} KB"


def _risk_indicator(r: "ModelReport") -> str:
    """
    Returns a risk emoji prefix for the model cell based on read activity and dbt dependents.
    🔴 = actively read AND has dbt dependents (highest impact)
    🟡 = either reads OR dependents (medium impact)
    (empty) = no reads and no dependents (likely safe)
    """
    has_reads = r.access_history_available and r.read_count > 0
    has_deps = bool(r.downstream_names)
    if has_reads and has_deps:
        return "🔴 "
    if has_reads or has_deps:
        return "🟡 "
    return ""


def _parse_next_link(link_header: str) -> str | None:
    """Parses the GitHub Link header and returns the 'next' URL if present."""
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, *rel_parts = part.strip().split(";")
        rel = " ".join(rel_parts).strip()
        if rel == 'rel="next"':
            return url_part.strip().strip("<>")
    return None
