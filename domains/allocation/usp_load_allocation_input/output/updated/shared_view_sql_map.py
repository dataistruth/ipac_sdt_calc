"""
Shared view SQL map — optional fallback for SQL-only views.

usp_load_allocation_input uses DataFrame builders in shared_views_builders.py
(mirrors ai_shared_views.py). CUSTOM_VIEW_ENTRIES is only needed for extra SQL views.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

from Common_V2.core.helpers import table_prefix

ViewBuilder = Callable[[str, int], str]

# ---------------------------------------------------------------------------
# EDIT: paste view name + SQL body from ai_shared_views.py
# Use {p} for table_prefix and {rid} for run_id in SQL strings.
# Example:
#   ("_k1_input", "SELECT * FROM {p}.K1Input WHERE RunID = {rid}"),
# ---------------------------------------------------------------------------
CUSTOM_VIEW_ENTRIES: list[tuple[str, str]] = [
    # ("_example_view", "SELECT * FROM {p}.SomeTable WHERE RunID = {rid}"),
]


def _format_sql(sql: str, prefix: str, run_id: int) -> str:
    return (
        sql.replace("{p}", prefix)
        .replace("{rid}", str(run_id))
        .replace("{run_id}", str(run_id))
    )


def _build_from_custom_entries(prefix: str, run_id: int) -> dict[str, str]:
    if not CUSTOM_VIEW_ENTRIES:
        return {}
    return {
        name: _format_sql(sql, prefix, run_id)
        for name, sql in CUSTOM_VIEW_ENTRIES
    }


def _try_ai_shared_views_hook(cfg: dict) -> dict[str, str] | None:
    try:
        from .. import ai_shared_views as mod
    except ImportError:
        return None

    if hasattr(mod, "get_shared_view_sql_map"):
        result = mod.get_shared_view_sql_map(cfg)
        if result:
            return dict(result)

    if hasattr(mod, "build_shared_view_sql_map"):
        result = mod.build_shared_view_sql_map(cfg)
        if result:
            return dict(result)

    return None


def _parse_sibling_ai_shared_views(cfg: dict) -> dict[str, str]:
    """Best-effort parse of ai_shared_views.py next to this module."""
    path = Path(__file__).resolve().parent.parent / "ai_shared_views.py"
    if not path.is_file():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace")
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    views: dict[str, str] = {}

    create_re = re.compile(
        r"CREATE\s+OR\s+REPLACE\s+TEMP\s+(?:VIEW|TABLE)\s+`?([A-Za-z_][\w]*)`?\s+AS\s+(.*)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in re.finditer(
        r'["\'](_[A-Za-z][\w]*)["\']\s*:\s*(?:f)?["\']((?:\\.|[^"\'\\])*)["\']',
        text,
    ):
        name, sql = match.group(1), match.group(2)
        if "SELECT" in sql.upper() or "FROM" in sql.upper():
            views[name] = _format_sql(
                sql.replace("\\n", "\n").replace("\\t", "\t"),
                prefix,
                run_id,
            )

    for match in re.finditer(r'spark\.sql\(\s*(?:f)?"""([^"]+)"""', text, re.DOTALL):
        created = create_re.match(match.group(1).strip())
        if created:
            views[created.group(1)] = created.group(2).strip()

    for match in re.finditer(r'spark\.sql\(\s*(?:f)?"([^"]+)"', text):
        created = create_re.match(match.group(1).strip())
        if created:
            views[created.group(1)] = created.group(2).strip()

    return views


def get_shared_view_sql_map(cfg: dict) -> dict[str, str] | None:
    """
    Return view_name → SQL body for CREATE TEMP VIEW ... AS <body>.

    Resolution order:
      1. CUSTOM_VIEW_ENTRIES in this file
      2. ai_shared_views.get_shared_view_sql_map / build_shared_view_sql_map
      3. Parse sibling ai_shared_views.py
    """
    prefix = table_prefix(cfg)
    run_id = int(cfg["run_id"])

    custom = _build_from_custom_entries(prefix, run_id)
    if custom:
        return custom

    hook = _try_ai_shared_views_hook(cfg)
    if hook:
        return hook

    parsed = _parse_sibling_ai_shared_views(cfg)
    if parsed:
        return parsed

    return None
