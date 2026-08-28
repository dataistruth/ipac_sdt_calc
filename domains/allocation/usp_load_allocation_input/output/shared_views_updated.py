"""
Parallel register_shared_views — 3 workers by default.

Resolves view SQL from (first match):
  1. config_parallel_hooks.get_shared_view_sql_map(cfg)
  2. ai_shared_views.get_shared_view_sql_map(cfg)
  3. Static parse of sibling ai_shared_views.py
  4. Sequential ai_shared_views.register_shared_views (fallback)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from . import config_parallel_hooks
from .ai_shared_views import register_shared_views
from .parallel_config_updated import parallel_workers, run_parallel_tasks

logger = logging.getLogger(__name__)

_CREATE_VIEW_RE = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+TEMP\s+(?:VIEW|TABLE)\s+`?([A-Za-z_][\w]*)`?\s+AS\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)


def _register_sql_view(spark: SparkSession, view_name: str, body_sql: str) -> None:
    spark.sql(f"CREATE OR REPLACE TEMP VIEW `{view_name}` AS {body_sql}")


def _resolve_view_sql_map(cfg: dict) -> dict[str, str]:
    hook_map = config_parallel_hooks.get_shared_view_sql_map(cfg)
    if hook_map:
        return dict(hook_map)

    try:
        from . import ai_shared_views as mod
        if hasattr(mod, "get_shared_view_sql_map"):
            explicit = mod.get_shared_view_sql_map(cfg)
            if explicit:
                return dict(explicit)
    except ImportError:
        pass

    parsed = _parse_view_sql_from_source(cfg)
    if parsed:
        return parsed

    return {}


def _parse_view_sql_from_source(cfg: dict) -> dict[str, str]:
    """Best-effort parse of ai_shared_views.py dict / CREATE VIEW strings."""
    path = Path(__file__).resolve().parent / "ai_shared_views.py"
    if not path.is_file():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace")
    views: dict[str, str] = {}

    # Pattern: "_view_name": "SELECT ..." or '_view_name': f"SELECT ..."
    for match in re.finditer(
        r'["\'](_[A-Za-z][\w]*)["\']\s*:\s*(?:f)?["\']((?:\\.|[^"\'\\])*)["\']',
        text,
    ):
        name, sql = match.group(1), match.group(2)
        if "SELECT" in sql.upper() or "FROM" in sql.upper():
            views[name] = sql.replace("\\n", "\n").replace("\\t", "\t")

    # Pattern: spark.sql("""CREATE OR REPLACE TEMP VIEW _x AS ...""")
    for match in re.finditer(
        r'spark\.sql\(\s*(?:f)?"""([^"]+)"""',
        text,
        re.DOTALL,
    ):
        sql = match.group(1)
        create = _CREATE_VIEW_RE.match(sql.strip())
        if create:
            views[create.group(1)] = create.group(2).strip()

    for match in re.finditer(
        r'spark\.sql\(\s*(?:f)?"([^"]+)"',
        text,
    ):
        sql = match.group(1)
        create = _CREATE_VIEW_RE.match(sql.strip())
        if create:
            views[create.group(1)] = create.group(2).strip()

    # Substitute common cfg placeholders when present in parsed SQL
    if views and cfg:
        run_id = cfg.get("run_id")
        prefix = cfg.get("schema") or cfg.get("database_name") or ""
        catalog = cfg.get("catalog") or cfg.get("catalog_name") or ""
        for name, sql in list(views.items()):
            try:
                views[name] = (
                    sql.replace("{run_id}", str(run_id))
                    .replace("{schema}", str(prefix))
                    .replace("{catalog}", str(catalog))
                )
            except Exception:
                pass

    return views


def _parallel_register_views(
    spark: SparkSession,
    view_map: dict[str, str],
    workers: int,
) -> None:
    if not view_map:
        return

    tasks = [
        (
            name,
            lambda n=name, sql=body: _register_sql_view(spark, n, sql),
        )
        for name, body in view_map.items()
    ]

    # Tasks return None; wrap to satisfy run_parallel_tasks typing
    wrapped: list[tuple[str, Any]] = [
        (name, lambda fn=fn: fn() or {}) for name, fn in tasks
    ]
    run_parallel_tasks(spark, "register_shared_views", wrapped, workers)


def register_shared_views_parallel(spark: SparkSession, cfg: dict) -> None:
    workers = parallel_workers(cfg)
    view_map = _resolve_view_sql_map(cfg)

    if view_map and workers > 1:
        print(
            f"[parallel_config] register_shared_views: {len(view_map)} view(s), "
            f"max_workers={workers}"
        )
        _parallel_register_views(spark, view_map, workers)
        return

    if workers > 1 and not view_map:
        print(
            "[parallel_config] register_shared_views: no SQL map found "
            "(edit config_parallel_hooks.get_shared_view_sql_map); sequential fallback"
        )

    register_shared_views(spark, cfg)
