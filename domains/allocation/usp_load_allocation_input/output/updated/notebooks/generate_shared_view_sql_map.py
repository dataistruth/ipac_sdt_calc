# Databricks notebook source
# MAGIC %md
# MAGIC # Generate `CUSTOM_VIEW_ENTRIES` for `shared_view_sql_map.py`
# MAGIC
# MAGIC Run once in workspace with `ai_shared_views.py` on `sys.path`.
# MAGIC Copy printed Python into `output/updated/shared_view_sql_map.py`.

# COMMAND ----------

dbutils.widgets.text(
    "sp_name",
    "usp_load_allocation_input",
    "SP folder under AllocationV2/",
)
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks_msingh/Source",
    "Monolith Source/",
)

sp_name = dbutils.widgets.get("sp_name").strip()
source_path = dbutils.widgets.get("source_path").strip()

import sys
import re
from pathlib import Path

if source_path and source_path not in sys.path:
    sys.path.insert(0, source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A — parse `ai_shared_views.py` from disk

# COMMAND ----------

views_path = Path(source_path) / "AllocationV2" / sp_name / "output" / "ai_shared_views.py"
print(f"ai_shared_views path: {views_path}")

if not views_path.is_file():
    raise FileNotFoundError(f"Not found: {views_path}")

text = views_path.read_text(encoding="utf-8", errors="replace")
entries: list[tuple[str, str]] = []

for match in re.finditer(
    r'["\'](_[A-Za-z][\w]*)["\']\s*:\s*(?:f)?["\']((?:\\.|[^"\'\\])*)["\']',
    text,
):
    name, sql = match.group(1), match.group(2)
    if "SELECT" in sql.upper() or "FROM" in sql.upper():
        sql = sql.replace("\\n", "\\n").replace("\\t", "\\t")
        entries.append((name, sql))

create_re = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+TEMP\s+(?:VIEW|TABLE)\s+`?([A-Za-z_][\w]*)`?\s+AS\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)
for match in re.finditer(r'spark\.sql\(\s*(?:f)?"""([^"]+)"""', text, re.DOTALL):
    created = create_re.match(match.group(1).strip())
    if created:
        entries.append((created.group(1), created.group(2).strip().replace("\n", "\\n")))

print(f"Found {len(entries)} view(s) via static parse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B — capture CREATE TEMP VIEW from `spark.sql` (if Option A misses views)

# COMMAND ----------

if len(entries) < 3:
    import importlib

    mod = importlib.import_module(f"AllocationV2.{sp_name}.output.ai_shared_views")
    captured: list[tuple[str, str]] = []

    class _CaptureSpark:
        def __init__(self, real):
            self._real = real

        def sql(self, query: str):
            m = create_re.match(query.strip())
            if m:
                captured.append((m.group(1), m.group(2).strip()))
            return self._real.sql(query)

        def __getattr__(self, name):
            return getattr(self._real, name)

    minimal_cfg = {
        "run_id": 1,
        "catalog": "QA7",
        "schema": "IPC_2025_QA7_15348",
        "catalog_name": "QA7",
        "database_name": "IPC_2025_QA7_15348",
    }
    try:
        mod.register_shared_views(_CaptureSpark(spark), minimal_cfg)
        print(f"Captured {len(captured)} view(s) via runtime hook")
        entries.extend(captured)
    except Exception as exc:
        print(f"Runtime capture skipped: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paste into `output/updated/shared_view_sql_map.py`

# COMMAND ----------

seen: set[str] = set()
lines = ["CUSTOM_VIEW_ENTRIES: list[tuple[str, str]] = ["]
for name, sql in entries:
    if name in seen:
        continue
    seen.add(name)
    # Normalize to {p} / {rid} placeholders where possible
    body = sql.replace("\\n", "\n").replace("\\t", "\t")
    body = body.replace("'", "\\'")
    lines.append(f'    ("{name}", "{body}"),')
lines.append("]")

print("\n".join(lines))
