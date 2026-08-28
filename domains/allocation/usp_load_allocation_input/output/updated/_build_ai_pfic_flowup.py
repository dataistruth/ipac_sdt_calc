"""One-off builder: normalized monolith paste -> optimized ai_pfic_flowup_service.py"""
import re
from pathlib import Path

ROOT = Path(__file__).parent
base_path = ROOT / "_ai_pfic_flowup_service_base.py"
out_path = ROOT / "ai_pfic_flowup_service.py"

text = base_path.read_text()
lines = [ln.rstrip() for ln in text.splitlines()]
collapsed: list[str] = []
for ln in lines:
    if ln == "" and collapsed and collapsed[-1] == "":
        continue
    collapsed.append(ln)
text = "\n".join(collapsed)

HEADER = '''"""
PFIC flowup pipeline — optimized (output/updated/ai_pfic_flowup_service.py).

Loaded by updated.load_allocation_input instead of monolith ai_pfic_flowup_service.

SQL lines: 5700-6900
"""

_MODULE = "updated.ai_pfic_flowup_service"


def _log(msg: str) -> None:
    print(f"[{_MODULE}] {msg}")


_log("module import")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing

logger = logging.getLogger(__name__)


def _load_pfic_foreign_corp_broadcast(spark: SparkSession, cfg: dict) -> DataFrame:
    key = "_pfic_foreign_corp_broadcast"
    if key not in cfg:
        cfg[key] = F.broadcast(read_table(spark, "PficForeignCorpClassificationInput", cfg))
        _log("cached broadcast PficForeignCorpClassificationInput")
    return cfg[key]


def _blocked_classification(pfic_foreign_corp: DataFrame, entity_id: int) -> DataFrame:
    return pfic_foreign_corp.filter(
        (F.lower(F.col("FootnoteClassification")) == "blocked")
        & (F.col("SourceEntityID") == entity_id)
    ).select(
        F.coalesce(F.col("PFICSourceEntityID"), F.lit(0)).alias("PFICSourceEntityID"),
        F.col("EntityID"),
        F.col("PficFootnoteID"),
        F.col("TrackingKey"),
    ).distinct()


def _maybe_inner_checkpoint(spark: SparkSession, df: DataFrame, cfg: dict, label: str) -> DataFrame:
    if cfg.get("skip_inner_pfic_checkpoints", True):
        _log(f"skip inner checkpoint ({label})")
        return df
    from .checkpoint import checkpoint
    _log(f"inner checkpoint ({label})")
    return checkpoint(spark, df, "base_flowup", cfg)


def _cached_zero_fa_only_ids(
    spark: SparkSession,
    cfg: dict,
    reclass_unblocked_df: DataFrame,
    pfic_line_item_df: DataFrame,
) -> DataFrame:
    cache_key = "_zero_fa_only_ids_result"
    if cache_key not in cfg:
        _log("compute _zero_fa_only_ids")
        cfg[cache_key] = _build_zero_fa_only_ids(spark, cfg, reclass_unblocked_df, pfic_line_item_df)
    else:
        _log("reuse cached _zero_fa_only_ids")
    return cfg[cache_key]

'''

idx = text.find("def _collect_result")
body = text[idx:]

# register_reclass_unblocked (before global PficForeignCorp replace)
body = re.sub(
    r"def register_reclass_unblocked\(spark: SparkSession, cfg: dict\) -> DataFrame:",
    "def register_reclass_unblocked(\n    spark: SparkSession,\n    cfg: dict,\n    pfic_foreign_corp: DataFrame | None = None,\n) -> DataFrame:",
    body,
    count=1,
)

body = re.sub(
    r"    reclass_data = spark\.table\(\"_reclass_data\"\)\n\n    pfic_foreign_corp = read_table\(spark, \"PficForeignCorpClassificationInput\", cfg\)\n\n    blocked = pfic_foreign_corp\.filter\(\n\n        \(F\.lower\(F\.col\(\"FootnoteClassification\"\)\) == \"blocked\"\)\n\n        & \(F\.col\(\"SourceEntityID\"\) == entity_id\)\n\n    \)\.select\(\n\n        F\.coalesce\(F\.col\(\"PFICSourceEntityID\"\), F\.lit\(0\)\)\.alias\(\"PFICSourceEntityID\"\),\n\n        F\.col\(\"EntityID\"\),\n\n        F\.col\(\"PficFootnoteID\"\),\n\n        F\.col\(\"TrackingKey\"\),\n\n    \)\.distinct\(\)",
    "    reclass_data = spark.table(\"_reclass_data\")\n\n    if pfic_foreign_corp is None:\n        pfic_foreign_corp = _load_pfic_foreign_corp_broadcast(spark, cfg)\n\n    blocked = _blocked_classification(pfic_foreign_corp, entity_id)",
    body,
    count=1,
)

step2_old = re.compile(
    r"    reclass_wf_id = cfg\.get\(\"lookthrough_reclass_workflow_id\", 0\)\n\n    if True:\n\n        reclass_data = spark\.table\(\"_reclass_data\"\)\n\n        pfic_foreign_corp = read_table\(spark, \"PficForeignCorpClassificationInput\", cfg\)\n\n.*?\n        reclass_unblocked\.createOrReplaceTempView\(f\"_reclass_unblocked_\{run_id\}\"\)\n\n",
    re.DOTALL,
)
step2_new = '''    reclass_wf_id = cfg.get("lookthrough_reclass_workflow_id", 0)

    if reclass_wf_id > 0:
        _log(f"Step 2 lookthrough reclass workflow_id={reclass_wf_id}")
        reclass_data = spark.table("_reclass_data")
        pfic_foreign_corp = _load_pfic_foreign_corp_broadcast(spark, cfg)
        reclass_unblocked = register_reclass_unblocked(spark, cfg, pfic_foreign_corp=pfic_foreign_corp)

'''
body, n = step2_old.subn(step2_new, body, count=1)
if n != 1:
    raise SystemExit(f"Step 2 replace failed: {n}")

body = body.replace(
    "read_table(spark, \"PficForeignCorpClassificationInput\", cfg)",
    "_load_pfic_foreign_corp_broadcast(spark, cfg)",
)

# checkpoint calls
body = body.replace(
    "base_flowup = checkpoint(spark, base_flowup, \"base_flowup\", cfg)",
    "base_flowup = _maybe_inner_checkpoint(spark, base_flowup, cfg, \"post-reclass\")",
)
# second occurrence label
parts = body.split("base_flowup = _maybe_inner_checkpoint(spark, base_flowup, cfg, \"post-reclass\")")
if len(parts) == 3:
    body = parts[0] + "base_flowup = _maybe_inner_checkpoint(spark, base_flowup, cfg, \"post-reclass\")" + parts[1] + "base_flowup = _maybe_inner_checkpoint(spark, base_flowup, cfg, \"post-zero\")" + parts[2]
elif len(parts) == 2:
    pass
else:
    raise SystemExit(f"checkpoint replace count unexpected: {len(parts)}")

# _fa_only uses cache
body = body.replace(
    "_fa_only = _build_zero_fa_only_ids(spark, cfg, reclass_unblocked, pfic_line_item).select(",
    "_fa_only = _cached_zero_fa_only_ids(spark, cfg, reclass_unblocked, pfic_line_item).select(",
)

body = body.replace(
    "footnote_amounts_only_ids = _build_zero_fa_only_ids(\n\n                spark, cfg, _ru_view, pfic_line_item\n\n            )",
    "footnote_amounts_only_ids = _cached_zero_fa_only_ids(spark, cfg, _ru_view, pfic_line_item)",
)

# pfic_foreign_corp_za and _2 already use broadcast via global replace

# has_domestic_blockers without first()
body = body.replace(
    "has_domestic_blockers = domestic_blocker_ids.limit(1).first() is not None",
    "has_domestic_blockers = domestic_blocker_ids.head(1) is not None",
)

# build_pfic_flowup_pipeline entry log
body = body.replace(
    "    log_section(\"build_pfic_flowup_pipeline\")\n\n    t0 = time.time()",
    "    log_section(\"build_pfic_flowup_pipeline\")\n    _log(\"build_pfic_flowup_pipeline START\")\n    t0 = time.time()",
    1,
)

body = body.replace(
    "    log_timing(\"build_pfic_flowup_pipeline\", t0)\n\n    return base_flowup",
    "    log_timing(\"build_pfic_flowup_pipeline\", t0)\n    _log(\"build_pfic_flowup_pipeline END\")\n    return base_flowup",
    1,
)

# build_custom_footnote_input - join instead of collect for excluded statuses
body = body.replace(
    "    excluded_statuses = workflow_status.filter(\n\n        F.lower(F.col(\"EnumerationName\")).isin(\"rejected\", \"err_critical\", \"err_noncritical\")\n\n    ).select(\"StatusID\")\n\n    excluded_status_ids = [row[\"StatusID\"] for row in excluded_statuses.collect()]\n",
    "    excluded_statuses = workflow_status.filter(\n        F.lower(F.col(\"EnumerationName\")).isin(\"rejected\", \"err_critical\", \"err_noncritical\")\n    ).select(\"StatusID\")\n\n    excluded_status_ids = None  # use join anti-pattern below\n",
)

body = body.replace(
    "    valid_txn_log = transaction_log.filter(\n\n        (F.col(\"ClientID\") == client_id)\n\n        & (F.col(\"TaxPeriodID\") == tax_period_id)\n\n        & (F.col(\"PhaseID\") == phase_id)\n\n        & (~F.col(\"StatusID\").isin(excluded_status_ids))\n\n        & (F.col(\"StatusID\") != 0)\n\n    )\n",
    "    valid_txn_log = transaction_log.filter(\n        (F.col(\"ClientID\") == client_id)\n        & (F.col(\"TaxPeriodID\") == tax_period_id)\n        & (F.col(\"PhaseID\") == phase_id)\n        & (F.col(\"StatusID\") != 0)\n    ).join(excluded_statuses, \"StatusID\", \"left_anti\")\n",
)

body = body.replace(
    "    global_event_ids_df = enu_event.filter(F.lower(F.col(\"EventName\")).isin([e.lower() for e in global_event_names])) \\\n\n        .select(\"EventTypeID\")\n\n    global_event_ids = [row[\"EventTypeID\"] for row in global_event_ids_df.collect()]\n",
    "    global_event_ids_df = F.broadcast(\n        enu_event.filter(F.lower(F.col(\"EventName\")).isin([e.lower() for e in global_event_names])).select(\"EventTypeID\")\n    )\n    global_event_ids = None  # use join below\n",
)

body = body.replace(
    "    entity_specific_txn = valid_txn_log.filter(~F.col(\"EventTypeID\").isin(global_event_ids)) \\\n",
    "    entity_specific_txn = valid_txn_log.join(global_event_ids_df, \"EventTypeID\", \"left_anti\") \\\n",
)

body = body.replace(
    "    global_txn = valid_txn_log.filter(F.col(\"EventTypeID\").isin(global_event_ids)) \\\n",
    "    global_txn = valid_txn_log.join(global_event_ids_df, \"EventTypeID\", \"inner\") \\\n",
)

body = body.replace(
    "    log_section(\"build_custom_footnote_input\")\n\n    t0 = time.time()",
    "    log_section(\"build_custom_footnote_input\")\n    _log(\"build_custom_footnote_input START\")\n    t0 = time.time()",
    1,
)

# check_pfic_xml_override_alert early log
body = body.replace(
    "    log_section(\"check_pfic_xml_override_alert\")\n\n    t0 = time.time()",
    "    log_section(\"check_pfic_xml_override_alert\")\n    _log(\"check_pfic_xml_override_alert START\")\n    t0 = time.time()",
    1,
)

# Remove unused delta import if present
body = body.replace("from delta.tables import DeltaTable\n\n", "")

out_path.write_text(HEADER + body)

# Fix illegal blank lines after line-continuation backslashes
lines = out_path.read_text().splitlines()
fixed: list[str] = []
for ln in lines:
    if ln.strip() == "" and fixed and fixed[-1].rstrip().endswith("\\"):
        continue
    fixed.append(ln)
out_path.write_text("\n".join(fixed) + "\n")

import ast
ast.parse(out_path.read_text())
print(f"Wrote {out_path} ({out_path.stat().st_size} bytes, {len(fixed)} lines)")
