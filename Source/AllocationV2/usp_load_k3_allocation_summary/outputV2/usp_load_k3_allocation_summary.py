"""usp_load_k3_allocation_summary.py

Converted from: dbo.uspLoadK3AllocationSummary
Source SQL Server: usazutaxw00110.us.deloitte.com / iPC_2025_QA7_15347

Rounds per-partner K3 look-through allocation amounts to whole numbers and
redistributes the rounding difference so per-partner / per-country sums tie back
to the K1 amounts. Two strategies:
  * country-level (Country Level Rounding Logic = 'C')  -> _country_rounding.py (S8)
  * standard highest-country plug (ELSE)                -> _rounding.py (S9)
then writes K3AllocationSummary and optionally clears fed tables.

Sections (see _plan.md / _state/logic_review.md):
  S1  load_sp_config            — config scalars + @IncomeAttrImportTransID
  S2-S5   _prep.py              — inputs + K1 summary amounts
  S6-S7   _summary.py           — rounded K3 summary + rounding difference
  S8      _country_rounding.py  — country-level rounding (pure PySpark, per-rank checkpoints)
  S9      _rounding.py          — standard rounding
  S10     _finalize.py          — 6a/6b 'after' + attribute/country backfill
  S11 write_k3_allocation_summary as Parquet via GenericResultStorer
  S12 conditional fed-table cleanup deletes — REMOVED (per lead; see logic_review.md).
      Destructive cross-table maintenance side-effect gated by a legacy ClearFedTables
      flag; table lifecycle is owned by orchestration, not this leaf SP.
  S13 framework logging (uspAddAllocationLog) — OMITTED (framework)

Usage:
    from AllocationV2.usp_load_k3_allocation_summary.outputV2.usp_load_k3_allocation_summary \
        import run_usp_load_k3_allocation_summary
    run_usp_load_k3_allocation_summary(
        spark, EntityID=3439, ClientID=15347, TaxPeriodID=1, RunID=955,
        CatalogName="dev7", SchemaName="<schema>")
"""

import json
import logging
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from Common_V2.core.helpers import read_table, get_logger, log_timing
from Common_V2.core.config import load_common_config
from Common_V2.core.checkpoint import (
    checkpoint as common_checkpoint,
    drop_checkpoints,
)
from Common_V2.core.execution_profiles import resolve_execution_profile
from Common_V2.core.parallel_helpers import (
    normalize_workers,
    parse_enabled_groups,
    run_parallel,
)

checkpoint = common_checkpoint
from Common_V2.core.generic_result_storer import GenericResultStorer

from AllocationV2.usp_load_k3_allocation_summary.outputV2._prep import (
    build_country_sic_lines,
    build_income_attr_rounding_import,
    build_k3_detail,
    build_rounding_flags,
    build_mapped_lines,
    has_mapped_lines,
    build_k1_summary_amounts,
)
from AllocationV2.usp_load_k3_allocation_summary.outputV2._summary import build_k3_summary_rounded, build_rounding_difference
from AllocationV2.usp_load_k3_allocation_summary.outputV2._country_rounding import apply_country_level_rounding
from AllocationV2.usp_load_k3_allocation_summary.outputV2._rounding import apply_standard_rounding
from AllocationV2.usp_load_k3_allocation_summary.outputV2._finalize import finalize_summary

KNOWN_GROUPS = frozenset({"independent_early_loads", "independent_inputs"})

logger = get_logger(__name__)

_OUTPUT_TABLE = "K3AllocationSummary"


def _start_dev_plan_profile(enabled):
    from .plan_profiler import disable_plan_profile

    on = bool(enabled) and str(enabled).strip().lower() not in {
        "0", "false", "off", "no", "",
    }
    print(f"[plan-profile] armed={on}")
    if not on:
        disable_plan_profile()
        return None
    from .plan_profiler import (
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
    )
    return (
        start_plan_profile(),
        start_checkpoint_plan_profile(),
        start_action_profile(),
    )


def _finish_dev_plan_profile(tokens, threshold=30, cfg=None):
    from .plan_profiler import disable_plan_profile, store_last_plan_profile

    if not tokens:
        print("[plan-profile] skipped (not armed)")
        disable_plan_profile()
        return
    from .plan_profiler import (
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        plan_profile_report,
    )

    def _pair(item):
        if isinstance(item, tuple) and len(item) >= 2:
            return item[0], list(item[1] or [])
        return item, []

    plan_token, cp_token, action_token = tokens
    plan_tok, builder = _pair(plan_token)
    cp_tok, checkpoints = _pair(cp_token)
    action_tok, actions = _pair(action_token)
    finish_action_profile(action_tok)
    finish_checkpoint_plan_profile(cp_tok)
    finish_plan_profile(plan_tok)
    if isinstance(cfg, dict):
        builder = builder + list(cfg.get("_plan_profile") or [])
        checkpoints = checkpoints + list(cfg.get("_checkpoint_plan_profile") or [])
        actions = actions + list(cfg.get("_action_plan_profile") or [])
    try:
        from AllocationV2.plan_profiler import LAST_PLAN_PROFILE as _shared_last

        if not builder:
            builder = list(_shared_last.get("builder") or [])
        if not checkpoints:
            checkpoints = list(_shared_last.get("checkpoint") or [])
        if not actions:
            actions = list(_shared_last.get("action") or [])
    except Exception:
        pass
    builder_rep = plan_profile_report(builder, threshold, label="BUILDER") or []
    ckpt_rep = plan_profile_report(checkpoints, threshold, label="CHECKPOINT") or []
    action_rep = plan_profile_report(actions, threshold, label="ACTION") or []
    store_last_plan_profile(builder_rep, ckpt_rep, action_rep)
    print(
        f"[plan-profile] stored builder={len(builder_rep)} "
        f"checkpoint={len(ckpt_rep)} action={len(action_rep)}"
    )
    disable_plan_profile()


def _blank(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _as_int(value):
    if _blank(value) is None:
        return None
    return int(value)


def _checkpoint_frame(spark, df, name, cfg):
    if df is None:
        return None
    if not hasattr(df, "columns"):
        return df
    from .plan_profiler import track_checkpoint_plan

    track_checkpoint_plan(name, df, cfg)
    return common_checkpoint(spark, df, name, cfg)


def _apply_execution_profile(
    spark,
    cfg,
    profile_name,
    profile,
    workers,
    checkpoint_mode,
    CheckpointMode,
    SqlShufflePartitions,
    profile_plan=False,
):
    shuffle_override = _as_int(SqlShufflePartitions)
    if shuffle_override is None:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(profile["shuffle_partitions"]),
        )
    else:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            str(shuffle_override),
        )
    mode = profile["checkpoint_mode"]
    cfg.update(
        {
            "checkpoint_mode": mode,
            "max_threads": workers,
            "execution_profile": profile_name,
        }
    )
    cfg.setdefault("_checkpoint_tables", [])
    cfg["profile_plan"] = bool(profile_plan)
    _plan_tokens = _start_dev_plan_profile(profile_plan)
    print(
        f"[uspLoadK3AllocationSummary] ExecutionProfile={profile_name} "
        f"CheckpointMode={mode} shuffle="
        f"{shuffle_override or profile['shuffle_partitions']} "
        f"MaxThreads={workers}"
    )
    return mode


# ---------------------------------------------------------------------------
# S1: SP-local config bootstrap  (SQL 44-83, 120-123)
# ---------------------------------------------------------------------------

def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Resolve @IncomeAttrImportTransID (udfGetLatestTransactionID inlined).

    udfGetLatestTransactionID(@ClientID,@TaxPeriodID,IncludeFailed=0,
                              @IncomeAttrImportEventID,@EntityID):
      'Import_InvestmentIncomeAttributeRounding' is not in the no-entity event
      list, so @UseEntityID=1 and IncludeFailed=0:
        SELECT MAX(TransactionID) FROM TransactionLog
        WHERE ClientID=@C AND EventTypeID=@E AND TaxPeriodID=@TP
          AND StatusID NOT IN (Rejected, Err_Critical, Err_NonCritical)
          AND EntityID=@EntityID AND PhaseID=@PhaseID
      (LEFT JOIN VW_Entity is a no-op and is dropped — see logic_review.)
    """
    logger.info("[S1] load_sp_config")
    t0 = time.time()

    event_id = cfg.get("event_type_id_import_investment_income_attribute_rounding")
    phase_id = cfg.get("phase_id")
    excluded = cfg.get("workflow_status_ids_excluded") or []
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    trans_id = None
    if event_id is not None:
        tl = read_table(spark, "TransactionLog", cfg).select(
            "TransactionID", "ClientID", "EventTypeID", "TaxPeriodID",
            "EntityID", "PhaseID", "StatusID",
        ).filter(
            (F.col("ClientID") == client_id)
            & (F.col("EventTypeID") == event_id)
            & (F.col("TaxPeriodID") == tax_period_id)
            & (F.col("EntityID") == entity_id)
            & (F.col("PhaseID") == F.lit(phase_id))
        )
        if excluded:
            tl = tl.filter(~F.col("StatusID").isin(excluded))
        row = tl.select(F.max("TransactionID").alias("m")).first()
        trans_id = row["m"] if row else None

    cfg["income_attr_import_trans_id"] = trans_id
    log_timing("load_sp_config", t0, logger)
    return cfg


# ---------------------------------------------------------------------------
# S11: build output projection + save
# ---------------------------------------------------------------------------

def _build_output(cfg: dict, k3_summary):
    """Project the summary to the K3AllocationSummary table schema (SQL 543-544).

    ShareClass/PeriodID are not populated by the SP -> emitted NULL.
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    return k3_summary.select(
        F.col("RunID").cast("long").alias("RunID"),
        F.lit(client_id).cast("long").alias("ClientID"),
        F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        F.col("EntityID").cast("int").alias("EntityID"),
        F.col("LineID").cast("int").alias("LineID"),
        F.substring(F.col("PartnerNumber").cast("string"), 1, 50).alias("PartnerNumber"),
        F.col("Amount").cast("double").alias("Amount"),
        F.lit(None).cast("string").alias("ShareClass"),
        F.lit(None).cast("int").alias("PeriodID"),
        F.col("CountryID").cast("int").alias("CountryID"),
        F.col("AttributeTypeID").cast("int").alias("AttributeTypeID"),
        F.col("AttributeID").cast("int").alias("AttributeID"),
    )


def _save_results(spark: SparkSession, cfg: dict, output_tables: dict) -> str:
    """Write K3AllocationSummary via GenericResultStorer (result_type defaults to 'Parquet').

    Returns the GenericResultStorer output — the ParquetToSQL FilePathInfo JSON in
    Parquet mode. Mirrors the other parquet SPs (e.g. uspApplyInvestmentLevelRounding).
    """
    t0 = time.time()
    if not output_tables:
        logger.info("[SAVE] No output tables to save")
        log_timing("_save_results", t0, logger)
        return ""

    storer = GenericResultStorer(spark)
    return_value = storer.save_results(
        result=output_tables,
        result_type=cfg.get("result_type", "Parquet"),
        catalog_name=cfg["catalog"],
        database_name=cfg["schema"],
        run_id=cfg["run_id"],
        client_id=cfg["client_id"],
        entity_id=cfg["entity_id"],
        execution_id=cfg.get("execution_id", ""),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path="",
        sql_username="",
        sql_password="",
    )
    log_timing("_save_results", t0, logger)
    logger.info(f"[SAVE] Results saved: {list(output_tables.keys())}")
    return return_value


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_usp_load_k3_allocation_summary(
    spark: SparkSession,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    CallFrom: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    cfg: dict = None,
    verbose: bool = False,
    max_threads: int = None,
    MaxThreads: int = None,
    parallel_groups: str = "all",
    ParallelGroups: str = None,
    execution_profile: str = None,
    ExecutionProfile: str = None,
    checkpoint_mode: int = None,
    CheckpointMode: int = None,
    SqlShufflePartitions=None,
    profile_plan: bool = False,
    plan_checkpoint_threshold: int = 30,
    **kwargs,
) -> dict:
    """Execute the full SP conversion pipeline."""
    profile_plan = bool(
        profile_plan or kwargs.get("ProfilePlan") or kwargs.get("profile_plan")
    )
    del kwargs
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog_name = CatalogName
    schema_name = SchemaName
    call_from = CallFrom
    result_type = ResultType
    volume_path = VolumePath
    execution_id = ExecutionID
    t0 = time.time()
    parallel_activity = []
    enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups, known_groups=KNOWN_GROUPS)
    profile = resolve_execution_profile(
        _blank(ExecutionProfile) or _blank(execution_profile),
        checkpoint_mode=(
            CheckpointMode if CheckpointMode is not None else checkpoint_mode
        ),
    )
    profile_name = profile["name"]
    workers = normalize_workers(
        max_threads=(
            MaxThreads
            if MaxThreads is not None
            else max_threads
            if max_threads is not None
            else profile["max_threads"]
        ),
        MaxThreads=MaxThreads,
    )
    mode = None
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    status = {
        "sp_name": "uspLoadK3AllocationSummary",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "FilePathInfo": "",
        "skip_reason": None,
    }
    save_result = ""

    original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")

    try:
        if cfg is None:
            cfg = load_common_config(
                spark,
                entity_id=entity_id,
                client_id=client_id,
                tax_period_id=tax_period_id,
                run_id=run_id,
                catalog=catalog_name,
                schema=schema_name,
                call_from=call_from,
            )
        elif call_from is not None:
            cfg["call_from"] = call_from
        cfg = {
            **cfg,
            "_checkpoint_tables": list(cfg.get("_checkpoint_tables") or []),
        }
        cfg.setdefault("result_type", result_type)
        cfg["verbose"] = verbose
        if volume_path is not None:
            cfg["volume_path"] = volume_path
        if execution_id is not None:
            cfg["execution_id"] = execution_id
        mode = _apply_execution_profile(
            spark,
            cfg,
            profile_name,
            profile,
            workers,
            checkpoint_mode,
            CheckpointMode,
            SqlShufflePartitions,
            profile_plan,
        )
        status["run_id"] = cfg.get("run_id")
        status["entity_id"] = cfg.get("entity_id")
        if (cfg.get("run_status") or "").upper() == "FAIL":
            logger.warning("[EARLY_EXIT] run_status=FAIL — nothing to do.")
            status["skip_reason"] = "run_status_fail"
            return status
        cfg = load_sp_config(spark, cfg)

        holder = {}
        run_parallel(
            [
                (
                    "country_sic",
                    lambda: holder.__setitem__(
                        "country_sic",
                        build_country_sic_lines(spark, cfg),
                    ),
                ),
                (
                    "income_attr_import",
                    lambda: holder.__setitem__(
                        "income_attr_import",
                        build_income_attr_rounding_import(spark, cfg),
                    ),
                ),
            ],
            workers,
            parallel_activity,
            "independent_early_loads",
            enabled_groups,
        )
        run_parallel(
            [
                (
                    "k3_detail",
                    lambda: holder.__setitem__(
                        "k3_detail", build_k3_detail(spark, cfg)
                    ),
                ),
                (
                    "mapped_lines",
                    lambda: holder.__setitem__(
                        "mapped_lines", build_mapped_lines(spark, cfg)
                    ),
                ),
            ],
            workers,
            parallel_activity,
            "independent_inputs",
            enabled_groups,
        )
        country_sic = holder["country_sic"]
        income_attr_import = holder["income_attr_import"]
        k3_detail = holder["k3_detail"]
        mapped_lines = holder["mapped_lines"]

        rounding_flags = build_rounding_flags(
            spark, cfg, k3_detail, income_attr_import
        )
        has_mapped = has_mapped_lines(cfg, mapped_lines)
        k1_amounts = build_k1_summary_amounts(
            spark, cfg, country_sic, mapped_lines, has_mapped
        )
        s6 = build_k3_summary_rounded(
            spark,
            cfg,
            k3_detail,
            rounding_flags,
            mapped_lines,
            has_mapped,
            k1_amounts,
        )
        k3_summary = _checkpoint_frame(spark, s6["summary"], "k3_summary", cfg)
        temp6a = s6["temp6a"]
        temp6b = s6["temp6b"]
        rounding_diff = build_rounding_difference(
            spark, cfg, k3_summary, k1_amounts
        )
        is_country_level = (
            (cfg.get("flag_country_level_rounding_logic") or "")
            .strip()
            .upper()
            == "C"
        )
        if is_country_level:
            k3_detail = _checkpoint_frame(spark, k3_detail, "k3_detail", cfg)
            k1_amounts = _checkpoint_frame(spark, k1_amounts, "k1_amounts", cfg)
            rounding_diff = _checkpoint_frame(
                spark, rounding_diff, "rounding_diff", cfg
            )
            k3_summary = apply_country_level_rounding(
                spark,
                cfg,
                k3_summary,
                k3_detail,
                rounding_diff,
                k1_amounts,
                mapped_lines,
                has_mapped,
            )
        else:
            k3_summary = apply_standard_rounding(
                spark,
                cfg,
                k3_summary,
                rounding_diff,
                rounding_flags,
                mapped_lines,
                has_mapped,
                temp6a,
                temp6b,
            )
        k3_summary = finalize_summary(
            spark, cfg, k3_summary, rounding_flags, mapped_lines, has_mapped
        )
        output_tables = {_OUTPUT_TABLE: _build_output(cfg, k3_summary)}
        t_save = time.time()
        save_result = _save_results(spark, cfg, output_tables)
        log_timing("save_results", t_save, logger)
    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error("[FAIL] %s", e, exc_info=True)
        raise
    finally:

        try:
            _finish_dev_plan_profile(locals().get("_plan_tokens"), cfg=locals().get("cfg"))
        except Exception:
            pass
        spark.conf.set("spark.sql.shuffle.partitions", original_shuffle)
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        if isinstance(cfg, dict):
            drop_checkpoints(spark, cfg)

    logger.info(
        "[DONE] uspLoadK3AllocationSummary | %ss | RunID=%s EntityID=%s",
        status["elapsed_seconds"],
        cfg.get("run_id"),
        cfg.get("entity_id"),
    )
    return save_result if save_result else status


# ════════════════════════════════════════════════════════════════
# __main__: standalone execution (Mode 3).
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_usp_load_k3_allocation_summary(
            spark,
            RunID=int(dbutils.widgets.get("run_id")),               # noqa: F821
            EntityID=int(dbutils.widgets.get("entity_id")),         # noqa: F821
            ClientID=int(dbutils.widgets.get("client_id")),         # noqa: F821
            TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),  # noqa: F821
            CatalogName=dbutils.widgets.get("catalog"),             # noqa: F821
            SchemaName=dbutils.widgets.get("schema"),               # noqa: F821
        )
    except Exception as exc:
        raise RuntimeError(
            f"Usage: provide run_id, entity_id, client_id, tax_period_id, catalog, schema "
            f"as widget parameters ({exc})"
        )

    try:
        dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
    except Exception:
        print(f"Result: {result}")
