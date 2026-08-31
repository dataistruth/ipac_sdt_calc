"""
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

from .flowup_run_filter import read_local_run_table, read_lower_tier_flowup

logger = logging.getLogger(__name__)


def _load_pfic_foreign_corp_broadcast(spark: SparkSession, cfg: dict) -> DataFrame:
    key = "_pfic_foreign_corp_broadcast"
    if key not in cfg:
        cfg[key] = F.broadcast(
            read_local_run_table(spark, "PficForeignCorpClassificationInput", cfg)
        )
        _log("cached broadcast PficForeignCorpClassificationInput (RunID partition prune)")
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


def _cached_lower_tier_funds(spark: SparkSession, cfg: dict, run_id: int) -> DataFrame:
    key = f"_lower_tier_funds_df_{run_id}"
    if key not in cfg:
        cfg[key] = spark.table(f"_lower_tier_funds_{run_id}")
        _log(f"cached _lower_tier_funds_{run_id}")
    return cfg[key]


def _flowup_checkpoint(
    spark: SparkSession, df: DataFrame, cfg: dict, label: str,
) -> DataFrame:
    """Inner flowup break (post-reclass / post-zero) — Delta or local per cfg."""
    from .checkpoint import inner_base_flowup_checkpoint, should_checkpoint

    if not should_checkpoint(cfg, "base_flowup"):
        return df
    _log(f"flowup checkpoint ({label})")
    return inner_base_flowup_checkpoint(spark, df, cfg, label)


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


def _collect_result(cfg: dict, df: DataFrame, table_name: str) -> None:
    """Collect DataFrame for batch write via GenericResultStorer at end of SP."""

    spark = df.sparkSession

    _schema_cache = cfg.setdefault("_schema_cache", {})

    if table_name not in _schema_cache:

        try:

            _schema_cache[table_name] = {

                f.name: f.dataType for f in read_table(spark, table_name, cfg).schema

            }

        except Exception:

            _schema_cache[table_name] = {}

    target_types = _schema_cache[table_name]

    for col_name in df.columns:

        if col_name in target_types:

            df = df.withColumn(col_name, F.col(col_name).cast(target_types[col_name]))

    if "_parquet_results" not in cfg:

        cfg["_parquet_results"] = {}

    if table_name in cfg["_parquet_results"]:

        cfg["_parquet_results"][table_name] = cfg["_parquet_results"][table_name].unionByName(df, allowMissingColumns=True)

    else:

        cfg["_parquet_results"][table_name] = df


def _build_zero_fa_only_ids(spark, cfg, reclass_unblocked_df, pfic_line_item_df):

    """M3: #ZeroFootnoteAmountOnlyIds source set (before the non-zero anti-join).

    SQL builds this FROM #ReclassFootnoteAllocationUnblockedData at the investment line,

    non-allocated, joined to #LowerTierFunds and to PficForeignCorpClassificationInput on

    (PFIC.TextValue = PC.EntityID AND LT.EntityID = PC.SourceEntityID) plus the two

    NULL-tolerant CASE predicates on PFICFootnoteID and TrackingKey, where

    FootnoteClassification = 'Footnote-Amounts Only'. Returns (ZeroFootnoteID, ZeroTrackingKey).

    M3 Fix:

      1. Dropped the `PC.SourceEntityID == entity_id` pre-filter — SQL only constrains

         LT.EntityID = PC.SourceEntityID (via RU.LTEntityID); pinning it to entity_id

         over-narrowed and wrongly retained lower-tier-sourced footnotes.

      2. Carry PFICFootnoteID/TrackingKey and add the two SQL CASE join predicates so a

         classification row only matches reclass rows of the SAME footnote/tracking-key

         (matching on TextValue + SourceEntityID alone over-broadened).

    """

    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    run_id = cfg["run_id"]

    fa_class = (

        _load_pfic_foreign_corp_broadcast(spark, cfg)

        .filter(F.lower(F.col("FootnoteClassification")) == "footnote-amounts only")

        .select(

            F.col("EntityID").alias("PC_EntityID"),

            F.col("SourceEntityID").alias("PC_SourceEntityID"),

            F.col("PFICFootnoteID").alias("PC_PFICFootnoteID"),

            F.col("TrackingKey").alias("PC_TrackingKey"),

        ).distinct()

    )

    lower_tier = (
        read_local_run_table(spark, "LowerTierFunds", cfg)
        .select(F.col("EntityID").alias("LT_EntityID"))
        .distinct()
    )

    return (

        reclass_unblocked_df.alias("RU")

        .filter(F.col("RU.LineID") == pfic_investment_line_id)

        .join(

            pfic_line_item_df.alias("PF"),

            (F.col("PF.LineID") == F.col("RU.LineID"))

            & ((F.col("PF.IsAllocated") == False) | F.col("PF.IsAllocated").isNull()),

            "inner"

        )

        .join(

            lower_tier.alias("LT"),

            F.col("RU.LTEntityID") == F.col("LT.LT_EntityID"),

            "inner"

        )

        .join(

            fa_class.alias("PC"),

            (F.col("RU.TextValue") == F.col("PC.PC_EntityID").cast("string"))

            & (F.coalesce(F.col("RU.LTEntityID"), F.lit(0)) == F.coalesce(F.col("PC.PC_SourceEntityID"), F.lit(0)))

            # SQL: CASE WHEN PC.PficFootnoteID IS NULL THEN 1 ELSE PC.PficFootnoteID END

            #        = CASE WHEN PC.PficFootnoteID IS NULL THEN 1 ELSE PFIC.FootnoteID END

            & (F.when(F.col("PC.PC_PFICFootnoteID").isNull(), F.lit(1)).otherwise(F.col("PC.PC_PFICFootnoteID"))

               == F.when(F.col("PC.PC_PFICFootnoteID").isNull(), F.lit(1)).otherwise(F.col("RU.FootnoteID")))

            # SQL: CASE WHEN PC.TrackingKey IS NULL THEN '1' ELSE PC.TrackingKey END

            #        = CASE WHEN PC.TrackingKey IS NULL THEN '1' ELSE PFIC.TrackingKey END

            & (F.when(F.col("PC.PC_TrackingKey").isNull(), F.lit("1")).otherwise(F.col("PC.PC_TrackingKey"))

               == F.when(F.col("PC.PC_TrackingKey").isNull(), F.lit("1")).otherwise(F.col("RU.TrackingKey"))),

            "inner"

        )

        .select(

            F.col("RU.FootnoteID").alias("ZeroFootnoteID"),

            F.col("RU.TrackingKey").alias("ZeroTrackingKey"),

        ).distinct()

    )

def register_reclass_unblocked(
    spark: SparkSession,
    cfg: dict,
    pfic_foreign_corp: DataFrame | None = None,
) -> DataFrame:

    """Build #ReclassFootnoteAllocationData_UnblockedFootnote (the unblocked-PFIC

    allowlist) and register `_reclass_unblocked_{run_id}`. Idempotent — safe to call from

    phase 6 (so the Gate-1 PFIC→K-1 conversion can filter against it) and phase 7a.

    Single source of truth so the flowup pipeline and the conversion gates agree.

    SQL: SP ~L2315-2324.

    """

    run_id = cfg["run_id"]

    entity_id = cfg["entity_id"]

    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    pfic_line_type = cfg.get("pfic_footnote_line_type_id")

    reclass_data = spark.table("_reclass_data")

    if pfic_foreign_corp is None:
        pfic_foreign_corp = _load_pfic_foreign_corp_broadcast(spark, cfg)

    blocked = _blocked_classification(pfic_foreign_corp, entity_id)

    reclass_unblocked = reclass_data.alias("RFA") \
        .filter(

            (F.col("RFA.LineID") == pfic_investment_line_id)

            & (F.col("RFA.LineTypeID") == pfic_line_type)

        ) \
        .join(

            blocked.alias("PB"),

            (F.coalesce(F.col("PB.PFICSourceEntityID"), F.lit(0)) == F.coalesce(F.col("RFA.SourceEntityID"), F.lit(0)))

            & (F.col("RFA.TextValue") == F.col("PB.EntityID").cast("string"))

            & (F.col("RFA.FootnoteID") == F.coalesce(F.col("PB.PficFootnoteID"), F.col("RFA.FootnoteID")))

            & (F.when(F.col("PB.TrackingKey").isNull(), F.lit("1"))

                 .otherwise(F.col("PB.TrackingKey"))

               == F.when(F.col("PB.TrackingKey").isNull(), F.lit("1"))

                 .otherwise(F.concat(F.coalesce(F.col("RFA.TrackingKey"), F.lit("")),

                                     F.lit("~"), F.lit(str(entity_id))))),

            "left"

        ) \
        .filter(F.col("PB.EntityID").isNull()) \
        .select(

            F.col("RFA.LTEntityID"), F.col("RFA.SourceEntityID"), F.col("RFA.FootnoteID"),

            F.col("RFA.LineID"), F.col("RFA.LineTypeID"), F.col("RFA.TextValue"),

            F.col("RFA.ParentEntityID"), F.col("RFA.TrackingKey"),

        ).distinct()

    reclass_unblocked.createOrReplaceTempView(f"_reclass_unblocked_{run_id}")

    return reclass_unblocked

def _filter_reclass_to_unblocked(spark: SparkSession, cfg: dict, rfa_df: DataFrame, alias_name: str = "RFA"):

    """R2 (Round 5): restrict a ReclassFootnoteAllocationData frame to the unblocked

    allowlist — i.e. reproduce #ReclassFootnoteAllocationUnblockedData (SP L2350-2382):

    INNER JOIN #ReclassFootnoteAllocationData_UnblockedFootnote ON FootnoteID, TrackingKey,

    ISNULL(LTEntityID,0), ISNULL(SourceEntityID,0), ISNULL(ParentEntityID,0).

    The four PFIC→K-1 conversion gates read the blocked-filtered set, NOT raw _reclass_data.

    """

    run_id = cfg["run_id"]

    try:

        ub = spark.table(f"_reclass_unblocked_{run_id}")

    except Exception:

        ub = register_reclass_unblocked(spark, cfg)

    ub = ub.select(

        F.col("FootnoteID").alias("_ub_FootnoteID"),

        F.col("TrackingKey").alias("_ub_TrackingKey"),

        F.col("LTEntityID").alias("_ub_LTEntityID"),

        F.col("SourceEntityID").alias("_ub_SourceEntityID"),

        F.col("ParentEntityID").alias("_ub_ParentEntityID"),

    ).distinct()

    a = rfa_df.alias(alias_name)

    return a.join(

        ub,

        (F.col(f"{alias_name}.FootnoteID") == F.col("_ub_FootnoteID"))

        & (F.col(f"{alias_name}.TrackingKey") == F.col("_ub_TrackingKey"))

        & (F.coalesce(F.col(f"{alias_name}.LTEntityID"), F.lit(0)) == F.coalesce(F.col("_ub_LTEntityID"), F.lit(0)))

        & (F.coalesce(F.col(f"{alias_name}.SourceEntityID"), F.lit(0)) == F.coalesce(F.col("_ub_SourceEntityID"), F.lit(0)))

        & (F.coalesce(F.col(f"{alias_name}.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("_ub_ParentEntityID"), F.lit(0))),

        "left_semi"

    )

def build_pfic_flowup_pipeline(

    spark: SparkSession,

    cfg: dict,

    pfic_snapshot_df: DataFrame,

    pfic_elections: dict,

    lower_tier_df: DataFrame,

) -> DataFrame:

    """Build the final PFICFootnoteFlowup DataFrame.

    Pipeline:

    1. Base PFIC flowup from snapshot (amounts per lower-tier entity)

    2. Apply auto election D (convert 1291 to deemed sale if enabled)

    3. Add lookthrough reclass adjustments

    4. Apply distribution line exclusion

    5. Filter by 1293 exclusion logic

    SQL lines: 5700-6900

    Returns: DataFrame[RunID, ClientID, TaxPeriodID, EntityID, FlowupEntityID,

             SourceEntityID, PFICFootnoteID, LineID, Amount, TextValue, TrackingKey]

    """

    log_section("build_pfic_flowup_pipeline")
    _log("build_pfic_flowup_pipeline START")
    t0 = time.time()

    prefix = table_prefix(cfg)

    entity_id = cfg["entity_id"]

    client_id = cfg["client_id"]

    tax_period_id = cfg["tax_period_id"]

    run_id = cfg["run_id"]

    fx_tid = cfg.get("fx_rate_transaction_id") or 0

    is_tracking = cfg.get("is_tracking_key", "C") == "C"

    pfic_line_type = cfg.get("pfic_footnote_line_type_id")

    is_foreign = cfg.get("is_foreign_entity", False)

    is_auto_elec_d = cfg.get("is_auto_elec_d_enabled", False)

    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    type_of_pfic_line = cfg.get("type_of_pfic_line_id")

    # Broadcast small shared views (used many times in Step 1–6).
    pfic_line_item = F.broadcast(spark.table("_pfic_line_item"))
    entity_tv = F.broadcast(spark.table("_entity"))
    fx_avg_rate = F.broadcast(spark.table("_fx_avg_rate"))
    lower_tier_funds = _cached_lower_tier_funds(spark, cfg, run_id)
    _log("broadcast shared views for flowup build")

    # Placeholder for the lookthrough-reclass unblocked set. The real data is

    # registered at Step 2 only when reclass_wf_id > 0; the zero-amount block

    # below (`if True`) reads this view unconditionally via spark.table(). In

    # Spark Connect spark.table() resolves lazily, so a try/except around the

    # read cannot catch a missing-view error (it surfaces at action time).

    # Register an empty view up front so the read always resolves; Step 2

    # overwrites it with createOrReplaceTempView when reclass data exists.

    spark.createDataFrame(

        [],

        "LTEntityID int, SourceEntityID int, FootnoteID int, LineID int, "

        "LineTypeID int, TextValue string, ParentEntityID int, TrackingKey string",

    ).createOrReplaceTempView(f"_reclass_unblocked_{run_id}")

    # ─── Step 1: Base PFIC Flowup ─────────────────────────────────────────

    # Direct PFIC flowup from snapshot (converted amounts)

    # FX bypass: percentage/share ShortNames are NOT converted

    pct_share_names = ['OwnershipPercentage', 'NumberofSharesBeginningofYear',

                       'NumberofSharesEndofYear', 'Part_5_G', 'CFCPartnershipownership']

    pfic_alloc = pfic_snapshot_df.alias("PFIC") \
        .join(

            pfic_line_item.alias("PL"),

            (F.col("PFIC.LineID") == F.col("PL.LineID"))

            & (F.col("PL.IsAllocated") == True),

            "inner"

        ) \
        .join(entity_tv.alias("E"), F.col("E.EntityID") == F.col("PFIC.SourceEntityID"), "inner") \
        .join(fx_avg_rate.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left") \
        .select(

            F.lit(run_id).alias("RunID"),

            F.lit(client_id).alias("ClientID"),

            F.lit(tax_period_id).alias("TaxPeriodID"),

            F.lit(entity_id).alias("EntityID"),

            F.lit(entity_id).alias("FlowupEntityID"),

            F.col("PFIC.SourceEntityID"),

            F.col("PFIC.PFICFootnoteID"),

            F.col("PFIC.LineID"),

            F.when(F.lower(F.col("PL.ShortName")).isin([s.lower() for s in pct_share_names]), F.col("PFIC.Amount"))

             .otherwise(F.round(F.col("PFIC.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))

             .alias("Amount"),

            F.col("PFIC.TextValue"),

            F.col("PFIC.SourceEntityID").cast("string").alias("TrackingKey"),

        )

    # Non-allocated lines (text/percentage lines, no rounding applied)

    pfic_non_alloc = pfic_snapshot_df.alias("PFIC") \
        .join(

            pfic_line_item.alias("PL"),

            (F.col("PFIC.LineID") == F.col("PL.LineID"))

            & ((F.col("PL.IsAllocated") == False) | F.col("PL.IsAllocated").isNull()),

            "inner"

        ) \
        .join(entity_tv.alias("E"), F.col("E.EntityID") == F.col("PFIC.SourceEntityID"), "inner") \
        .join(fx_avg_rate.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left") \
        .select(

            F.lit(run_id).alias("RunID"),

            F.lit(client_id).alias("ClientID"),

            F.lit(tax_period_id).alias("TaxPeriodID"),

            F.lit(entity_id).alias("EntityID"),

            F.lit(entity_id).alias("FlowupEntityID"),

            F.col("PFIC.SourceEntityID"),

            F.col("PFIC.PFICFootnoteID"),

            F.col("PFIC.LineID"),

            F.when(F.lower(F.col("PL.ShortName")).isin([s.lower() for s in pct_share_names]), F.col("PFIC.Amount"))

             .otherwise(F.col("PFIC.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)))

             .alias("Amount"),

            F.col("PFIC.TextValue"),

            F.col("PFIC.SourceEntityID").cast("string").alias("TrackingKey"),

        )

    base_flowup = pfic_alloc.unionByName(pfic_non_alloc)

    reclass_wf_id = cfg.get("lookthrough_reclass_workflow_id", 0)

    # ─── Step 2: Lookthrough Reclass Flowup ───────────────────────────────

    if reclass_wf_id > 0:
        _log(f"Step 2 lookthrough reclass workflow_id={reclass_wf_id}")
        reclass_data = spark.table("_reclass_data")
        pfic_foreign_corp = _load_pfic_foreign_corp_broadcast(spark, cfg)
        reclass_unblocked = register_reclass_unblocked(spark, cfg, pfic_foreign_corp=pfic_foreign_corp)

        # Allocated lines from unblocked reclass data

        reclass_flowup_alloc = reclass_data.alias("RFA") \
            .join(

                reclass_unblocked.alias("UB"),

                (F.col("RFA.FootnoteID") == F.col("UB.FootnoteID"))

                & (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.coalesce(F.col("UB.TrackingKey"), F.lit("")))

                & (F.coalesce(F.col("RFA.LTEntityID"), F.lit(0)) == F.coalesce(F.col("UB.LTEntityID"), F.lit(0)))

                & (F.coalesce(F.col("RFA.SourceEntityID"), F.lit(0)) == F.coalesce(F.col("UB.SourceEntityID"), F.lit(0)))

                & (F.coalesce(F.col("RFA.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("UB.ParentEntityID"), F.lit(0))),

                "left_semi"

            ) \
            .join(

                pfic_line_item.alias("PF"),

                (F.col("PF.LineID") == F.col("RFA.LineID")) & (F.col("PF.IsAllocated") == True),

                "inner"

            ) \
            .filter(F.col("RFA.LineTypeID") == pfic_line_type) \
            .groupBy(

                F.col("RFA.LTEntityID"), F.col("RFA.SourceEntityID"), F.col("RFA.FootnoteID"),

                F.col("RFA.LineID"), F.col("RFA.TextValue"), F.coalesce(F.col("RFA.TrackingKey"), F.lit("")).alias("TrackingKey_grp"),

            ).agg(F.sum("RFA.FlowupAmount").alias("Amount")) \
            .select(

                F.lit(run_id).alias("RunID"),

                F.lit(client_id).alias("ClientID"),

                F.lit(tax_period_id).alias("TaxPeriodID"),

                F.lit(entity_id).alias("EntityID"),

                F.col("LTEntityID").alias("FlowupEntityID"),

                F.col("SourceEntityID"),

                F.col("FootnoteID").alias("PFICFootnoteID"),

                F.col("LineID"),

                F.col("Amount"),

                F.col("TextValue"),

                F.col("TrackingKey_grp").alias("TrackingKey"),

            )

        base_flowup = base_flowup.unionByName(reclass_flowup_alloc)

        # Non-allocated lines from unblocked reclass data

        reclass_flowup_non_alloc = reclass_data.alias("RFA") \
            .join(

                reclass_unblocked.alias("UB"),

                (F.col("RFA.FootnoteID") == F.col("UB.FootnoteID"))

                & (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.coalesce(F.col("UB.TrackingKey"), F.lit("")))

                & (F.coalesce(F.col("RFA.LTEntityID"), F.lit(0)) == F.coalesce(F.col("UB.LTEntityID"), F.lit(0)))

                & (F.coalesce(F.col("RFA.SourceEntityID"), F.lit(0)) == F.coalesce(F.col("UB.SourceEntityID"), F.lit(0)))

                & (F.coalesce(F.col("RFA.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("UB.ParentEntityID"), F.lit(0))),

                "left_semi"

            ) \
            .join(

                pfic_line_item.alias("PF"),

                (F.col("PF.LineID") == F.col("RFA.LineID"))

                & ((F.col("PF.IsAllocated") == False) | F.col("PF.IsAllocated").isNull()),

                "inner"

            ) \
            .filter(F.col("RFA.LineTypeID") == pfic_line_type) \
            .groupBy(

                F.col("RFA.LTEntityID"), F.col("RFA.SourceEntityID"), F.col("RFA.FootnoteID"),

                F.col("RFA.LineID"), F.col("RFA.TextValue"), F.coalesce(F.col("RFA.TrackingKey"), F.lit("")).alias("TrackingKey_grp"),

            ).agg(F.max("RFA.FlowupAmount").alias("Amount")) \
            .select(

                F.lit(run_id).alias("RunID"),

                F.lit(client_id).alias("ClientID"),

                F.lit(tax_period_id).alias("TaxPeriodID"),

                F.lit(entity_id).alias("EntityID"),

                F.col("LTEntityID").alias("FlowupEntityID"),

                F.col("SourceEntityID"),

                F.col("FootnoteID").alias("PFICFootnoteID"),

                F.col("LineID"),

                F.col("Amount"),

                F.col("TextValue"),

                F.col("TrackingKey_grp").alias("TrackingKey"),

            )

        _nonalloc_non_zero = (

            base_flowup.alias("BF")

            .join(pfic_line_item.alias("FL"), F.col("BF.LineID") == F.col("FL.LineID"), "inner")

            .filter(

                (F.col("BF.Amount") != 0)

                & (F.col("FL.IsActive") == True)

                & (F.col("FL.IsAllocated") == True)

                & (F.col("BF.RunID") == run_id)

            )

            .select(F.col("BF.PFICFootnoteID"), F.col("BF.TrackingKey")).distinct()

        )

        _fa_only = _cached_zero_fa_only_ids(spark, cfg, reclass_unblocked, pfic_line_item).select(

            F.col("ZeroFootnoteID"), F.col("ZeroTrackingKey")

        )

        _zero_fa_only_ids = (

            _fa_only.alias("FA")

            .join(

                _nonalloc_non_zero.alias("NZ"),

                (F.col("FA.ZeroFootnoteID") == F.col("NZ.PFICFootnoteID"))

                & (F.coalesce(F.col("FA.ZeroTrackingKey"), F.lit("")) == F.coalesce(F.col("NZ.TrackingKey"), F.lit(""))),

                "left_anti"

            )

        )

        reclass_flowup_non_alloc = (

            reclass_flowup_non_alloc.alias("NA")

            .join(

                _zero_fa_only_ids.alias("ZFAO"),

                (F.col("NA.PFICFootnoteID") == F.col("ZFAO.ZeroFootnoteID"))

                & (F.coalesce(F.col("NA.TrackingKey"), F.lit("")) == F.coalesce(F.col("ZFAO.ZeroTrackingKey"), F.lit(""))),

                "left_anti"

            )

        )

        base_flowup = base_flowup.unionByName(reclass_flowup_non_alloc)

        base_flowup = _flowup_checkpoint(spark, base_flowup, cfg, "post-reclass")

    # ─── Step 3: Zero-Amount PFICs ────────────────────────────────────────

    # PFICs from PFICFootnoteFlowupWithTrackingKey that have no amounts in current flowup

    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    # Existing non-zero PFIC IDs

    non_zero_pfics = base_flowup.alias("BF") \
        .join(pfic_line_item.alias("FL"), F.col("BF.LineID") == F.col("FL.LineID"), "inner") \
        .filter(

            (F.col("BF.Amount") != 0)

            & (F.col("FL.IsActive") == True)

            & (F.col("FL.IsAllocated") == True)

            & (F.col("BF.RunID") == run_id)

        ) \
        .select(F.col("BF.PFICFootnoteID"), F.col("BF.TrackingKey")).distinct()

    # Existing PFIC footnotes in current flowup

    existing_pfic_footnotes = base_flowup.filter(F.col("RunID") == run_id) \
        .select("PFICFootnoteID", "FlowupEntityID", "TrackingKey").distinct()

    # Zero amount PFICs from prior flowup (partitioned on RunID — prune to lower-tier LTRunIDs)
    pfic_flowup_tracking = read_lower_tier_flowup(
        spark, "PFICFootnoteFlowupWithTrackingKey", cfg
    )

    enu_tax_class = F.broadcast(read_table(spark, "ENU_TaxClass", cfg))

    zero_amount_pfics = pfic_flowup_tracking.alias("PFIC") \
        .join(

            lower_tier_funds.alias("LT"),

            F.col("PFIC.RunID") == F.col("LT.RunID"),

            "inner"

        ) \
        .join(entity_tv.alias("E"), F.col("LT.EntityID") == F.col("E.EntityID"), "inner") \
        .join(enu_tax_class.alias("TC"), F.col("E.TaxClassID") == F.col("TC.TaxClassID"), "left") \
        .join(

            existing_pfic_footnotes.alias("F"),

            (F.col("F.PFICFootnoteID") == F.col("PFIC.PFICFootnoteID"))

            & (F.col("F.TrackingKey") == F.col("PFIC.TrackingKey"))

            & (F.coalesce(F.col("LT.EntityID"), F.lit(0)) == F.coalesce(F.col("F.FlowupEntityID"), F.lit(0))),

            "left"

        ) \
        .filter(

            F.col("F.TrackingKey").isNull()

        ) \
        .groupBy(

            F.col("PFIC.SourceEntityID"), F.col("PFIC.PFICFootnoteID"), F.col("PFIC.LineID"),

            F.col("LT.EntityID").alias("LTEntityID"),

            F.when(

                (F.coalesce(F.col("E.IsForeign"), F.lit(False)) == False)

                & (F.lower(F.coalesce(F.col("TC.TaxClassName"), F.lit(""))) == "disregarded entity"),

                F.lit(True)

            ).otherwise(F.coalesce(F.col("E.IsForeign"), F.lit(False))).alias("IsForeign"),

            F.col("PFIC.TrackingKey"),

        ).agg(

            F.lit(0).alias("Amount"),

            F.max("PFIC.TextValue").alias("TextValue"),

        ) \
        .select(

            "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "TextValue",

            "LTEntityID", "IsForeign", "TrackingKey",

        )

    if True:  # zero-amount processing is always applied (empty DF = no-op)

        try:

            _ru_view = spark.table(f"_reclass_unblocked_{run_id}")

            footnote_amounts_only_ids = _cached_zero_fa_only_ids(spark, cfg, _ru_view, pfic_line_item)

        except Exception:

            footnote_amounts_only_ids = spark.createDataFrame(

                [], "ZeroFootnoteID int, ZeroTrackingKey string"

            )

        # Only keep those that are NOT in non_zero_pfics (truly zero-only)

        zero_footnote_amount_only_ids = (

            footnote_amounts_only_ids.alias("FA")

            .join(

                non_zero_pfics.alias("NZ"),

                (F.col("FA.ZeroFootnoteID") == F.col("NZ.PFICFootnoteID")) &

                (F.coalesce(F.col("FA.ZeroTrackingKey"), F.lit("")) == F.coalesce(F.col("NZ.TrackingKey"), F.lit(""))),

                "left_anti"

            )

        )

        # Anti-join: exclude ZeroFootnoteAmountOnlyIds from zero_amount_pfics (construction-time)

        zero_amount_pfics = (

            zero_amount_pfics.alias("ZAP")

            .join(

                zero_footnote_amount_only_ids.alias("ZFAO"),

                (F.col("ZAP.PFICFootnoteID") == F.col("ZFAO.ZeroFootnoteID")) &

                (F.coalesce(F.col("ZAP.TrackingKey"), F.lit("")) == F.coalesce(F.col("ZFAO.ZeroTrackingKey"), F.lit(""))),

                "left_anti"

            )

        )

        domestic_blocker_ids = (

            entity_tv.alias("E")

            .join(lower_tier_funds.alias("LT"), F.col("LT.EntityID") == F.col("E.EntityID"), "inner")

            .filter(F.col("E.IsDomesticBlocker") == True)

            .select(F.col("E.EntityID").alias("DB_EntityID"))

            .distinct()

        )

        has_domestic_blockers = domestic_blocker_ids.limit(1).first() is not None

        if has_domestic_blockers:

            # PFICs flowing through domestic blocker (to be deleted)

            del_set = zero_amount_pfics.alias("Z2") \
                .join(

                    domestic_blocker_ids.alias("DB"),

                    F.col("DB.DB_EntityID") == F.col("Z2.LTEntityID"),

                    "inner"

                ) \
                .filter(F.col("Z2.SourceEntityID") != F.col("DB.DB_EntityID")) \
                .select(

                    F.col("Z2.PFICFootnoteID"), F.col("Z2.LTEntityID"), F.col("Z2.TrackingKey")

                ).distinct()

            # Non-delete set (PFICs sourced from domestic blocker with specific text values)

            keep_set = zero_amount_pfics.alias("Z3") \
                .join(

                    domestic_blocker_ids.alias("DB2"),

                    F.col("DB2.DB_EntityID") == F.col("Z3.SourceEntityID"),

                    "inner"

                ) \
                .filter(

                    (F.col("Z3.LineID") == type_of_pfic_line)

                    & F.lower(F.col("Z3.TextValue")).isin("is1293eligibledeemed", "is1291anydistribution")

                ) \
                .select(F.col("Z3.PFICFootnoteID")).distinct()

            zero_amount_pfics = zero_amount_pfics.alias("Z") \
                .join(

                    del_set.alias("DEL"),

                    (F.col("Z.PFICFootnoteID") == F.col("DEL.PFICFootnoteID"))

                    & (F.coalesce(F.col("Z.TrackingKey"), F.lit("")) == F.coalesce(F.col("DEL.TrackingKey"), F.lit("")))

                    & (F.coalesce(F.col("Z.LTEntityID"), F.lit(0)) == F.coalesce(F.col("DEL.LTEntityID"), F.lit(0))),

                    "left"

                ) \
                .join(

                    keep_set.alias("KEEP"),

                    F.col("Z.PFICFootnoteID") == F.col("KEEP.PFICFootnoteID"),

                    "left"

                ) \
                .filter(

                    F.col("DEL.PFICFootnoteID").isNull() | F.col("KEEP.PFICFootnoteID").isNotNull()

                ) \
                .select(

                    F.col("Z.SourceEntityID"), F.col("Z.PFICFootnoteID"), F.col("Z.LineID"),

                    F.col("Z.Amount"), F.col("Z.TextValue"), F.col("Z.LTEntityID"),

                    F.col("Z.IsForeign"), F.col("Z.TrackingKey"),

                )

        # Update PFICOwnership to 'Indirect' and PFICStatus to '' for zero-amount PFICs

        pfic_ownership_line = cfg.get("pfic_ownership_line_id")

        pfic_pstatus_line = cfg.get("pfic_pstatus_line_id")

        type_of_foreign_corp_line = cfg.get("type_of_foreign_corp_line_id")

        part_vii_indicator = cfg.get("part_vii_indicator", 0)

        zero_amount_pfics = zero_amount_pfics.withColumn(

            "TextValue",

            F.when(

                (F.col("LineID") == pfic_ownership_line) & (F.coalesce(F.col("TextValue"), F.lit("")) != ""),

                F.lit("Indirect")

            ).when(

                F.col("LineID") == pfic_pstatus_line,

                F.lit("")

            ).otherwise(F.col("TextValue"))

        )

        # Build non-delete set for domestic zero PFICs

        non_delete_cond_1 = zero_amount_pfics.filter(

            (F.col("LineID") == type_of_pfic_line)

            & (

                F.lower(F.coalesce(F.col("TextValue"), F.lit(""))).isin("is1291anydistribution", "is1293eligibledeemed")

                | (

                    (F.lit(part_vii_indicator) == 1)

                    & (F.lower(F.col("TextValue")) == "is1291nodistribution")

                )

            )

        ).select("SourceEntityID", "PFICFootnoteID", "LTEntityID", "TrackingKey").distinct()

        non_delete_cond_2 = zero_amount_pfics.filter(

            (F.col("LineID") == type_of_foreign_corp_line)

            & F.lower(F.coalesce(F.col("TextValue"), F.lit(""))).isin("foreign corporation", "controlled foreign corporation")

            & (F.coalesce(F.col("IsForeign"), F.lit(False)) == False)

        ).select("SourceEntityID", "PFICFootnoteID", "LTEntityID", "TrackingKey").distinct()

        non_delete_domestic = non_delete_cond_1.unionByName(non_delete_cond_2).distinct()

        # Keep only rows that are either in non-delete set OR are foreign

        zero_amount_pfics = zero_amount_pfics.alias("Z") \
            .join(

                non_delete_domestic.alias("DF"),

                (F.col("Z.PFICFootnoteID") == F.col("DF.PFICFootnoteID"))

                & (F.col("Z.TrackingKey") == F.col("DF.TrackingKey"))

                & (F.col("Z.SourceEntityID") == F.col("DF.SourceEntityID"))

                & (F.coalesce(F.col("Z.LTEntityID"), F.lit(0)) == F.coalesce(F.col("DF.LTEntityID"), F.lit(0))),

                "left"

            ) \
            .filter(

                F.col("DF.PFICFootnoteID").isNotNull() | (F.coalesce(F.col("Z.IsForeign"), F.lit(False)) == True)

            ) \
            .select(

                F.col("Z.SourceEntityID"), F.col("Z.PFICFootnoteID"), F.col("Z.LineID"),

                F.col("Z.Amount"), F.col("Z.TextValue"), F.col("Z.LTEntityID"),

                F.col("Z.IsForeign"), F.col("Z.TrackingKey"),

            )

        pfic_foreign_corp_za = _load_pfic_foreign_corp_broadcast(spark, cfg)

        pfic_investment_line_id = cfg.get("pfic_investment_line_id")

        pfic_blocked_for_zero = (

            pfic_foreign_corp_za

            .filter(

                (F.lower(F.col("FootnoteClassification")) == "blocked") &

                (F.col("SourceEntityID") == entity_id)

            )

            .select(

                F.col("EntityID").alias("BlockedEntityID"),

                F.col("PFICSourceEntityID").alias("BlockedSourceEntityID"),

                F.col("PficFootnoteID").alias("BlockedFootnoteID"),

                F.col("TrackingKey").alias("BlockedTrackingKey"),

            )

            .distinct()

        )

        # Unblocked footnote keys: investment-line zero rows that are NOT blocked

        unblocked_zero_keys = (

            zero_amount_pfics.alias("ZINV")

            .filter(F.col("ZINV.LineID") == pfic_investment_line_id)

            .join(

                pfic_blocked_for_zero.alias("BLK"),

                (F.coalesce(F.col("BLK.BlockedSourceEntityID"), F.lit(0)) ==

                 F.coalesce(F.col("ZINV.SourceEntityID"), F.lit(0))) &

                (F.col("ZINV.TextValue") == F.col("BLK.BlockedEntityID").cast("string")) &

                (F.when(F.col("BLK.BlockedFootnoteID").isNull(), F.lit(1))

                  .otherwise(F.col("ZINV.PFICFootnoteID")) ==

                 F.when(F.col("BLK.BlockedFootnoteID").isNull(), F.lit(1))

                  .otherwise(F.col("BLK.BlockedFootnoteID"))) &

                (F.when(F.col("BLK.BlockedTrackingKey").isNull(), F.lit("1"))

                  .otherwise(F.concat(F.coalesce(F.col("ZINV.TrackingKey"), F.lit("")),

                                      F.lit("~"), F.lit(str(entity_id)))) ==

                 F.when(F.col("BLK.BlockedTrackingKey").isNull(), F.lit("1"))

                  .otherwise(F.col("BLK.BlockedTrackingKey"))),

                "left_anti"

            )

            .select(

                F.col("ZINV.SourceEntityID").alias("u_SourceEntityID"),

                F.col("ZINV.PFICFootnoteID").alias("u_PFICFootnoteID"),

                F.col("ZINV.LTEntityID").alias("u_LTEntityID"),

                F.col("ZINV.TrackingKey").alias("u_TrackingKey"),

            )

            .distinct()

        )

        zero_amount_pfics = (

            zero_amount_pfics.alias("ZAP2")

            .join(

                unblocked_zero_keys.alias("UK"),

                (F.col("ZAP2.SourceEntityID") == F.col("UK.u_SourceEntityID")) &

                (F.col("ZAP2.PFICFootnoteID") == F.col("UK.u_PFICFootnoteID")) &

                (F.coalesce(F.col("ZAP2.LTEntityID"), F.lit(0)) ==

                 F.coalesce(F.col("UK.u_LTEntityID"), F.lit(0))) &

                (F.col("ZAP2.TrackingKey") == F.col("UK.u_TrackingKey")),

                "left_semi"

            )

        )

        # Insert zero-amount PFICs into flowup

        zero_flowup = zero_amount_pfics.select(

            F.lit(run_id).alias("RunID"),

            F.lit(client_id).alias("ClientID"),

            F.lit(tax_period_id).alias("TaxPeriodID"),

            F.lit(entity_id).alias("EntityID"),

            F.col("LTEntityID").alias("FlowupEntityID"),

            F.col("SourceEntityID"),

            F.col("PFICFootnoteID"),

            F.col("LineID"),

            F.lit(0).alias("Amount"),

            F.col("TextValue"),

            F.col("TrackingKey"),

        )

        base_flowup = base_flowup.unionByName(zero_flowup)

        base_flowup = _flowup_checkpoint(spark, base_flowup, cfg, "post-zero")

        # SQL lines 2529-2551: @FlowZeroPFICs block

        flow_zero_pfics = cfg.get("flow_zero_pfics")

        if flow_zero_pfics == "C":  # J2: SQL @FlowZeroPFICs block (SP L2538) has no reclass-workflow guard

            reclass_unblocked_v = spark.table(f"_reclass_unblocked_{run_id}")

            pfic_foreign_corp_2 = _load_pfic_foreign_corp_broadcast(spark, cfg)

            flow_zero_extra = base_flowup.alias("PFIC") \
                .join(

                    lower_tier_funds.alias("LTF"),

                    (F.col("PFIC.RunID") == F.col("LTF.RunID"))

                    & (F.col("PFIC.EntityID") == F.col("LTF.EntityID")),

                    "inner"

                ) \
                .join(

                    reclass_unblocked_v.alias("F2"),

                    (F.col("PFIC.PFICFootnoteID") == F.col("F2.FootnoteID"))

                    & (F.coalesce(F.col("PFIC.TrackingKey"), F.lit("")) == F.coalesce(F.col("F2.TrackingKey"), F.lit("")))

                    & (F.coalesce(F.col("PFIC.SourceEntityID"), F.lit(0)) == F.coalesce(F.col("F2.SourceEntityID"), F.lit(0)))

                    & (F.coalesce(F.col("F2.LTEntityID"), F.lit(0)) == entity_id),

                    "inner"

                ) \
                .filter(F.col("PFIC.LineID") == pfic_investment_line_id) \
                .join(

                    pfic_foreign_corp_2.alias("PC"),

                    (F.col("PFIC.TextValue") == F.col("PC.EntityID").cast("string"))

                    & (F.col("PC.SourceEntityID") == entity_id)

                    & (F.lower(F.col("PC.FootnoteClassification")) == "footnote-amounts only")

                    & (F.coalesce(F.col("PC.PficFootnoteID"), F.col("PFIC.PFICFootnoteID")) == F.col("PFIC.PFICFootnoteID"))

                    & (F.coalesce(

                        F.col("PC.TrackingKey"),

                        F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))

                    ) == F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))),

                    "left_anti"

                ) \
                .join(

                    existing_pfic_footnotes.alias("P"),

                    (F.col("P.PFICFootnoteID") == F.col("PFIC.PFICFootnoteID"))

                    & (F.coalesce(F.col("P.TrackingKey"), F.lit("")) == F.coalesce(F.col("PFIC.TrackingKey"), F.lit(""))),

                    "left_anti"

                ) \
                .select(

                    F.lit(run_id).alias("RunID"),

                    F.lit(client_id).alias("ClientID"),

                    F.lit(tax_period_id).alias("TaxPeriodID"),

                    F.lit(entity_id).alias("EntityID"),

                    F.lit(entity_id).alias("FlowupEntityID"),

                    F.col("PFIC.SourceEntityID"),

                    F.col("PFIC.PFICFootnoteID"),

                    F.col("PFIC.LineID"),

                    F.lit(0).alias("Amount"),

                    F.col("PFIC.TextValue"),

                    F.col("PFIC.TrackingKey"),

                ).distinct()

            base_flowup = base_flowup.unionByName(flow_zero_extra)

    # ─── Step 4: Auto Election-D ──────────────────────────────────────────

    if is_auto_elec_d:

        election_d_line_id = cfg.get("election_d_line_id")

        type_of_pfic_line_id = cfg.get("type_of_pfic_line_id")

        is_1293_deemed_desc = cfg.get("is_1293_eligible_deemed_desc", "IS1293EligibleDeemed")

        if election_d_line_id and type_of_pfic_line_id:

            # Identify PFICs that have IS1293EligibleDeemed on the type-of-pfic line

            deemed_pfics = base_flowup.filter(

                (F.col("LineID") == type_of_pfic_line_id)

                & (F.lower(F.coalesce(F.col("TextValue"), F.lit(""))) == (is_1293_deemed_desc or "").lower())

            ).select(

                F.coalesce(F.col("EntityID"), F.lit(0)).alias("_EntityID"),

                F.coalesce(F.col("FlowupEntityID"), F.lit(0)).alias("_FlowupEntityID"),

                F.coalesce(F.col("SourceEntityID"), F.lit(0)).alias("_SourceEntityID"),

                F.coalesce(F.col("PFICFootnoteID"), F.lit(0)).alias("_PFICFootnoteID"),

                F.coalesce(F.col("TrackingKey"), F.lit("")).alias("_TrackingKey"),

            ).distinct()

            # Find PFICs needing new Election-D rows (don't already have one)

            existing_elec_d = base_flowup.filter(F.col("LineID") == election_d_line_id) \
                .select(

                    F.coalesce(F.col("EntityID"), F.lit(0)).alias("_EntityID"),

                    F.coalesce(F.col("FlowupEntityID"), F.lit(0)).alias("_FlowupEntityID"),

                    F.coalesce(F.col("SourceEntityID"), F.lit(0)).alias("_SourceEntityID"),

                    F.coalesce(F.col("PFICFootnoteID"), F.lit(0)).alias("_PFICFootnoteID"),

                    F.coalesce(F.col("TrackingKey"), F.lit("")).alias("_TrackingKey"),

                ).distinct()

            # New Election-D inserts for deemed PFICs missing them

            pfics_needing_elec_d = deemed_pfics.join(existing_elec_d, [

                "_EntityID", "_FlowupEntityID", "_SourceEntityID", "_PFICFootnoteID", "_TrackingKey"

            ], "left_anti")

            # Get one representative row per PFIC to get RunID etc.

            rep_rows = base_flowup.filter(

                (F.col("LineID") == type_of_pfic_line_id)

                & (F.lower(F.coalesce(F.col("TextValue"), F.lit(""))) == (is_1293_deemed_desc or "").lower())

            ).select(

                "RunID", "ClientID", "TaxPeriodID", "EntityID", "FlowupEntityID",

                "SourceEntityID", "PFICFootnoteID", "TrackingKey",

            )

            election_d_inserts = rep_rows.alias("PF") \
                .join(

                    pfics_needing_elec_d.alias("ND"),

                    (F.coalesce(F.col("PF.EntityID"), F.lit(0)) == F.col("ND._EntityID"))

                    & (F.coalesce(F.col("PF.FlowupEntityID"), F.lit(0)) == F.col("ND._FlowupEntityID"))

                    & (F.coalesce(F.col("PF.SourceEntityID"), F.lit(0)) == F.col("ND._SourceEntityID"))

                    & (F.coalesce(F.col("PF.PFICFootnoteID"), F.lit(0)) == F.col("ND._PFICFootnoteID"))

                    & (F.coalesce(F.col("PF.TrackingKey"), F.lit("")) == F.col("ND._TrackingKey")),

                    "inner"

                ) \
                .select(

                    F.col("PF.RunID"), F.col("PF.ClientID"), F.col("PF.TaxPeriodID"),

                    F.col("PF.EntityID"), F.col("PF.FlowupEntityID"), F.col("PF.SourceEntityID"),

                    F.col("PF.PFICFootnoteID"),

                    F.lit(election_d_line_id).alias("LineID"),

                    F.lit(None).cast("double").alias("Amount"),

                    F.lit("True").alias("TextValue"),

                    F.col("PF.TrackingKey"),

                )

            # Update existing Election-D rows to 'True' where PFIC is deemed

            updated_flowup = base_flowup.alias("PF") \
                .join(

                    deemed_pfics.alias("DP"),

                    (F.coalesce(F.col("PF.EntityID"), F.lit(0)) == F.col("DP._EntityID"))

                    & (F.coalesce(F.col("PF.FlowupEntityID"), F.lit(0)) == F.col("DP._FlowupEntityID"))

                    & (F.coalesce(F.col("PF.SourceEntityID"), F.lit(0)) == F.col("DP._SourceEntityID"))

                    & (F.coalesce(F.col("PF.PFICFootnoteID"), F.lit(0)) == F.col("DP._PFICFootnoteID"))

                    & (F.coalesce(F.col("PF.TrackingKey"), F.lit("")) == F.col("DP._TrackingKey")),

                    "left"

                ) \
                .select(

                    F.col("PF.RunID"), F.col("PF.ClientID"), F.col("PF.TaxPeriodID"),

                    F.col("PF.EntityID"), F.col("PF.FlowupEntityID"), F.col("PF.SourceEntityID"),

                    F.col("PF.PFICFootnoteID"), F.col("PF.LineID"), F.col("PF.Amount"),

                    F.when(

                        (F.col("PF.LineID") == election_d_line_id) & F.col("DP._EntityID").isNotNull(),

                        F.lit("True")

                    ).otherwise(F.col("PF.TextValue")).alias("TextValue"),

                    F.col("PF.TrackingKey"),

                )

            base_flowup = updated_flowup.unionByName(election_d_inserts)

    log_timing("build_pfic_flowup_pipeline", t0)
    _log("build_pfic_flowup_pipeline END")
    return base_flowup

def build_custom_footnote_input(

    spark: SparkSession, cfg: dict,

) -> DataFrame:

    """Build custom footnote input rows for allocation.

    Uses latest transaction IDs per entity/line type, then loads from

    CustomFootnoteInput and CustomFootnoteAllocationSummary (flowup path).

    SQL lines: 2800-2920

    Returns: DataFrame with AllocationInput schema

    """

    log_section("build_custom_footnote_input")
    _log("build_custom_footnote_input START")
    t0 = time.time()

    prefix = table_prefix(cfg)

    client_id = cfg["client_id"]

    tax_period_id = cfg["tax_period_id"]

    run_id = cfg["run_id"]

    entity_id = cfg["entity_id"]

    phase_id = cfg.get("phase_id", 0)

    fx_tid = cfg.get("fx_rate_transaction_id") or 0

    is_tracking = cfg.get("is_tracking_key", "C") == "C"

    is_pfic_cfc_qfc = cfg.get("is_pfic_cfc_qfc_entity", False)

    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)

    cf_gate_open = (not is_pfic_cfc_qfc) or (not is_blocker_checked)

    parts = []

    # Read required tables

    entity_relationship = read_table(spark, "EntityRelationship", cfg)

    entity_tbl = read_table(spark, "Entity", cfg)

    enu_entity_type = read_table(spark, "ENU_EntityType", cfg)

    custom_import_detail = read_table(spark, "CustomImportDetail", cfg)

    enu_event = read_table(spark, "ENU_Event", cfg)

    enu_line_type = read_table(spark, "ENU_LineType", cfg)

    transaction_log = read_table(spark, "TransactionLog", cfg)

    workflow_status = read_table(spark, "WorkflowStatus", cfg)

    custom_footnote_line_item = read_table(spark, "CustomFootnoteLineItem", cfg)

    custom_footnote_input = read_table(spark, "CustomFootnoteInput", cfg)

    custom_footnote_alloc_summary = read_table(spark, "CustomFootnoteAllocationSummary", cfg)

    custom_footnote_package = read_table(spark, "CustomFootnotePackage", cfg)

    lower_tier_funds = _cached_lower_tier_funds(spark, cfg, run_id)

    entity_tv = F.broadcast(spark.table("_entity"))

    fx_avg_rate = F.broadcast(spark.table("_fx_avg_rate"))

    k1_package = F.broadcast(spark.table("_k1_package"))

    # Step 1: Get latest custom footnote transaction IDs

    # Sub-step: get entity list (self + underlying investments)

    investment_type_id = enu_entity_type.filter(

        (F.lower(F.col("EntityTypeName")) == "investment") & (F.col("ClientID") == client_id)

    ).select("EntityTypeID").first()

    investment_type_id_val = investment_type_id["EntityTypeID"] if investment_type_id else -1

    investments = entity_relationship.alias("ER") \
        .join(entity_tbl.alias("E"), F.col("ER.LowerTierEntityID") == F.col("E.EntityID"), "inner") \
        .filter(

            (F.col("ER.UpperTierEntityID") == entity_id)

            & (F.col("E.FundOrInvestmentID") == investment_type_id_val)

        ).select(F.col("E.EntityID"))

    cf_entity_list = spark.createDataFrame([(entity_id,)], ["EntityID"]).union(investments)

    # Sub-step: get custom footnote event types

    cf_event_types = custom_import_detail.alias("CD") \
        .join(enu_event.alias("EE"), F.col("CD.ImportName") == F.col("EE.EventName"), "inner") \
        .join(

            enu_line_type.alias("EL"),

            (F.col("EL.LineType") == F.col("CD.ImportName"))

            & (F.col("EL.ClientID") == client_id)

            & (F.col("EL.TaxPeriodID") == tax_period_id),

            "inner"

        ) \
        .filter(F.col("CD.IsCustomFootnote") == True) \
        .select(

            F.col("EE.EventTypeID"),

            F.col("EL.LineTypeID"),

            F.col("CD.GlobalMenuID").alias("RegisterTypeID"),

        )

    # Sub-step: Get rejected/error status IDs to exclude

    excluded_statuses = workflow_status.filter(
        F.lower(F.col("EnumerationName")).isin("rejected", "err_critical", "err_noncritical")
    ).select("StatusID")

    excluded_status_ids = None  # use join anti-pattern below

    # Sub-step: Get global event names (those that apply across all entities)

    global_event_names = [

        'Import_EntityRelationship', 'Import_Historic',

        'Import_MasterTaxableIncome', 'DataFeed_ByEntityInvestment',

        'DataFeed_Entities', 'DataFeed_Deals-Specific',

        'DataFeed_Investors-Specific', 'DataFeed_Investors',

        'DataFeed_Deals', 'DataFeed_Chart of Accounts',

        'DataFeed_Financial', 'Import_CompositeWithholdingBridge',

        'Import_WHPaymentAllocation', 'Import_EntityConfiguration',

    ]

    global_event_ids_df = F.broadcast(
        enu_event.filter(F.lower(F.col("EventName")).isin([e.lower() for e in global_event_names])).select("EventTypeID")
    )
    global_event_ids = None  # use join below

    # Sub-step: Build cross join of entity list x event types + k1_package

    entity_event_cross = cf_entity_list.alias("EL") \
        .crossJoin(cf_event_types.alias("TF")) \
        .join(k1_package.alias("K"), F.col("K.UpperTierEntityID") == F.col("EL.EntityID"), "inner")

    # Sub-step: Get max TransactionID per (EntityID, EventTypeID) from TransactionLog

    valid_txn_log = transaction_log.filter(
        (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.col("PhaseID") == phase_id)
        & (F.col("StatusID") != 0)
    ).join(excluded_statuses, "StatusID", "left_anti")

    # For entity-specific events: join on EntityID

    entity_specific_txn = valid_txn_log.join(global_event_ids_df, "EventTypeID", "left_anti") \
        .groupBy("EventTypeID", "EntityID") \
        .agg(F.max("TransactionID").alias("TransactionID"))

    # For global events: max TransactionID per EventTypeID (any entity)

    global_txn = valid_txn_log.join(global_event_ids_df, "EventTypeID", "inner") \
        .groupBy("EventTypeID") \
        .agg(F.max("TransactionID").alias("TransactionID"))

    # Join entity_event_cross with transaction data

    # Entity-specific path

    path_specific = entity_event_cross.alias("EEC") \
        .join(

            entity_specific_txn.alias("TS"),

            (F.col("EEC.EventTypeID") == F.col("TS.EventTypeID"))

            & (F.col("EEC.EntityID") == F.col("TS.EntityID")),

            "inner"

        ) \
        .select(

            F.col("EEC.EntityID"),

            F.col("TS.TransactionID"),

            F.col("EEC.LineTypeID"),

            F.col("EEC.EventTypeID"),

            F.col("EEC.RegisterTypeID"),

            F.coalesce(F.col("EEC.K1PackageID"), F.lit(0)).alias("K1PackageID"),

        )

    # Global path

    path_global = entity_event_cross.alias("EEC") \
        .join(

            global_txn.alias("TG"),

            F.col("EEC.EventTypeID") == F.col("TG.EventTypeID"),

            "inner"

        ) \
        .select(

            F.col("EEC.EntityID"),

            F.col("TG.TransactionID"),

            F.col("EEC.LineTypeID"),

            F.col("EEC.EventTypeID"),

            F.col("EEC.RegisterTypeID"),

            F.coalesce(F.col("EEC.K1PackageID"), F.lit(0)).alias("K1PackageID"),

        )

    cf_latest_txn = path_specific.unionByName(path_global) \
        .filter(F.col("TransactionID").isNotNull()) \
        .distinct()

    # Register as temp view for write_form_flowups (CustomFootnoteFlowup section)

    cf_latest_txn.createOrReplaceTempView(f"_cf_latest_txn_{run_id}")

    # Step 2: Get custom footnote line items for filtering

    cf_line_items = custom_footnote_line_item.filter(

        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)

    ).select("LineID", "LineDataType", "IsAllocable", "IsActive", "ClientID", "TaxPeriodID")

    # Step 3: Direct custom footnote input (from CustomFootnoteInput via latest transaction)

    cf_latest_txn_direct = cf_latest_txn.select(

        F.col("TransactionID"), F.col("EntityID"), F.col("LineTypeID")

    ).distinct()

    direct_cf = custom_footnote_input.alias("CF") \
        .join(

            cf_latest_txn_direct.alias("TT"),

            F.col("CF.CustomFootnoteTransactionID") == F.col("TT.TransactionID"),

            "inner"

        ) \
        .join(

            cf_line_items.alias("FL"),

            (F.col("CF.LineID") == F.col("FL.LineID"))

            & (F.col("FL.IsAllocable") == True)

            & (F.col("FL.IsActive") == True)

            & (F.coalesce(F.col("CF.Amount"), F.lit(0)) != 0),

            "inner"

        ) \
        .join(entity_tv.alias("E"), F.col("E.EntityID") == F.col("TT.EntityID"), "inner") \
        .join(

            custom_footnote_package.alias("P"),

            F.col("P.CustomFootnoteID") == F.col("CF.CustomFootnoteID"),

            "inner"

        ) \
        .join(k1_package.alias("K1P"), F.col("K1P.K1PackageID") == F.col("P.K1PackageID"), "inner") \
        .join(fx_avg_rate.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left") \
        .filter(

            (F.col("CF.ClientID") == client_id)

            & (F.col("CF.TaxPeriodID") == tax_period_id)

        ) \
        .select(

            F.lit(None).cast("int").alias("SuperParentEntityID"),

            F.col("K1P.LowerTierEntityID").alias("EntityID"),

            F.col("TT.LineTypeID").alias("LineTypeID"),

            F.col("CF.LineID"),

            F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("CF.Amount"))

             .otherwise(F.round(F.col("CF.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))

             .alias("Amount"),

            F.lit(None).cast("string").alias("TransactionName"),

            F.lit(None).cast("int").alias("TransactionEntityID"),

            F.col("CF.CustomFootnoteID").alias("QuicklinkID"),

            F.lit(None).cast("int").alias("CategoryID"),

            F.lit(None).cast("int").alias("PeriodID"),

            F.lit(None).cast("string").alias("LineCode"),

            F.lit(0).alias("ParentEntityID"),

            F.lit(None).cast("int").alias("AdjustmentTypeID"),

            F.lit(None).cast("string").alias("Tag"),

            F.when(F.lit(is_tracking), F.col("TT.EntityID").cast("string"))

             .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),

            F.lit(None).cast("int").alias("SchID"),

            F.lit(None).cast("int").alias("OriginalParentEntityID"),

        )

    if cf_gate_open:  # K1 (Round 10): SP L1227-1582 IsPficCfcQfc/blocker gate

        parts.append(direct_cf)

    # Step 4: Flowup from CustomFootnoteAllocationSummary

    flowup_cf = custom_footnote_alloc_summary.alias("CFA") \
        .join(

            lower_tier_funds.alias("LT"),

            (F.col("CFA.RunID") == F.col("LT.RunID"))

            & (F.col("LT.PartnerNumber") == F.col("CFA.PartnerNumber")),

            "inner"

        ) \
        .join(

            cf_line_items.alias("FL"),

            (F.col("CFA.LineID") == F.col("FL.LineID"))

            & F.upper(F.col("FL.LineDataType")).isin("NUMBER", "PERCENT")

            & (F.col("FL.IsAllocable") == True),

            "inner"

        ) \
        .join(

            custom_footnote_package.alias("P"),

            F.col("P.CustomFootnoteID") == F.col("CFA.CustomFootnoteID"),

            "inner"

        ) \
        .join(k1_package.alias("K"), F.col("K.K1PackageID") == F.col("P.K1PackageID"), "inner") \
        .filter(

            (F.col("CFA.ClientID") == client_id)

            & (F.col("CFA.TaxPeriodID") == tax_period_id)

        ) \
        .groupBy(

            F.col("CFA.LineID"), F.col("CFA.LineTypeID"), F.col("CFA.CustomFootnoteID"),

            F.col("K.LowerTierEntityID"),

            F.coalesce(F.col("CFA.ParentEntityId"), F.lit(0)).alias("_ParentEntityId_raw"),

            F.col("CFA.SourceEntityID"),

            F.col("CFA.EntityID"),

            F.coalesce(F.col("CFA.TrackingKey"), F.lit("")).alias("_TrackingKey"),

            F.coalesce(F.col("CFA.OriginalParentEntityID"), F.lit(entity_id)).alias("_OriginalParentEntityID"),

        ) \
        .agg(F.sum("CFA.FlowupAmount").alias("Amount")) \
        .select(

            F.col("EntityID").alias("SuperParentEntityID"),

            F.col("LowerTierEntityID").alias("EntityID"),

            F.col("LineTypeID"),

            F.col("LineID"),

            F.col("Amount"),

            F.lit(None).cast("string").alias("TransactionName"),

            F.lit(None).cast("int").alias("TransactionEntityID"),

            F.col("CustomFootnoteID").alias("QuicklinkID"),

            F.lit(None).cast("int").alias("CategoryID"),

            F.lit(None).cast("int").alias("PeriodID"),

            F.lit(None).cast("string").alias("LineCode"),

            F.when(

                (F.col("_ParentEntityId_raw") == 0) | (F.col("_ParentEntityId_raw") == F.col("SourceEntityID")),

                F.when(F.col("EntityID") == F.col("LowerTierEntityID"), F.lit(0))

                 .otherwise(F.col("EntityID"))

            ).otherwise(F.col("_ParentEntityId_raw")).alias("ParentEntityID"),

            F.lit(None).cast("int").alias("AdjustmentTypeID"),

            F.lit(None).cast("string").alias("Tag"),

            F.col("_TrackingKey").alias("TrackingKey"),

            F.lit(None).cast("int").alias("SchID"),

            F.col("_OriginalParentEntityID").alias("OriginalParentEntityID"),

        )

    if cf_gate_open:  # K1 (Round 10): SP L1227-1582 IsPficCfcQfc/blocker gate

        parts.append(flowup_cf)

    if not parts:

        result = spark.createDataFrame([], StructType())

    else:

        result = parts[0]

        for p in parts[1:]:

            result = result.unionByName(p, allowMissingColumns=True)

    log_timing("build_custom_footnote_input", t0)

    return result

def check_pfic_xml_override_alert(

    spark: SparkSession, cfg: dict, pfic_flowup_df: DataFrame,

) -> None:

    """Detect PFIC XML override changes between runs and insert alerts.

    SQL lines: 5100-5285

    Compares BINARY_CHECKSUM(TextValue, Amount) for PFIC flowup rows matched

    against PFICXmlOverrideInput between current and previous successful run.

    If any checksum differs (data changed), inserts into PFICUpdateAlert + PFICAlertDetails.

    """

    log_section("check_pfic_xml_override_alert")
    _log("check_pfic_xml_override_alert START")
    t0 = time.time()

    prefix = table_prefix(cfg)

    client_id = cfg["client_id"]

    tax_period_id = cfg["tax_period_id"]

    entity_id = cfg["entity_id"]

    run_id = cfg["run_id"]

    phase_id = cfg["phase_id"]

    # Read required tables

    global_menu = read_table(spark, "GlobalMenu", cfg)

    enu_global_menu_group = read_table(spark, "ENU_GlobalMenuGroup", cfg)

    enu_event = read_table(spark, "ENU_Event", cfg)

    enu_df_datalist = read_table(spark, "ENU_DF_DATALIST", cfg)

    pfic_line_item = spark.table("_pfic_line_item")

    # Check if "Override 8621 XML Import" is enabled

    xml_enabled = global_menu.alias("GM") \
        .join(enu_global_menu_group.alias("GMG"),

              F.col("GM.GlobalMenuGroupID") == F.col("GMG.GlobalMenuGroupID"), "inner") \
        .filter(

            (F.lower(F.col("GMG.GroupName")) == "other logic/imports")

            & (F.lower(F.col("GM.MenuName")) == "override 8621 xml import")

            & (F.upper(F.col("GM.State")) == "C")

            & (F.col("GM.ClientID") == client_id)

            & (F.col("GM.TaxPeriodID") == tax_period_id)

        ).limit(1).first()

    if xml_enabled is None:

        log_timing("check_pfic_xml_override_alert", t0)

        return

    # Batch 3 small lookups into one collect to save 2 roundtrips

    _lookup_rows = (

        enu_event.filter(F.lower(F.col("EventName")) == "import_override8621xml_entitydata")

        .select(F.col("EventTypeID").cast("string").alias("val"), F.lit("xml_event").alias("key"))

        .unionByName(

            enu_df_datalist.filter(

                (F.lower(F.col("Category")) == "pficxmlsource") & (F.lower(F.col("LookUpData")) == "ipacs 8621 flowup")

            ).select(F.col("LookUpValue").cast("string").alias("val"), F.lit("data_source").alias("key"))

        )

        .unionByName(

            pfic_line_item.filter(F.lower(F.col("ShortName")) == "investment")

            .select(F.col("LineID").cast("string").alias("val"), F.lit("pfic_inv_line").alias("key"))

        )

        .collect()

    )

    _lookup_map = {r["key"]: r["val"] for r in _lookup_rows}

    xml_event_type_id = int(_lookup_map["xml_event"]) if _lookup_map.get("xml_event") else None

    data_source_id = _lookup_map.get("data_source")  # keep as string — used in SourceType comparison

    pfic_investment_line_id = int(_lookup_map["pfic_inv_line"]) if _lookup_map.get("pfic_inv_line") else None

    if xml_event_type_id is None or data_source_id is None or pfic_investment_line_id is None:

        log_timing("check_pfic_xml_override_alert", t0)

        return

    # Get latest transaction ID for this event (udfGetLatestTransactionID, IncludeFailed=0).

    transaction_log = read_table(spark, "TransactionLog", cfg)

    _ws_xml = read_table(spark, "WorkflowStatus", cfg)

    _xml_excl_ids = [

        r["StatusID"] for r in _ws_xml.filter(

            F.lower(F.col("EnumerationName")).isin("rejected", "err_critical", "err_noncritical")

        ).select("StatusID").collect()

    ]

    xml_trans_row = transaction_log.alias("TL") \
        .filter(

            (F.col("TL.ClientID") == client_id)

            & (F.col("TL.TaxPeriodID") == tax_period_id)

            & (F.col("TL.EventTypeID") == xml_event_type_id)

            & (F.col("TL.EntityID") == entity_id)

            & (F.col("TL.PhaseID") == phase_id)

            & (~F.col("TL.StatusID").isin(_xml_excl_ids))

        ).agg(F.max("TL.TransactionID").alias("TransactionID")).first()

    xml_trans_id = xml_trans_row["TransactionID"] if xml_trans_row else None

    if not xml_trans_id or xml_trans_id == 0:

        log_timing("check_pfic_xml_override_alert", t0)

        return

    # Load PFICXmlOverrideInput for current transaction

    pfic_xml_override_input = read_table(spark, "PFICXmlOverrideInput", cfg)

    pfic_xml_override_package = read_table(spark, "PFICXmlOverridePackage", cfg)

    xml_override_input = pfic_xml_override_input.alias("P") \
        .join(

            pfic_xml_override_package.alias("PK"),

            (F.col("P.PFICXmlOverrideFootnoteID") == F.col("PK.PFICXmlOverrideFootnoteID"))

            & (F.col("PK.SourceType") == data_source_id),

            "inner"

        ) \
        .filter(

            (F.col("PK.EntityTransactionID") == xml_trans_id)

            & (

                (F.coalesce(F.col("P.Amount").cast("string"), F.lit("")) != "")

                | (F.coalesce(F.col("P.TextValue"), F.lit("")) != "")

            )

        ) \
        .select(

            F.col("P.PFICXmlOverrideFootnoteID"), F.col("P.LineID"),

            F.col("PK.EntityID"), F.coalesce(F.col("PK.InvestmentID"), F.lit(0)).alias("FlowupEntityID"),

            F.coalesce(F.col("PK.SourceEntityID"), F.lit(0)).alias("SourceEntityID"),

            F.col("P.Amount"), F.col("P.TextValue"),

        )

    # Find previous successful run

    allocation_run = read_table(spark, "AllocationRun", cfg)

    prv_run_row = allocation_run.filter(

        (F.col("RunID") < run_id)

        & (F.col("ClientID") == client_id)

        & (F.col("TaxPeriodID") == tax_period_id)

        & (F.col("PhaseID") == phase_id)

        & (F.col("EntityID") == entity_id)

        & (F.upper(F.col("RunStatus")) == "SUCCESS")

    ).agg(F.max("RunID").alias("PrvRunID")).first()

    prv_run_id = prv_run_row["PrvRunID"] if prv_run_row else None

    if not prv_run_id or prv_run_id == 0:

        log_timing("check_pfic_xml_override_alert", t0)

        return

    # Build checksum data for current run

    cur_investment_ids = pfic_flowup_df.alias("P").filter(

        (F.col("P.RunID") == run_id)

        & (F.col("P.LineID") == pfic_investment_line_id)

        & (F.col("P.SourceEntityID") != entity_id)

    ).join(

        xml_override_input.alias("I"),

        (F.col("P.TextValue") == F.col("I.TextValue"))

        & (F.col("P.LineID") == F.col("I.LineID"))

        & (F.col("P.EntityID") == F.col("I.EntityID"))

        & (F.col("P.FlowupEntityID") == F.col("I.FlowupEntityID"))

        & (F.col("P.SourceEntityID") == F.col("I.SourceEntityID")),

        "inner"

    ).select(

        F.col("P.PFICFootnoteID"),

        F.col("P.TextValue").alias("InvestmentID"),

    )

    pfic_cur_data = pfic_flowup_df.alias("P") \
        .filter(F.col("P.RunID") == run_id) \
        .join(cur_investment_ids.alias("PD"), F.col("P.PFICFootnoteID") == F.col("PD.PFICFootnoteID"), "inner") \
        .join(

            xml_override_input.alias("I"),

            (F.col("P.LineID") == F.col("I.LineID"))

            & (F.col("P.EntityID") == F.col("I.EntityID"))

            & (F.col("P.FlowupEntityID") == F.col("I.FlowupEntityID"))

            & (F.col("P.SourceEntityID") == F.col("I.SourceEntityID")),

            "inner"

        ) \
        .select(

            F.col("P.EntityID"), F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),

            F.col("PD.InvestmentID"), F.col("P.LineID"),

            F.hash(F.col("P.TextValue"), F.col("P.Amount")).alias("CheckSumNumber"),

        )

    # Build checksum data for previous run

    prv_investment_ids = pfic_flowup_df.alias("P").filter(

        (F.col("P.RunID") == prv_run_id)

        & (F.col("P.LineID") == pfic_investment_line_id)

        & (F.col("P.SourceEntityID") != entity_id)

    ).join(

        xml_override_input.alias("I"),

        (F.col("P.TextValue") == F.col("I.TextValue"))

        & (F.col("P.LineID") == F.col("I.LineID"))

        & (F.col("P.EntityID") == F.col("I.EntityID"))

        & (F.col("P.FlowupEntityID") == F.col("I.FlowupEntityID"))

        & (F.col("P.SourceEntityID") == F.col("I.SourceEntityID")),

        "inner"

    ).select(

        F.col("P.PFICFootnoteID"),

        F.col("P.TextValue").alias("InvestmentID"),

    )

    pfic_prv_data = pfic_flowup_df.alias("P") \
        .filter(F.col("P.RunID") == prv_run_id) \
        .join(prv_investment_ids.alias("PD"), F.col("P.PFICFootnoteID") == F.col("PD.PFICFootnoteID"), "inner") \
        .join(

            xml_override_input.alias("I"),

            (F.col("P.LineID") == F.col("I.LineID"))

            & (F.col("P.EntityID") == F.col("I.EntityID"))

            & (F.col("P.FlowupEntityID") == F.col("I.FlowupEntityID"))

            & (F.col("P.SourceEntityID") == F.col("I.SourceEntityID")),

            "inner"

        ) \
        .select(

            F.col("P.EntityID"), F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),

            F.col("PD.InvestmentID"), F.col("P.LineID"),

            F.hash(F.col("P.TextValue"), F.col("P.Amount")).alias("CheckSumNumber"),

        )

    # Compare checksums — only flag updates (not inserts/deletes)

    changed_rows = pfic_cur_data.alias("P") \
        .join(

            pfic_prv_data.alias("PD"),

            (F.col("P.EntityID") == F.col("PD.EntityID"))

            & (F.col("P.FlowupEntityID") == F.col("PD.FlowupEntityID"))

            & (F.col("P.SourceEntityID") == F.col("PD.SourceEntityID"))

            & (F.col("P.InvestmentID") == F.col("PD.InvestmentID"))

            & (F.col("P.LineID") == F.col("PD.LineID")),

            "inner"

        ) \
        .filter(F.col("P.CheckSumNumber") != F.col("PD.CheckSumNumber"))

    has_changes = changed_rows.limit(1).first() is not None

    if has_changes:

        try:

            mx = read_table(spark, "PFICUpdateAlert", cfg).agg(

                F.coalesce(F.max("AlertID"), F.lit(0)).alias("m")

            ).first()

            alert_id = int(mx["m"] or 0) + 1

        except Exception:

            alert_id = 1

        alert_header_df = spark.createDataFrame(

            [(alert_id, entity_id, client_id, tax_period_id, True)],

            ["AlertID", "EntityID", "ClientID", "TaxPeriodID", "IsActive"]

        ).withColumn("UpdateTime", F.current_timestamp())

        _collect_result(cfg, alert_header_df, "PFICUpdateAlert")

        if alert_id:

            # Insert alert details for changed rows (same AlertID as the header)

            alert_details_df = changed_rows.select(

                F.lit(alert_id).alias("AlertID"),

                F.col("P.EntityID"),

                F.col("P.FlowupEntityID").alias("FlowUpEntityID"),

                F.col("P.SourceEntityID"),

                F.col("P.InvestmentID").alias("PFICFootNoteEntityID"),

                F.lit(data_source_id).cast("int").alias("DataSourceID"),

                F.col("P.LineID"),

            )

            _collect_result(cfg, alert_details_df, "PFICAlertDetails")

    log_timing("check_pfic_xml_override_alert", t0)
