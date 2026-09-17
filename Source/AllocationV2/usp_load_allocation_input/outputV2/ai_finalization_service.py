"""
ai_finalization_service.py

Tag percentage allocation, rounding, and final writes for uspLoadAllocationInput.

Applies tag-based percentage splits with plug-to-largest rounding,
then writes to AllocationInput and PFICFootnoteFlowup tables.

SQL lines: 6900-7363
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing
from . import spark_optimizations as _ckpt


def prune_to_lower_tier_runs(df, spark, cfg):
    fn = getattr(_ckpt, "prune_to_lower_tier_runs", None)
    if fn:
        return fn(df, spark, cfg)
    if "RunID" not in df.columns:
        return df
    keys = (
        spark.table(f"_lower_tier_funds_{cfg['run_id']}")
        .select(F.col("RunID").cast("long").alias("RunID"))
        .where(F.col("RunID").isNotNull())
        .distinct()
    )
    return df.join(F.broadcast(keys), "RunID", "left_semi")

logger = logging.getLogger(__name__)


def purge_output_tables(spark: SparkSession, cfg: dict) -> None:
    """Purge is now handled via replaceWhere in the write phase.

    All tables use atomic overwrite with a partition predicate — no separate
    DELETE transactions needed. This function is kept as a no-op placeholder
    for orchestration compatibility.
    """
    log_section("purge_output_tables")
    t0 = time.time()
    # No-op: all purge logic moved to replaceWhere in the write phase
    log_timing("purge_output_tables", t0)


def _collect_result(cfg: dict, df: DataFrame, table_name: str) -> None:
    """Collect DataFrame for batch write via GenericResultStorer at end of SP."""
    # Cast columns to match the actual Delta table schema (avoids merge errors)
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
    from pyspark.sql.types import StringType, NumericType
    src_types = {f.name: f.dataType for f in df.schema.fields}
    for col_name in df.columns:
        if col_name in target_types:
            src_type = src_types.get(col_name)
            tgt_type = target_types[col_name]
            # Skip unsafe STRING→numeric casts (would lose data)
            if isinstance(src_type, StringType) and isinstance(tgt_type, NumericType):
                continue
            if src_type != tgt_type:
                df = df.withColumn(col_name, F.col(col_name).cast(tgt_type))
    if "_parquet_results" not in cfg:
        cfg["_parquet_results"] = {}
    if table_name in cfg["_parquet_results"]:
        cfg["_parquet_results"][table_name] = cfg["_parquet_results"][table_name].unionByName(df, allowMissingColumns=True)
    else:
        cfg["_parquet_results"][table_name] = df


def apply_master_feed_override(
    spark: SparkSession, cfg: dict, allocation_input_df: DataFrame,
) -> DataFrame:
    """Delete K1/Form926/199A/8886/PFIC rows for entities with master feed override.

    SQL lines: 6460-6530. If GlobalMenu 'Run ProRata with Master Feed Alloc' is enabled,
    calls uspGetMasterFeedEntityInvs and deletes matching rows by SourceType.
    """
    log_section("apply_master_feed_override")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    k1_lt = cfg.get("k1_line_type_id")
    form926_lt = cfg.get("form926_line_type_id")
    form199a_lt = cfg.get("form199a_line_type_id")
    form8886_lt = cfg.get("form8886_line_type_id")
    pfic_lt = cfg.get("pfic_footnote_line_type_id")

    # Check if master feed override is enabled (pre-loaded in config)
    is_master_feed = cfg.get("is_master_feed_override", False)

    if not is_master_feed:
        log_timing("apply_master_feed_override", t0)
        return allocation_input_df

    # Get master feed entities — calls the SP logic inline
    # uspGetMasterFeedEntityInvs returns (EntityID, LowerTierEntityInvID, SourceType)
    # Reads MasterImportEntityFeed and resolves LowerTierEntityInvID via Entity join
    vw_entity_df = read_table(spark, "Entity", cfg)
    master_feed_df = (
        read_table(spark, "MasterImportEntityFeed", cfg)
        .filter(
            (F.col("EntityId") == entity_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
        .alias("T")
        .join(
            vw_entity_df.alias("E"),
            F.col("T.InvestmentIdentification") == F.col("E.EntityIdentification"),
            "left"
        )
        .select(
            F.col("T.EntityId").alias("EntityID"),
            F.coalesce(F.col("E.EntityID"), F.col("T.EntityId")).alias("LowerTierEntityInvID"),
            F.col("T.Source").alias("SourceType"),
        )
        .distinct()
    )

    # Build source type to line type mapping
    source_type_map = {
        "K-1 Input": k1_lt,
        "Form 926": form926_lt,
        "Form 199A": form199a_lt,
        "Form 8886": form8886_lt,
        "Foreign Corporations": pfic_lt,
    }

    del_lt_col = F.lit(None).cast("int")
    for source_type, lt_id in source_type_map.items():
        if lt_id:
            del_lt_col = F.when(
                F.lower(F.col("SourceType")) == source_type.lower(), F.lit(lt_id)
            ).otherwise(del_lt_col)

    delete_keys = (
        master_feed_df
        .withColumn("_del_lt", del_lt_col)
        .filter(F.col("_del_lt").isNotNull())
        .select(
            F.col("LowerTierEntityInvID").alias("_del_entity"),
            F.col("_del_lt"),
        )
        .distinct()
    )

    result = (
        allocation_input_df.alias("A")
        .join(
            delete_keys.alias("DK"),
            (F.col("A.EntityID") == F.col("DK._del_entity")) &
            (F.col("A.LineTypeID") == F.col("DK._del_lt")),
            "left_anti"
        )
    )

    log_timing("apply_master_feed_override", t0)
    return result


def apply_blocker_entity_cleanup(
    spark: SparkSession, cfg: dict, allocation_input_df: DataFrame,
) -> DataFrame:
    """Blocker-entity cleanup — filters AllocationInput based on entity PFIC/CFC/QFC status.

    SQL lines: 6535-6567:
    - PFIC entities: keep K1 + PFIC rows for other entities (with K1 PFICClassType filter)
    - CFC/QFC entities: keep only PFIC rows for other entities
    - Other: only keep rows for current entity

    Also handles IsPficCfcQfcEntity + IsForeignBlockerFootnotesFlowupChecked:
    only keep rows with LineTypeID in ENU_DF_DataList 'AllocateFootNoteData'.
    """
    log_section("apply_blocker_entity_cleanup")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    is_blocker_entity = cfg.get("is_blocker_entity", False)
    is_pfic = cfg.get("is_pfic", False)
    is_cfc_or_qfc = cfg.get("is_cfc_or_qfc", False)
    is_pfic_cfc_qfc = cfg.get("is_pfic_cfc_qfc_entity", False)
    is_blocker_checked = cfg.get("is_foreign_blocker_footnotes_flowup_checked", False)
    k1_lt = cfg.get("k1_line_type_id")
    pfic_lt = cfg.get("pfic_footnote_line_type_id")

    # SQL lines 6500-6567: Entity cleanup DELETE logic is gated by
    # IF @IsTPGBlocker = 1 (source SP). For non-blocker entities, NO entity
    # filtering occurs — all EntityIDs from the pipeline are kept.
    if is_blocker_entity:
        if is_pfic:
            # PFIC: keep K1 + PFIC rows for other entities, but K1 only for PFIC-classified lines
            k1_line_item_df = spark.table("_k1_line_item")
            pfic_class_types = ['PFIC Ordinary', 'PFIC Gain', 'PFIC Distribution', 'PFIC Gain Longterm']
            pfic_lines = (
                k1_line_item_df
                .filter(F.lower(F.col("PFICClassType")).isin([t.lower() for t in pfic_class_types]))
                .select("LineID").distinct()
            )
            pfic_line_ids = [r["LineID"] for r in pfic_lines.collect()]
            result = allocation_input_df.filter(
                (F.col("EntityID") == entity_id)
                | (F.col("LineTypeID") == pfic_lt)
                | ((F.col("LineTypeID") == k1_lt) & F.col("LineID").isin(pfic_line_ids))
            )
        elif is_cfc_or_qfc:
            # CFC/QFC: keep PFIC rows for other entities
            result = allocation_input_df.filter(
                (F.col("EntityID") == entity_id) | (F.col("LineTypeID") == pfic_lt)
            )
        else:
            # Non-PFIC/CFC/QFC blocker entity: only keep current entity rows
            result = allocation_input_df.filter(F.col("EntityID") == entity_id)
    else:
        # Non-blocker entity: keep ALL rows (no entity filter) — matches SQL SP
        result = allocation_input_df

    if is_pfic_cfc_qfc and is_blocker_checked:
        allowed_scope = (
            read_table(spark, "ENU_DF_DataList", cfg)
            .filter(
                (F.lower(F.col("Category")) == "allocatefootnotedata") &
                (F.lower(F.col("LookUpData")) == "linetypeid")
            )
        )
        has_null_lookup = not allowed_scope.filter(F.col("LookUpValue").isNull()).isEmpty()
        if not has_null_lookup:
            allowed_lt = (
                allowed_scope
                .filter(F.col("LookUpValue").isNotNull())
                .select(F.col("LookUpValue").alias("LineTypeID"))
                .distinct()
            )
            allowed = [r.LineTypeID for r in allowed_lt.collect()]
            result = result.filter(F.col("LineTypeID").isin(allowed))

    log_timing("apply_blocker_entity_cleanup", t0)
    return result


def apply_distribution_line_suppression(
    spark: SparkSession, cfg: dict, allocation_input_df: DataFrame,
) -> DataFrame:
    """For domestic entities, delete K1 distribution lines from non-local entities.

    SQL lines: 6749-6760. Uses udfGetDistributionLines to get distribution LineIDs,
    then deletes K1 rows for those lines where EntityID != current entity.
    """
    log_section("apply_distribution_line_suppression")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    is_foreign = cfg.get("is_foreign_entity", False)
    k1_lt = cfg.get("k1_line_type_id")

    if is_foreign or not k1_lt:
        log_timing("apply_distribution_line_suppression", t0)
        return allocation_input_df

    # Inline udfGetDistributionLines: two branches based on GlobalMenu config
    has_config = cfg.get("has_k1_line_item_config", False)
    k1_line_item_df = spark.table("_k1_line_item")

    if has_config:
        # Branch 1: PFICClassType distribution lines + offset lines via ContributionLineWithAttributes
        contrib_df = read_table(spark, "ContributionLineWithAttributes", cfg)
        pfic_dist_k1 = (
            k1_line_item_df
            .filter(F.lower(F.col("PFICClassType")) == "pfic distribution")
            .select("LineID")
        )
        # Direct PFIC Distribution lines with no offset
        base_lines = (
            contrib_df.alias("C")
            .join(pfic_dist_k1.alias("K"), F.col("C.K1LineID") == F.col("K.LineID"), "inner")
            .filter(F.col("C.Offset").isNull())
            .select(F.col("C.K1LineID").alias("LineID"))
        )
        # Offset lines matched via ContributionLineWithAttributes self-join.
        base_attrs = (
            contrib_df.alias("C")
            .join(
                base_lines.select("LineID").distinct().alias("D"),
                F.col("C.K1LineID") == F.col("D.LineID"),
                "inner"
            )
            .select(
                F.col("C.ContributionLineID"),
                F.col("C.ParentLineID"), F.col("C.Source"), F.col("C.Waterfall"),
                F.col("C.TransactionDate"), F.col("C.FN"), F.col("C.ContributionLineClassification")
            )
        )
        offset_lines = (
            base_attrs.alias("D")
            .join(
                contrib_df.alias("C2"),
                (F.col("D.ContributionLineID") == F.col("C2.ContributionLineID")) &
                (F.coalesce(F.col("D.ParentLineID"), F.lit(0)) == F.coalesce(F.col("C2.ParentLineID"), F.lit(0))) &
                (F.coalesce(F.col("D.Waterfall"), F.lit("")) == F.coalesce(F.col("C2.Waterfall"), F.lit(""))) &
                (F.coalesce(F.col("D.Source"), F.lit("")) == F.coalesce(F.col("C2.Source"), F.lit(""))) &
                (F.coalesce(F.col("D.TransactionDate"), F.lit("")) == F.coalesce(F.col("C2.TransactionDate"), F.lit(""))) &
                (F.coalesce(F.col("D.FN"), F.lit("")) == F.coalesce(F.col("C2.FN"), F.lit(""))) &
                (F.coalesce(F.col("D.ContributionLineClassification"), F.lit("")) == F.coalesce(F.col("C2.ContributionLineClassification"), F.lit(""))),
                "inner"
            )
            .filter(F.col("C2.Offset").isNotNull())
            .select(F.col("C2.K1LineID").alias("LineID"))
        )
        dist_lines_df = base_lines.unionByName(offset_lines).distinct()
    else:
        # Branch 2: Simple PFICClassType filter
        dist_lines_df = (
            k1_line_item_df
            .filter(F.lower(F.col("PFICClassType")) == "pfic distribution")
            .select("LineID")
        )

    # Filter out rows where EntityID != current AND LineTypeID == K1 AND LineID in dist_lines
    # Use left_anti join approach (more efficient than .subtract())
    rows_to_remove = (
        allocation_input_df
        .filter(
            (F.col("EntityID") != entity_id) &
            (F.col("LineTypeID") == k1_lt)
        )
        .join(F.broadcast(dist_lines_df), "LineID", "inner")
        .select(*allocation_input_df.columns)
    )

    result = allocation_input_df.join(
        rows_to_remove.select(
            F.col("EntityID").alias("_eid"),
            F.col("LineTypeID").alias("_ltid"),
            F.col("LineID").alias("_lid"),
            F.col("QuicklinkID").alias("_qlid"),
            F.col("TrackingKey").alias("_tk"),
        ),
        (F.col("EntityID") == F.col("_eid")) &
        (F.col("LineTypeID") == F.col("_ltid")) &
        (F.col("LineID") == F.col("_lid")) &
        (F.coalesce(F.col("QuicklinkID"), F.lit(0)) == F.coalesce(F.col("_qlid"), F.lit(0))) &
        (F.coalesce(F.col("TrackingKey"), F.lit("")) == F.coalesce(F.col("_tk"), F.lit(""))),
        "left_anti"
    )

    log_timing("apply_distribution_line_suppression", t0)
    return result


def apply_tag_percentages(
    spark: SparkSession,
    cfg: dict,
    allocation_input_df: DataFrame,
) -> DataFrame:
    """Apply tag percentage allocation with plug-to-largest rounding.

    If InvestmentTAGWorkflowID is set:
    1. Join AllocationInput with TagPercentage_Snapshot
    2. Multiply amounts by percentage
    3. Round and plug diff to largest-percentage tag
    4. Replace original rows with tagged rows

    SQL lines: 6900-7180
    Returns: Updated AllocationInput DataFrame
    """
    log_section("apply_tag_percentages")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    tag_wf_id = cfg.get("investment_tag_workflow_id", 0)

    if not tag_wf_id or tag_wf_id == 0:
        log_timing("apply_tag_percentages", t0)
        return allocation_input_df

    # Pre-filter TagPercentage_Snapshot to only this workflow (small, broadcastable)
    tag_pct_df = F.broadcast(
        read_table(spark, "TagPercentage_Snapshot", cfg)
        .filter(F.col("WorkflowID") == tag_wf_id)
        .select("UnderlyingID", "LineID", "Source", "FootnoteID", "Percentage", "Tag")
    )

    # Step 1: Join with TagPercentage_Snapshot (line-level matches first, then wildcard)
    # Part A: Specific line matches (T.LineID != -1)
    tag_specific = tag_pct_df.filter(F.col("LineID") != -1)
    tag_wildcard = tag_pct_df.filter(F.col("LineID") == -1)

    tagged_specific = (
        allocation_input_df.alias("A")
        .join(
            tag_specific.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineID") == F.col("T.LineID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))),
            "inner"
        )
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.EntityID"), F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.lit(None).cast("string").alias("TransactionName"),
            F.lit(None).cast("int").alias("TransactionEntityID"),
            F.col("A.QuicklinkID"), F.col("A.CategoryID"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.lit(None).cast("string").alias("LineCode"),
            F.col("A.ParentEntityID"), F.lit(0).cast("int").alias("AdjustmentTypeID"),
            F.col("T.Tag"),
            F.lit(None).cast("string").alias("TrackingKey"),
            F.lit(None).cast("int").alias("SchID"),
            F.col("A.OriginalParentEntityID"),
        )
    )

    # Part B: Wildcard matches (T.LineID == -1) where no specific match exists
    # First get the keys that have specific matches to exclude them
    has_specific = (
        allocation_input_df.alias("A")
        .join(
            tag_specific.alias("T1"),
            (F.col("A.EntityID") == F.col("T1.UnderlyingID")) &
            (F.col("A.LineID") == F.col("T1.LineID")) &
            (F.col("A.LineTypeID") == F.col("T1.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T1.FootnoteID"), F.lit(0))),
            "inner"
        )
        .select(
            F.col("A.EntityID").alias("_eid"),
            F.col("A.LineID").alias("_lid"),
            F.col("A.LineTypeID").alias("_ltid"),
            F.col("A.QuicklinkID").alias("_qlid"),
        )
        .distinct()
    )

    tagged_wildcard = (
        allocation_input_df.alias("A")
        .join(
            tag_wildcard.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))),
            "inner"
        )
        .join(
            has_specific,
            (F.col("A.EntityID") == F.col("_eid")) &
            (F.col("A.LineID") == F.col("_lid")) &
            (F.col("A.LineTypeID") == F.col("_ltid")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("_qlid"), F.lit(0))),
            "left_anti"
        )
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.EntityID"), F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.lit(None).cast("string").alias("TransactionName"),
            F.lit(None).cast("int").alias("TransactionEntityID"),
            F.col("A.QuicklinkID"), F.col("A.CategoryID"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.lit(None).cast("string").alias("LineCode"),
            F.col("A.ParentEntityID"), F.lit(0).cast("int").alias("AdjustmentTypeID"),
            F.col("T.Tag"),
            F.lit(None).cast("string").alias("TrackingKey"),
            F.lit(None).cast("int").alias("SchID"),
            F.col("A.OriginalParentEntityID"),
        )
    )

    tagged = tagged_specific.unionByName(tagged_wildcard)

    # Step 2+3: Compute rounding difference per group and plug to highest-percentage tag
    partition_cols = ["SuperParentEntityID", "ParentEntityID", "EntityID", "LineTypeID", "LineID", "QuicklinkID"]
    grp_window = Window.partitionBy(*partition_cols)
    rank_window = Window.partitionBy(*partition_cols).orderBy(
        F.col("Percentage").desc(),
        F.col("SuperParentEntityID").asc(), F.col("ParentEntityID").asc(),
        F.col("EntityID").asc(), F.col("LineTypeID").asc(),
        F.col("LineID").asc(), F.col("QuicklinkID").asc(), F.col("Tag").asc()
    )

    tagged_ranked = tagged.withColumn("rnk", F.rank().over(rank_window))

    plugged = (
        tagged_ranked
        .withColumn("grp_unrounded", F.sum("Unrounded").over(grp_window))
        .withColumn("grp_rounded", F.sum("Amount").over(grp_window))
        .withColumn(
            "Amount",
            F.when(
                F.col("rnk") == 1,
                F.col("Amount") + F.round(F.col("grp_unrounded") - F.col("grp_rounded"), 0).cast("bigint")
            ).otherwise(F.col("Amount"))
        )
        .select(
            "SuperParentEntityID", "EntityID", "LineTypeID", "LineID", "Amount",
            "TransactionName", "TransactionEntityID",
            "QuicklinkID", "CategoryID", "PeriodID", "LineCode",
            "ParentEntityID", "AdjustmentTypeID", "Tag",
            "TrackingKey", "SchID", "OriginalParentEntityID"
        )
    )

    # Step 4: Remove original tagged rows from AllocationInput, add plugged rows
    # Use left_anti to find untagged rows
    tagged_keys = (
        tagged
        .select(
            F.col("EntityID"), F.col("LineID"), F.col("LineTypeID"),
            F.coalesce(F.col("QuicklinkID"), F.lit(0)).alias("_qlid_key")
        )
        .distinct()
    )

    _left = allocation_input_df.withColumn("_qlid_key", F.coalesce(F.col("QuicklinkID"), F.lit(0)))
    untagged = (
        _left.alias("ORIG")
        .join(
            tagged_keys.alias("TK"),
            (F.col("ORIG.EntityID") == F.col("TK.EntityID")) &
            (F.col("ORIG.LineID") == F.col("TK.LineID")) &
            (F.col("ORIG.LineTypeID") == F.col("TK.LineTypeID")) &
            (F.col("ORIG._qlid_key") == F.col("TK._qlid_key")),
            "left_anti"
        )
        .select(
            F.col("ORIG.SuperParentEntityID"), F.col("ORIG.EntityID"), F.col("ORIG.LineTypeID"), F.col("ORIG.LineID"), F.col("ORIG.Amount"),
            F.lit(None).cast("string").alias("TransactionName"),
            F.lit(None).cast("int").alias("TransactionEntityID"),
            F.col("ORIG.QuicklinkID"), F.col("ORIG.CategoryID"),
            F.lit(None).cast("int").alias("PeriodID"),
            F.lit(None).cast("string").alias("LineCode"),
            F.col("ORIG.ParentEntityID"), F.lit(0).cast("int").alias("AdjustmentTypeID"),
            F.lit(None).cast("string").alias("Tag"),
            F.col("ORIG.TrackingKey"), F.col("ORIG.SchID"), F.col("ORIG.OriginalParentEntityID"),
        )
    )

    result = untagged.unionByName(plugged, allowMissingColumns=True)
    log_timing("apply_tag_percentages", t0)
    return result


def write_allocation_input(spark: SparkSession, cfg: dict, allocation_input_df: DataFrame) -> None:
    """Final INSERT INTO AllocationInput with GROUP BY aggregation.

    SQL lines: 7180-7250
    Includes entity cleanup logic from SQL lines 6535-6567:
    - PFIC entities: keep K1 + PFIC rows for other entities
    - CFC/QFC entities: keep PFIC rows for other entities
    - All other entities: only keep rows for current entity
    """
    log_section("write_allocation_input")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    entity_id = cfg["entity_id"]
    allocation_type = cfg.get("allocation_type", "")

    # Tag handling
    tag_wf_id = cfg.get("investment_tag_workflow_id", 0)
    tags_active = tag_wf_id and tag_wf_id != 0 and allocation_type != 'Pro Rata'

    # Filter zero amounts
    df = allocation_input_df.filter(F.coalesce(F.col("Amount"), F.lit(0)) != 0)

    # Group-by keys
    group_cols = [
        F.col("EntityID"),
        F.col("LineTypeID"),
        F.col("LineID"),
        F.col("QuicklinkID"),
        F.coalesce(F.col("CategoryID"), F.lit(0)).alias("CategoryID"),
        F.coalesce(F.col("ParentEntityID"), F.lit(0)).alias("ParentEntityID"),
        F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
        F.coalesce(F.col("TrackingKey"), F.lit("")).alias("TrackingKey"),
        F.col("SchID"),
        F.col("OriginalParentEntityID"),
    ]

    if tags_active:
        group_cols.append(F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"))
    else:
        pass  # Tag will be added as literal after groupBy

    # Prepare for groupBy — need string column names
    # Use withColumn to pre-compute coalesced values, then groupBy on column names
    df = (
        df
        .filter(F.coalesce(F.col("Amount"), F.lit(0)) != 0)
        .withColumn("_CategoryID", F.coalesce(F.col("CategoryID"), F.lit(0)))
        .withColumn("_ParentEntityID", F.coalesce(F.col("ParentEntityID"), F.lit(0)))
        .withColumn("_SuperParentEntityID", F.coalesce(F.col("SuperParentEntityID"), F.lit(0)))
        .withColumn("_TrackingKey", F.coalesce(F.col("TrackingKey"), F.lit("")))
        .withColumn("_Tag", F.coalesce(F.col("Tag"), F.lit("")) if tags_active else F.lit(""))
        .withColumn("_LineCode", F.coalesce(F.col("LineCode"), F.lit("")))
        .withColumn("_AdjustmentTypeID", F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)))
    )

    group_by_cols = [
        "EntityID", "LineTypeID", "LineID", "QuicklinkID",
        "_CategoryID", "_ParentEntityID", "_SuperParentEntityID",
        "_TrackingKey", "SchID", "OriginalParentEntityID",
        "PeriodID", "_LineCode", "_AdjustmentTypeID",
    ]
    if tags_active:
        group_by_cols.append("_Tag")

    # Aggregate
    result_df = (
        df.groupBy(*group_by_cols)
        .agg(
            F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount"),
            F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount704b"),
        )
    )

    # Build final select with constants and renames
    result_df = (
        result_df.select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.col("EntityID").cast("int"),
            F.col("LineTypeID").cast("int"),
            F.col("LineID").cast("int"),
            F.col("Amount"),
            F.col("Amount704b"),
            F.col("QuicklinkID").cast("int"),
            F.col("_CategoryID").cast("int").alias("CategoryID"),
            F.col("PeriodID").cast("int").alias("PeriodID"),
            F.col("_LineCode").alias("LineCode"),
            F.col("_ParentEntityID").cast("int").alias("ParentEntityID"),
            F.col("_SuperParentEntityID").cast("int").alias("SuperParentEntityID"),
            F.col("_AdjustmentTypeID").cast("int").alias("AdjustmentTypeID"),
            F.col("_TrackingKey").alias("TrackingKey"),
            (F.col("_Tag") if tags_active else F.lit("")).alias("Tag"),
            F.col("SchID").cast("int"),
            F.col("OriginalParentEntityID").cast("int"),
        )
    )

    # Collect for batch write via GenericResultStorer
    _collect_result(cfg, result_df, "AllocationInput")

    log_timing("write_allocation_input", t0)


def write_pfic_flowup(spark: SparkSession, cfg: dict, pfic_flowup_df: DataFrame) -> None:
    """Write PFICFootnoteFlowup records to both legacy and tracking-key tables.

    SQL lines: 6811-7020
    1. Legacy PFICFootnoteFlowup (without TrackingKey) — grouped by key columns
    2. PFICFootnoteFlowupWithTrackingKey — with #PFICLineItem filter for zero-amount rows
    """
    log_section("write_pfic_flowup")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    is_tracking = cfg.get("is_tracking_key", "C") == "C"

    # ─── Legacy PFICFootnoteFlowup (without TrackingKey) ──────────────────
    # SQL line 6813: INSERT INTO PFICFootnoteFlowup ... GROUP BY (no TrackingKey)
    # DELETE already done in purge_output_tables

    pfic_line_item_df = spark.table("_pfic_line_item").filter(F.col("IsActive") == True)
    pfic_line_item_all = (
        read_table(spark, "PFICFootnoteLineItem", cfg)
        .filter(F.col("IsActive") == True)
        .select("LineID", "IsAllocated")
    )
    pfic_joined = (
        pfic_flowup_df.alias("P")
        .join(
            pfic_line_item_all.alias("L"),
            (F.col("P.LineID") == F.col("L.LineID")),
            "inner"
        )
    )

    group_cols = ["P.RunID", "P.ClientID", "P.TaxPeriodID", "P.EntityID",
                  "P.FlowupEntityID", "P.SourceEntityID", "P.PFICFootnoteID",
                  "P.LineID", "L.IsAllocated"]

    df_pfic = (
        pfic_joined
        .groupBy(
            F.col("P.RunID").alias("RunID"), F.col("P.ClientID").alias("ClientID"),
            F.col("P.TaxPeriodID").alias("TaxPeriodID"), F.col("P.EntityID").alias("EntityID"),
            F.col("P.FlowupEntityID").alias("FlowupEntityID"),
            F.col("P.SourceEntityID").alias("SourceEntityID"),
            F.col("P.PFICFootnoteID"),
            F.col("P.LineID"), F.col("L.IsAllocated")
        )
        .agg(
            F.sum("P.Amount").alias("_sum_amount"),
            F.max("P.Amount").alias("_max_amount"),
            F.max("P.TextValue").alias("TextValue"),
        )
        .withColumn(
            "Amount",
            F.when(F.col("IsAllocated") == False, F.col("_max_amount"))
            .otherwise(F.col("_sum_amount"))
        )
        .select("RunID", "ClientID", "TaxPeriodID", "EntityID", "FlowupEntityID",
                "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "TextValue")
    )
    _collect_result(cfg, df_pfic, "PFICFootnoteFlowup")

    # ─── PFICFootnoteFlowupWithTrackingKey ────────────────────────────────
    # SQL line 6929: Build #PFICFootnoteFlowupFinal with TrackingKey + '~' + EntityID
    # SQL line 6943: Filter through #PFICLineItem for zero rows, pass all non-zero
    # DELETE already done in purge_output_tables

    # Build PFICLineItem filter: lines in ENU_DF_DataList 'PFICFootnoteFlowUpLines'
    enu_df = read_table(spark, "ENU_DF_DataList", cfg)
    pfic_filter_lines = (
        spark.table("_pfic_line_item").alias("FL")
        .join(
            enu_df.alias("EL"),
            (F.col("EL.LookUpData") == F.col("FL.ShortName")) &
            (F.lower(F.col("EL.Category")) == "pficfootnoteflowuplines") &
            (F.col("EL.LookUpValue") == "1"),
            "inner"
        )
        .select(F.col("FL.LineID"))
        .distinct()
    )

    # Group with TrackingKey included
    pfic_tk_grouped = (
        pfic_flowup_df.alias("P")
        .join(
            pfic_line_item_all.alias("L"),
            (F.col("P.LineID") == F.col("L.LineID")),
            "inner"
        )
        .groupBy(
            F.col("P.RunID").alias("RunID"), F.col("P.ClientID").alias("ClientID"),
            F.col("P.TaxPeriodID").alias("TaxPeriodID"), F.col("P.EntityID").alias("EntityID"),
            F.col("P.FlowupEntityID").alias("FlowupEntityID"),
            F.col("P.SourceEntityID").alias("SourceEntityID"),
            F.col("P.PFICFootnoteID"),
            F.col("P.LineID"), F.col("P.TrackingKey"), F.col("L.IsAllocated")
        )
        .agg(
            F.sum("P.Amount").alias("_sum_amount"),
            F.max("P.Amount").alias("_max_amount"),
            F.max("P.TextValue").alias("TextValue"),
        )
        .withColumn(
            "Amount",
            F.when(F.col("IsAllocated") == False, F.col("_max_amount"))
            .otherwise(F.col("_sum_amount"))
        )
        .withColumn("TrackingKey", F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id))))
        .select("RunID", "ClientID", "TaxPeriodID", "EntityID", "FlowupEntityID",
                "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "TextValue", "TrackingKey")
    )

    # Filter: keep non-zero rows OR rows whose LineID is in the filter list
    df_pfic_tk = (
        pfic_tk_grouped.alias("P")
        .join(pfic_filter_lines.alias("F"), F.col("P.LineID") == F.col("F.LineID"), "left")
        .filter(
            (F.coalesce(F.col("P.Amount"), F.lit(0)) != 0) |
            (F.coalesce(F.col("P.TextValue"), F.lit("")) != "") |
            F.col("F.LineID").isNotNull()
        )
        .select(
            F.col("P.RunID"), F.col("P.ClientID"), F.col("P.TaxPeriodID"),
            F.col("P.EntityID"), F.col("P.FlowupEntityID"),
            F.col("P.SourceEntityID"), F.col("P.PFICFootnoteID"),
            F.col("P.LineID"), F.col("P.Amount"), F.col("P.TextValue"), F.col("P.TrackingKey")
        )
    )
    _collect_result(cfg, df_pfic_tk, "PFICFootnoteFlowupWithTrackingKey")

    log_timing("write_pfic_flowup", t0)


def write_form_flowups(spark: SparkSession, cfg: dict) -> None:
    """Write form flowup data to Form926Flowup, Form199AFlowup, Form8865Flowup,
    Form8886Flowup, CustomFootnoteFlowup, Form200616Flowup, AtRiskFlowup tables.

    SQL lines: 3900-5400. Each form type has:
    1. Direct flowup from snapshot (current entity as FlowupEntityID)
    2. ReclassFootnoteAllocationData aggregation (lower-tier flowup)
    3. Non-allocated line pass-through from existing flowup table

    This function handles all form-type flowup persistence in one pass.
    """
    log_section("write_form_flowups")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    fx_tid = cfg.get("fx_rate_transaction_id") or 0

    form926_lt = cfg.get("form926_line_type_id")
    form199a_lt = cfg.get("form199a_line_type_id")
    form8886_lt = cfg.get("form8886_line_type_id")
    form8865_lt = cfg.get("form8865_line_type_id")
    at_risk_lt = cfg.get("at_risk_line_type_id")
    line_9a_after = cfg.get("line_9a_after_line_id", 0)
    line_9a_before = cfg.get("line_9a_before_line_id", 0)

    # Register UnBlockedFootnotes view for non-allocated pass-through INSERTs
    # SQL line 1833: SELECT DISTINCT ... INTO #UnBlockedFootnotes FROM ReclassFootnoteAllocationData
    lt_type_ids_list = [x for x in [form926_lt, form8886_lt, form199a_lt] if x]
    reclass_data_df = spark.table("_reclass_data")
    if lt_type_ids_list:
        unblocked_footnotes_df = (
            reclass_data_df
            .filter(F.col("LineTypeID").isin(lt_type_ids_list))
            .select("EntityID", "SourceEntityID", "FootnoteID", "LineTypeID",
                    "ParentEntityID", "LTEntityID", "TrackingKey", "OriginalParentEntityID")
            .distinct()
        )
    # Helper DataFrames used across form flowups
    aiw_df = spark.table("_aiw")
    k1_wf_df = spark.table(f"_k1_workflow_{run_id}")
    entity_df = spark.table("_entity")
    fx_avg_rate_df = spark.table("_fx_avg_rate")
    lower_tier_df = spark.table(f"_lower_tier_funds_{run_id}")

    # ─── Form 926 Flowup ──────────────────────────────────────────────────
    if form926_lt:
        f926_snapshot = read_table(spark, "Form926Input_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        form926_transfer_date_line_id = cfg.get("form926_transfer_date_line_id", 0)
        has_transfer_date = (
            form926_transfer_date_line_id is not None
            and form926_transfer_date_line_id != 0
            and not read_table(spark, "Form926LineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id)
                & (F.col("TaxPeriodID") == tax_period_id)
                & (F.lower(F.col("ShortName")) == "transferdate")
                & (F.col("IsActive") == True)
            )
            .isEmpty()
        )

        # Build transfer date values per Form926ID.
        if has_transfer_date:
            f926_date_values = (
                f926_snapshot.alias("F926dv")
                .filter(F.col("F926dv.LineID") == form926_transfer_date_line_id)
                .join(k1_wf_df.alias("AIWdv"),
                      F.col("F926dv.WorkflowID") == F.col("AIWdv.WorkflowID"), "left_semi")
                .select(F.col("F926dv.Form926ID"),
                        F.col("F926dv.TextValue").alias("TransferDateValue"),
                        F.col("F926dv.WorkflowID"))
            )
        else:
            f926_date_values = None

        alloc_run_for_fx = read_table(spark, "AllocationRun", cfg)
        run_type = cfg.get("run_type")
        phase_id = cfg.get("phase_id")
        max_success_fx_row = (
            alloc_run_for_fx
            .filter(
                (F.col("EntityID") == entity_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.upper(F.col("RunStatus")) == "SUCCESS") &
                (F.col("RunType") == run_type) &
                (F.col("PhaseID") == phase_id)
            )
            .agg(F.max("RunID").alias("MaxRunID"))
            .first()
        )
        if max_success_fx_row and max_success_fx_row["MaxRunID"]:
            fx_run_row = (
                alloc_run_for_fx
                .filter(F.col("RunID") == max_success_fx_row["MaxRunID"])
                .select("ForeignCurrencyRateTransactionID")
                .first()
            )
            spot_fx_tid = fx_run_row["ForeignCurrencyRateTransactionID"] if fx_run_row else fx_tid
        else:
            spot_fx_tid = fx_tid

        fx_rate_tbl = read_table(spark, "ForeignCurrencyRate", cfg)

        # Join base flowup with date info
        base_f926 = (
            f926_snapshot.alias("F926")
            .join(k1_wf_df.alias("AIW"), F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("AIW.EntityID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
        )

        if f926_date_values is not None:
            any_various = not (
                f926_date_values
                .filter(F.lower(F.col("TransferDateValue")) == "various")
                .isEmpty()
            )

            if any_various:
                # 'Various' present anywhere → AverageRate for all rows.
                amount_expr = F.when(
                    F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
                ).otherwise(
                    F.round(F.col("F926.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                )
            else:
                # No 'Various' → date-based spot rate for all rows.
                date_value_concrete = (
                    f926_date_values
                    .filter(
                        (F.lower(F.col("TransferDateValue")) != "various")
                        & (F.coalesce(F.col("TransferDateValue"), F.lit("")) != "")
                    )
                    .groupBy(F.col("Form926ID"))
                    .agg(F.max(F.col("TransferDateValue")).alias("DateValue"))
                )
                # Form926Package / K1Package act as existence filters in the SQL spot-rate
                # branch. Filter by client/tax period (safe: Form926ID and K1PackageID are
                # unique to one client/tax period) to enable Delta data skipping.
                f926_package = (
                    read_table(spark, "Form926Package", cfg)
                    .filter(
                        (F.col("ClientID") == client_id)
                        & (F.col("TaxPeriodID") == tax_period_id)
                    )
                    .select(F.col("Form926ID"), F.col("K1PackageID"))
                )
                k1_package = (
                    read_table(spark, "K1Package", cfg)
                    .filter(F.col("TaxPeriodID") == tax_period_id)
                    .select(F.col("K1PackageID"))
                    .distinct()
                )
                spot_rate = (
                    fx_rate_tbl
                    .filter((F.col("TransactionID") == spot_fx_tid) & (F.col("ClientID") == client_id))
                    .groupBy("ClientID", "CurrencyCode", "TransactionID", "Range")
                    .agg(F.first("Rate", ignorenulls=True).alias("Rate"))
                )
                base_f926 = (
                    base_f926
                    .join(date_value_concrete.alias("DVC"),
                          F.col("DVC.Form926ID") == F.col("F926.Form926ID"), "inner")
                    .join(f926_package.alias("PKG"),
                          F.col("PKG.Form926ID") == F.col("F926.Form926ID"), "inner")
                    .join(k1_package.alias("K1P"),
                          F.col("K1P.K1PackageID") == F.col("PKG.K1PackageID"), "inner")
                    .join(
                        spot_rate.alias("SR"),
                        (F.col("SR.ClientID") == client_id) &
                        (F.col("SR.CurrencyCode") == F.col("E.CurrencyCode")) &
                        (F.col("SR.TransactionID") == spot_fx_tid) &
                        (F.col("SR.Range") == F.col("DVC.DateValue").cast("date")),
                        "left"
                    )
                )
                amount_expr = F.when(
                    F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
                ).otherwise(
                    F.round(F.col("F926.Amount") / F.when(
                        (F.upper(F.col("E.CurrencyCode")) == "USD") | (F.coalesce(F.col("E.CurrencyCode"), F.lit("")) == ""),
                        F.lit(1)
                    ).otherwise(F.coalesce(F.col("SR.Rate"), F.lit(1))), 0)
                )
        else:
            amount_expr = F.when(
                F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
            ).otherwise(
                F.round(F.col("F926.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
            )

        df = base_f926.select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.lit(entity_id).cast("int").alias("FlowupEntityID"),
            F.col("AIW.EntityID").alias("SourceEntityID"),
            F.col("F926.Form926ID"), F.col("F926.LineID"),
            amount_expr.alias("Amount"),
            F.col("F926.TextValue"),
        )
        _collect_result(cfg, df, "Form926Flowup")

        # Reclass flowup
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form926_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form926ID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "Form926Flowup")

        # Non-allocated pass-through from existing lower-tier flowup
        f926_line_item = (
            read_table(spark, "Form926LineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
                ((F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()) &
                (F.col("IsActive") == True)
            )
        )
        f926_flowup = prune_to_lower_tier_runs(
            read_table(spark, "Form926Flowup", cfg), spark, cfg
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_df = unblocked_footnotes_df.filter(F.col("LineTypeID") == form926_lt) if lt_type_ids_list else None
        if ub_df is not None:
            # Build suffix for LIKE match
            df = (
                f926_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f926_line_item.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_df.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form926ID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form926ID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form926Flowup")

    # ─── Form 199A Flowup ─────────────────────────────────────────────────
    if form199a_lt:
        f199a_snapshot = read_table(spark, "Form199AInput_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f199a_line_item = read_table(spark, "Form199ALineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) & (F.col("IsActive") == True)
        )
        df = (
            f199a_snapshot.alias("F199A")
            .join(k1_wf_df.alias("KW"), F.col("F199A.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f199a_line_item.alias("FL"), F.col("F199A.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F199A.Form199AID"), F.col("F199A.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F199A.Amount"))
                .otherwise(F.round(F.col("F199A.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F199A.TextValue"),
            )
        )
        _collect_result(cfg, df, "Form199AFlowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form199a_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form199AID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "Form199AFlowup")

        # Non-allocated pass-through
        f199a_line_item_non_alloc = f199a_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f199a_flowup = prune_to_lower_tier_runs(
            read_table(spark, "Form199AFlowup", cfg), spark, cfg
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_199a = unblocked_footnotes_df.filter(F.col("LineTypeID") == form199a_lt) if lt_type_ids_list else None
        if ub_199a is not None:
            df = (
                f199a_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f199a_line_item_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_199a.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form199AID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form199AID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form199AFlowup")

    # ─── Form 8865 Flowup ─────────────────────────────────────────────────
    if form8865_lt:
        f8865_snapshot = read_table(spark, "Form8865Input_Snapshot", cfg).withColumn(
            "Amount", F.expr("try_cast(Amount as double)")
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8865_line_item = read_table(spark, "Form8865LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) & (F.col("IsActive") == True)
        )
        # Direct from snapshot
        df = (
            f8865_snapshot.alias("F8865")
            .join(k1_wf_df.alias("KW"), F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f8865_line_item.alias("FL"), F.col("F8865.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F8865.Form8865ID"), F.col("F8865.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F8865.TextValue"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # SchA from Form8865SchInput_Snapshot
        f8865_sch_snapshot = read_table(spark, "Form8865SchInput_Snapshot", cfg).withColumn(
            "Amount", F.expr("try_cast(Amount as double)")
        )
        f8865_sch_package = read_table(spark, "Form8865SchPackage", cfg)
        f8865_line_all = read_table(spark, "Form8865LineItem", cfg).filter(
            (F.col("IsActive") == True) & (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8865_sch_snapshot.alias("F8865")
            .join(f8865_sch_package.alias("P"), F.col("P.SchID") == F.col("F8865.SchID"), "inner")
            .join(k1_wf_df.alias("KW"), F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f8865_line_all.alias("FL"), F.col("F8865.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("P.Form8865ID"), F.col("F8865.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F8865.TextValue"), F.col("F8865.SchID"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # Reclass from Form8865AllocationSummary
        f8865_alloc_summary = prune_to_lower_tier_runs(
            read_table(spark, "Form8865AllocationSummary", cfg), spark, cfg
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8865_line_alloc = f8865_line_item.filter(F.col("IsAllocated") == True)
        df = (
            f8865_alloc_summary.alias("F8865")
            .join(
                lower_tier_df.alias("LT"),
                (F.col("F8865.RunID") == F.col("LT.RunID")) &
                (F.col("F8865.EntityID") == F.col("LT.EntityID")) &
                (F.col("F8865.PartnerNumber") == F.col("LT.PartnerNumber")),
                "inner"
            )
            .join(f8865_line_alloc.alias("line"), F.col("line.LineID") == F.col("F8865.LineID"), "inner")
            .groupBy(
                F.col("LT.EntityID").alias("FlowupEntityID"),
                F.col("F8865.SourceEntityID"), F.col("F8865.Form8865ID"),
                F.col("F8865.LineID"), F.col("F8865.SchID")
            )
            .agg(F.sum("F8865.FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("FlowupEntityID"), F.col("SourceEntityID"),
                F.col("Form8865ID"), F.col("LineID"), F.col("Amount"),
                F.lit(None).cast("string").alias("TextValue"), F.col("SchID"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # Non-allocated pass-through
        f8865_line_non_alloc = f8865_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f8865_flowup = prune_to_lower_tier_runs(
            read_table(spark, "Form8865Flowup", cfg), spark, cfg
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8865_flowup.alias("fl")
            .join(
                lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                "inner"
            )
            .join(f8865_line_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("ltf.EntityID").alias("FlowupEntityID"),
                F.col("fl.SourceEntityID"), F.col("fl.Form8865ID"), F.col("fl.LineID"),
                F.col("fl.Amount"), F.col("fl.TextValue"), F.col("fl.SchID"),
            )
            .distinct()
        )
        _collect_result(cfg, df, "Form8865Flowup")

    # ─── Form 8886 Flowup ─────────────────────────────────────────────────
    if form8886_lt:
        f8886_snapshot = read_table(spark, "Form8886Input_Snapshot", cfg).withColumn(
            "_NumericAmount", F.expr("try_cast(TextValue as double)")
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8886_line_item = read_table(spark, "Form8886LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8886_snapshot.alias("F8886")
            .join(f8886_line_item.alias("FL"), F.col("F8886.LineID") == F.col("FL.LineID"), "inner")
            .join(k1_wf_df.alias("KW"), F.col("F8886.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F8886.Form8886ID"), F.col("F8886.LineID"),
                F.when(F.col("FL.IsAllocated") == True,
                    F.round(F.col("F8886._NumericAmount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                ).otherwise(
                    F.round(F.col("F8886.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                ).alias("Amount"),
                F.col("F8886.TextValue"), F.col("F8886.TransactionName"),
                F.col("F8886.EntityID").alias("TransactionEntityID"),
                F.col("F8886.Comments"), F.col("F8886.SecIIComments"),
            )
        )
        _collect_result(cfg, df, "Form8886Flowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form8886_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID",
                     "TextValue", "TransactionName", "TransactionEntityID",
                     "Comments", "SecIIComments")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form8886ID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
                F.col("TransactionName"), F.col("TransactionEntityID"),
                F.col("Comments"), F.col("SecIIComments"),
            )
        )
        _collect_result(cfg, df, "Form8886Flowup")

        # Non-allocated pass-through
        f8886_line_non_alloc = f8886_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f8886_flowup = prune_to_lower_tier_runs(
            read_table(spark, "Form8886Flowup", cfg), spark, cfg
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_8886 = unblocked_footnotes_df.filter(F.col("LineTypeID") == form8886_lt) if lt_type_ids_list else None
        if ub_8886 is not None:
            df = (
                f8886_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f8886_line_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_8886.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form8886ID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form8886ID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                    F.col("fl.TransactionName"), F.col("fl.TransactionEntityID"),
                    F.col("fl.Comments"), F.col("fl.SecIIComments"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form8886Flowup")

    # ─── At-Risk Flowup ───────────────────────────────────────────────────
    if at_risk_lt:
        ar_snapshot = read_table(spark, "AtRiskInput_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
            (F.coalesce(F.col("Amount"), F.lit(0)) != 0)
        )
        k1_line_item_tbl = read_table(spark, "K1LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
            (F.upper(F.col("LineDataType")) == "NUMBER") & (F.col("IsActive") == True)
        )
        map_k1 = read_table(spark, "MAP_K1LineItemLineType", cfg).filter(F.col("LineTypeID") == at_risk_lt)
        ar_package = read_table(spark, "AtRiskPackage", cfg)
        k1_package_df = spark.table("_k1_package")

        df = (
            ar_snapshot.alias("AR")
            .join(k1_line_item_tbl.alias("KL"), F.col("KL.LineID") == F.col("AR.LineID"), "inner")
            .join(map_k1.alias("M"), F.col("KL.LineID") == F.col("M.K1LineItemID"), "inner")
            .join(aiw_df.alias("KW"), F.col("AR.WorkflowID") == F.col("KW.ImportAtRiskWorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(ar_package.alias("P"), F.col("P.AtRiskID") == F.col("AR.AtRiskID"), "inner")
            .join(k1_package_df.alias("K1P"), F.col("K1P.K1PackageID") == F.col("P.K1PackageID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("AR.AtRiskID"), F.col("AR.LineID"),
                F.round(F.col("AR.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0).alias("Amount"),
                F.col("AR.TextValue"),
            )
        )
        _collect_result(cfg, df, "AtRiskFlowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == at_risk_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("AtRiskID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "AtRiskFlowup")

    # ─── Custom Footnote Flowup ───────────────────────────────────────────
    cf_input = read_table(spark, "CustomFootnoteInput", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )
    cf_txn_df = spark.table(f"_cf_latest_txn_{run_id}")
    cf_line_item = read_table(spark, "CustomFootnoteLineItem", cfg).filter(
        (F.col("IsActive") == True) & (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )
    cf_line_item = cf_line_item.unionByName(
        spark.createDataFrame([(-1, "TEXT")], "LineID int, LineDataType string"),
        allowMissingColumns=True
    )

    df = (
        cf_input.alias("CF")
        .join(cf_txn_df.alias("TXN"), F.col("CF.CustomFootnoteTransactionID") == F.col("TXN.TransactionID"), "inner")
        .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("TXN.EntityID"), "inner")
        .join(cf_line_item.alias("FL"), F.col("CF.LineID") == F.col("FL.LineID"), "inner")
        .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.lit(entity_id).cast("int").alias("FlowupEntityID"),
            F.col("TXN.EntityID").alias("SourceEntityID"),
            F.col("CF.CustomFootnoteID"), F.col("CF.LineID"),
            F.col("TXN.LineTypeID"),
            F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("CF.Amount"))
            .otherwise(F.round(F.col("CF.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
            .alias("Amount"),
            F.col("CF.TextValue"),
        )
        .distinct()
    )
    _collect_result(cfg, df, "CustomFootnoteFlowup")

    # Pass-through from existing CustomFootnoteFlowup for lower-tier entities
    cff_existing = prune_to_lower_tier_runs(
        read_table(spark, "CustomFootnoteFlowup", cfg), spark, cfg
    )
    df = (
        cff_existing.alias("CFF")
        .join(
            lower_tier_df.alias("LTF"),
            (F.col("LTF.RunID") == F.col("CFF.RunID")) & (F.col("LTF.EntityID") == F.col("CFF.EntityID")),
            "inner"
        )
        .join(cf_line_item.alias("FL"), F.col("FL.LineID") == F.col("CFF.LineID"), "inner")
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("LTF.EntityID").alias("FlowupEntityID"),
            F.col("CFF.SourceEntityID"), F.col("CFF.CustomFootnoteID"),
            F.col("CFF.LineID"), F.col("CFF.LineTypeID"), F.col("CFF.Amount"), F.col("CFF.TextValue"),
        )
        .distinct()
    )
    _collect_result(cfg, df, "CustomFootnoteFlowup")

    # ─── Form 200616 Flowup ───────────────────────────────────────────────
    f2006_snapshot = read_table(spark, "Form200616_Snapshot", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )
    df = (
        f2006_snapshot.alias("F2006")
        .join(k1_wf_df.alias("KW"), F.col("F2006.WorkflowID") == F.col("KW.WorkflowID"), "inner")
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.lit(entity_id).cast("int").alias("FlowupEntityID"),
            F.col("KW.EntityID").alias("SourceEntityID"),
            F.col("F2006.Form2006EntityID"),
        )
    )
    _collect_result(cfg, df, "Form200616Flowup")

    # Reclass from Form200616AllocationSummary
    f2006_alloc = prune_to_lower_tier_runs(
        read_table(spark, "Form200616AllocationSummary", cfg), spark, cfg
    ).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    )
    lt_funds = spark.table(f"_lower_tier_funds_{run_id}")
    df = (
        f2006_alloc.alias("F2006")
        .join(
            lt_funds.alias("LT"),
            (F.col("F2006.RunID") == F.col("LT.RunID")) &
            (F.col("F2006.EntityID") == F.col("LT.EntityID")) &
            (F.col("F2006.PartnerNumber") == F.col("LT.PartnerNumber")),
            "inner"
        )
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("LT.EntityID").alias("FlowupEntityID"),
            F.col("F2006.SourceEntityID"),
            F.col("F2006.Form2006EntityID"),
        )
        .distinct()
    )
    _collect_result(cfg, df, "Form200616Flowup")

    log_timing("write_form_flowups", t0)
