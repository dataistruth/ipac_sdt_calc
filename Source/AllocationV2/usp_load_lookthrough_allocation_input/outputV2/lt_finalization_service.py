"""
finalization_service.py — Sections 15-21: Box JKL, exclusions, tags, validation, final write.

Functions:
    build_box_jkl_input         — Section 15: Box JKL allocation input
    apply_master_feed_exclusion — Section 16: Remove K1 amounts for master feed entities
    apply_blocker_entity        — Section 17: blocker-entity exclusion (PFIC / Corp)
    apply_tag_percentages       — Section 18: Tag percentage splitting with rounding correction
    apply_line_exclusions       — Section 19: Map_Form163J + excluded allocations
    validate_k3_rules           — Section 20: K3 gain/loss validation
    write_final_output          — Section 21: Write to LookThroughAllocationInput + SchKTaxableIncome
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import time

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, sql_round, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, sql_round, log_section, log_timing, logger


# ---------------------------------------------------------------------------
# Section 15: build_box_jkl_input
# SQL lines ~2930-3130
# Only if BoxJKLAllocation = 'Aggregate and Allocate'
# ---------------------------------------------------------------------------

def build_box_jkl_input(spark: SparkSession, cfg: dict, alloc_input_df: DataFrame,
                        fx_rates_df: DataFrame) -> DataFrame:
    """
    Build Box JKL allocation input and union into alloc_input.
    Returns updated alloc_input_df.
    """
    log_section("build_box_jkl_input")
    t0 = time.time()

    box_jkl_alloc = cfg.get("box_jkl_allocation", "")
    if box_jkl_alloc != "Aggregate and Allocate":
        logger.info("BoxJKLAllocation != 'Aggregate and Allocate' — skipping Box JKL.")
        log_timing("build_box_jkl_input", t0)
        return alloc_input_df

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    allocation_type = cfg.get("allocation_type", "")
    foreign_currency_rate_tx_id = cfg.get("foreign_currency_rate_transaction_id")

    # Get BoxJKL LineTypeID
    box_jkl_lt = (
        tbl(spark, "ENU_LineType", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.lower(F.col("LineType")) == "boxjkl")
        )
        .select("LineTypeID")
        .first()
    )
    if box_jkl_lt is None:
        logger.warning("BoxJKL LineTypeID not found — skipping.")
        log_timing("build_box_jkl_input", t0)
        return alloc_input_df
    box_jkl_lt_id = box_jkl_lt["LineTypeID"]

    # Tech config values
    box_jkl_update = cfg.get("box_jkl_logic", "0")
    box_jkl_override = cfg.get("flowup_boi_liabilities", "0")

    # K1 workflows — broadcast (small lookup)
    k1_workflow = F.broadcast(
        tbl(spark, "AllocationInputWorkflow", cfg)
        .filter(
            (F.col("RunID") == run_id) &
            ~(F.coalesce(F.col("K1WorkflowID"), F.lit(0)) == 0)
        )
        .select(
            F.col("K1WorkflowID").alias("WorkflowID"),
            F.col("EntityID"),
        )
    )

    # --- Direct BoxJKL from snapshot ---
    box_jkl_snapshot = tbl(spark, "BoxJKLInput_Snapshot", cfg)
    box_jkl_line_item = F.broadcast(
        tbl(spark, "BoxJKLLineItem", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (
                (F.upper(F.col("Box")) == "K") |
                ((F.lower(F.col("LineDescription")) == "current year increase (decrease)") & (F.lit(box_jkl_update) == "1"))
            )
        )
    )
    entity_tbl = F.broadcast(tbl(spark, "Entity", cfg))
    fx_avg_rate = F.broadcast(
        tbl(spark, "ForeignCurrencyAverageRate", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TransactionID") == foreign_currency_rate_tx_id)
        )
    )

    # Tracking key logic — always on
    if box_jkl_update == "1":
        tracking_key_expr = F.col("KW.EntityID").cast("string")
    else:
        tracking_key_expr = F.when(
            F.col("KW.EntityID") == entity_id,
            F.col("KW.EntityID").cast("string"),
        ).otherwise(F.lit(None).cast("string"))

    direct_jkl = (
        box_jkl_snapshot.alias("JKL")
        .join(box_jkl_line_item.alias("BL"),
              (F.col("JKL.LineID") == F.col("BL.LineID")) &
              (F.col("BL.ClientID") == F.col("JKL.ClientID")))
        .join(k1_workflow.alias("KW"),
              (F.col("JKL.WorkflowID") == F.col("KW.WorkflowID")) &
              (F.col("JKL.ClientID") == client_id) &
              (F.coalesce(F.col("JKL.Amount"), F.lit(0)) != 0))
        .join(entity_tbl.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"))
        .join(fx_avg_rate.alias("R"),
              F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
        .select(
            F.lit(entity_id).alias("ParentEntityID"),
            F.col("KW.EntityID").alias("EntityID"),
            F.lit(box_jkl_lt_id).alias("LineTypeID"),
            F.col("JKL.LineID"),
            F.round(F.col("JKL.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0).alias("Amount"),
            tracking_key_expr.alias("TrackingKey"),
        )
    )

    # --- Flowup BoxJKL from ReclassBoxJKLLookThroughAllocationData ---
    reclass_jkl = (
        tbl(spark, "ReclassBoxJKLLookThroughAllocationData", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.upper(F.col("BoxJKLBox")) == "K") &
            (F.col("RunID") == run_id)
        )
    )

    flowup_jkl = (
        reclass_jkl
        .groupBy("ParentEntityID", "EntityID", "LineID", "SuperParentEntityID",
                 "TrackingKey", F.coalesce(F.col("OriginalParentEntityID"), F.lit(entity_id)).alias("OriginalParentEntityID"))
        .agg(F.sum("FlowupAmount").alias("Amount"))
        .select(
            F.col("ParentEntityID"),
            F.col("EntityID"),
            F.lit(box_jkl_lt_id).alias("LineTypeID"),
            F.col("LineID"),
            F.col("Amount"),
            F.col("TrackingKey"),
            F.col("SuperParentEntityID"),
            F.col("OriginalParentEntityID"),
        )
    )

    # Union into alloc input
    alloc_input_df = alloc_input_df.unionByName(direct_jkl, allowMissingColumns=True)
    alloc_input_df = alloc_input_df.unionByName(flowup_jkl, allowMissingColumns=True)

    # --- Basis Override Import (if configured) ---
    # SQL: EXEC uspIsGlobalMenuConfigured @ClientID, @TaxPeriodID, 'Other Logic/Imports', 'Basis Override Import', @ConfigBasisOverrideImport OUTPUT
    # Requires BOTH GlobalMenu configured AND FlowupBOILiabilities = 1
    config_basis_override = False
    if box_jkl_override == "1":
        gm_group = tbl(spark, "ENU_GlobalMenuGroup", cfg).filter(
            F.lower(F.col("GroupName")) == "other logic/imports"
        ).select("GlobalMenuGroupID").first()
        if gm_group:
            config_basis_override = (
                tbl(spark, "GlobalMenu", cfg)
                .filter(
                    (F.col("GlobalMenuGroupID") == gm_group["GlobalMenuGroupID"]) &
                    (F.lower(F.col("MenuName")) == "basis override import") &
                    (F.col("ClientID") == client_id) &
                    (F.col("TaxPeriodID") == tax_period_id) &
                    (F.lower(F.col("State")).isin("c", "cg"))
                )
                .select(F.lit(1)).first() is not None
            )

    if config_basis_override:
        basis_override = tbl(spark, "BasisOverrideImportData", cfg).filter(
            F.col("UpperTierEntityID") == entity_id
        )
        if basis_override.select(F.lit(1)).first() is not None:
            # Update existing amounts
            alloc_input_df = (
                alloc_input_df.alias("I")
                .join(
                    basis_override.alias("B"),
                    (F.col("I.LineID") == F.col("B.LineID")) &
                    (F.col("I.EntityID") == F.col("B.LowerTierEntityID")) &
                    (F.col("I.LineTypeID") == F.col("B.LineTypeId").cast("int")),
                    "left",
                )
                .select(
                    F.col("I.*"),
                    F.when(F.col("B.Value").isNotNull(), F.col("B.Value").cast("double"))
                     .otherwise(F.col("I.Amount")).alias("_new_amount"),
                )
                .withColumn("Amount", F.col("_new_amount"))
                .drop("_new_amount")
            )

            # Insert new lines not already present
            new_lines = (
                basis_override.alias("B")
                .join(
                    alloc_input_df.alias("I"),
                    (F.col("I.LineID") == F.col("B.LineID")) &
                    (F.col("I.EntityID") == F.col("B.LowerTierEntityID")) &
                    (F.col("I.LineTypeID") == F.col("B.LineTypeId").cast("int")),
                    "left_anti",
                )
                .select(
                    F.col("B.UpperTierEntityID").alias("ParentEntityID"),
                    F.col("B.LowerTierEntityID").alias("EntityID"),
                    F.col("B.LineTypeId").cast("int").alias("LineTypeID"),
                    F.col("B.LineID"),
                    F.col("B.Value").cast("double").alias("Amount"),
                    F.col("B.LowerTierEntityID").cast("string").alias("TrackingKey"),
                )
            )
            alloc_input_df = alloc_input_df.unionByName(new_lines, allowMissingColumns=True)

    log_timing("build_box_jkl_input", t0)
    return alloc_input_df


# ---------------------------------------------------------------------------
# Section 16: apply_master_feed_exclusion
# SQL lines ~3140-3170
# ---------------------------------------------------------------------------

def apply_master_feed_exclusion(spark: SparkSession, cfg: dict,
                                alloc_input_df: DataFrame) -> DataFrame:
    """
    Remove K1 amounts for master feed entities if configured.
    Returns updated alloc_input_df.
    """
    log_section("apply_master_feed_exclusion")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_lt = cfg["k1_line_type_id"]

    # Check if global menu is configured
    global_menu = tbl(spark, "GlobalMenu", cfg)
    menu_group = tbl(spark, "ENU_GlobalMenuGroup", cfg)

    is_configured = (
        global_menu.alias("G")
        .join(menu_group.alias("EN"), F.col("G.GlobalMenuGroupID") == F.col("EN.GlobalMenuGroupID"))
        .filter(
            (F.lower(F.col("EN.GroupName")) == "other configuration") &
            (F.lower(F.col("G.MenuName")) == "run prorata with master feed alloc") &
            (F.upper(F.col("G.State")) == "C")
        )
        .limit(1)
        .count() > 0
    )

    if not is_configured:
        logger.info("Master feed exclusion not configured — skipping.")
        log_timing("apply_master_feed_exclusion", t0)
        return alloc_input_df

    # Inline uspGetMasterFeedEntityInvs: queries MasterImportEntityFeed + Entity
    master_feed_entities = spark.sql(f"""
        SELECT DISTINCT
            T.EntityId AS EntityID,
            COALESCE(E.EntityID, T.EntityId) AS LowerTierEntityInvID,
            T.Source AS SourceType
        FROM {tbl_name('MasterImportEntityFeed', cfg)} T
        LEFT JOIN {tbl_name('Entity', cfg)} E
            ON T.InvestmentIdentification = E.EntityIdentification
        WHERE T.EntityId = {entity_id}
          AND T.ClientID = {client_id}
          AND T.TaxPeriodID = {tax_period_id}
    """)

    k1_source_entities = (
        master_feed_entities
        .filter(F.lower(F.col("SourceType")) == "k-1 input")
        .select("LowerTierEntityInvID")
    )

    # Delete K1 amounts for those entities
    if k1_source_entities.select(F.lit(1)).first() is not None:
        alloc_input_df = alloc_input_df.join(
            k1_source_entities,
            (alloc_input_df["EntityID"] == k1_source_entities["LowerTierEntityInvID"]) &
            (alloc_input_df["LineTypeID"] == k1_lt),
            "left_anti",
        )

    log_timing("apply_master_feed_exclusion", t0)
    return alloc_input_df


# ---------------------------------------------------------------------------
# Section 17: apply_blocker_entity
# SQL lines ~3175-3230
# ---------------------------------------------------------------------------

def apply_blocker_entity(spark: SparkSession, cfg: dict,
                      alloc_input_df: DataFrame) -> DataFrame:
    """
    Apply blocker-entity logic: if entity is PFIC corporation, keep only PFIC K1 lines;
    if not PFIC, remove all non-self entity lines.
    Returns updated alloc_input_df.
    """
    log_section("apply_blocker_entity")
    t0 = time.time()

    is_blocker_entity = cfg.get("is_blocker_entity", 0)
    if not is_blocker_entity:
        log_timing("apply_blocker_entity", t0)
        return alloc_input_df

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    k1_lt = cfg["k1_line_type_id"]

    # Check if entity is PFIC
    entity_row = (
        tbl(spark, "Entity", cfg)
        .filter(F.col("EntityID") == entity_id)
        .select("IsPFIC")
        .first()
    )
    is_pfic = bool(entity_row and entity_row["IsPFIC"])

    if is_pfic:
        # Keep only K1 lines, and only PFIC class types for non-self entities
        pfic_class_types = ["PFIC ORDINARY", "PFIC GAIN", "PFIC DISTRIBUTION", "PFIC GAIN LONGTERM"]

        k1_line_item = tbl(spark, "K1LineItem", cfg).filter(
            F.upper(F.col("PFICClassType")).isin(*pfic_class_types)
        ).select("LineID")

        # Remove non-K1 lines for non-self entities
        alloc_input_df = alloc_input_df.filter(
            (F.col("EntityID") == entity_id) | (F.col("LineTypeID") == k1_lt)
        )

        # Remove K1 lines that aren't PFIC types for non-self entities
        alloc_input_df = alloc_input_df.filter(
            (F.col("EntityID") == entity_id) |
            F.col("LineID").isin(
                [r["LineID"] for r in k1_line_item.collect()]
            )
        )

        # Remove PFIC Distribution lines for non-self entities where underlying entity is PFIC
        pfic_dist_lines = tbl(spark, "K1LineItem", cfg).filter(
            F.lower(F.col("PFICClassType")) == "pfic distribution"
        ).select("LineID")

        pfic_entities = tbl(spark, "Entity", cfg).filter(
            F.col("IsPFIC") == True
        ).select("EntityID")

        alloc_input_df = alloc_input_df.join(
            pfic_dist_lines.crossJoin(pfic_entities).alias("PD"),
            (alloc_input_df["LineID"] == F.col("PD.LineID")) &
            (alloc_input_df["EntityID"] == F.col("PD.EntityID")) &
            (alloc_input_df["LineTypeID"] == k1_lt) &
            (alloc_input_df["EntityID"] != entity_id),
            "left_anti",
        )
    else:
        # Not PFIC — remove all non-self entity lines
        alloc_input_df = alloc_input_df.filter(F.col("EntityID") == entity_id)

    log_timing("apply_blocker_entity", t0)
    return alloc_input_df


# ---------------------------------------------------------------------------
# Section 18: apply_tag_percentages
# SQL lines ~3240-3500
# ---------------------------------------------------------------------------

def apply_tag_percentages(spark: SparkSession, cfg: dict,
                          alloc_input_df: DataFrame,
                          reclass_k1_df: DataFrame) -> DataFrame:
    """
    Apply tag percentage splitting to alloc_input.
    Splits amounts by tag percentages, applies rounding correction,
    then replaces original lines with tagged lines.
    Returns updated alloc_input_df.
    """
    log_section("apply_tag_percentages")
    t0 = time.time()

    inv_tag_wf_id = cfg.get("investment_tag_workflow_id", 0)
    if not inv_tag_wf_id:
        logger.info("No InvestmentTAGWorkflowID — skipping tag percentages.")
        log_timing("apply_tag_percentages", t0)
        return alloc_input_df

    entity_id = cfg["entity_id"]
    lookthrough_reclass_wf_id = cfg.get("lookthrough_reclass_workflow_id", 0)

    # Load TagPercentage_Snapshot
    tag_pct = tbl(spark, "TagPercentage_Snapshot", cfg).filter(
        F.col("WorkflowID") == inv_tag_wf_id
    )

    # Load LookthroughReclass_Snapshot
    lt_reclass = tbl(spark, "LookthroughReclass_Snapshot", cfg).filter(
        F.col("WorkflowID") == lookthrough_reclass_wf_id
    )

    # Identify excluded reclass amounts (source line has tags, dest does not)
    exclude_reclass = (
        reclass_k1_df.alias("R")
        .join(
            lt_reclass.alias("LR"),
            (F.col("R.LineID") == F.col("LR.ReclassLineID")) &
            (F.coalesce(F.col("R.TrackingKey"), F.lit("")) == F.coalesce(F.col("LR.TrackingKey"), F.lit(""))) &
            (F.col("R.EntityID") == F.col("LR.UnderlyingEntityID")) &
            (F.col("R.ParentEntityID") == F.col("LR.ParentEntityID")),
        )
        .join(
            tag_pct.alias("T"),
            F.when(
                F.coalesce(F.col("T.LineID"), F.lit("")).cast("string") == "-1",
                F.coalesce(F.col("T.LineID"), F.lit("")).cast("string"),
            ).otherwise(
                F.coalesce(F.col("LR.LineID"), F.lit("")).cast("string")
            ) == F.coalesce(F.col("T.LineID"), F.lit("")).cast("string"),
            "left",
        )
        .filter(
            F.col("T.LineID").isNull() &
            (F.coalesce(F.col("R.Tag"), F.lit("")) != "")
        )
        .select(
            F.col("LR.EntityID"),
            F.col("LR.UnderlyingEntityID"),
            F.col("LR.ReclassLineID"),
            F.col("LR.TrackingKey"),
            F.col("LR.ParentEntityID"),
            F.col("R.Tag"),
        )
    )

    # --- Tag split pass 1: exact line match ---
    tagged_1 = (
        alloc_input_df.alias("A")
        .join(
            tag_pct.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineID") == F.col("T.LineID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("T.TrackingKey"), F.lit(""))),
        )
        .join(
            exclude_reclass.alias("D"),
            (F.col("A.LineID") == F.col("D.ReclassLineID")) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.TrackingKey"), F.lit(""))) &
            (F.col("A.EntityID") == F.col("D.UnderlyingEntityID")) &
            (F.coalesce(F.col("A.Tag"), F.lit("")) == F.coalesce(F.col("D.Tag"), F.lit(""))),
            "left_anti",
        )
        .filter((F.col("T.WorkflowID") == inv_tag_wf_id) & (F.col("T.LineID") != -1))
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.ParentEntityID"), F.col("A.EntityID"),
            F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.col("A.TrackingKey"), F.col("T.Tag"),
            F.col("A.OriginalParentEntityID"), F.col("A.LTEntityID"),
            F.col("A.QuicklinkID"),
        )
    )

    # --- Tag split pass 2: LineID = -1, specific TrackingKey (catchall by line type) ---
    tagged_2 = (
        alloc_input_df.alias("A")
        .join(
            tag_pct.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineID") == F.col("T.LineID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))),
        )
        .join(
            exclude_reclass.alias("D"),
            (F.col("A.LineID") == F.col("D.ReclassLineID")) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.TrackingKey"), F.lit(""))) &
            (F.col("A.EntityID") == F.col("D.UnderlyingEntityID")) &
            (F.coalesce(F.col("A.Tag"), F.lit("")) == F.coalesce(F.col("D.Tag"), F.lit(""))),
            "left_anti",
        )
        .join(
            tagged_1.alias("T1"),
            (F.col("A.EntityID") == F.col("T1.EntityID")) &
            (F.col("A.LineID") == F.col("T1.LineID")) &
            (F.col("A.LineTypeID") == F.col("T1.LineTypeID")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T1.QuicklinkID"), F.lit(0))) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("T1.TrackingKey"), F.lit(""))),
            "left_anti",
        )
        .filter(
            (F.col("T.WorkflowID") == inv_tag_wf_id) &
            (F.col("T.LineID") != -1) &
            (F.coalesce(F.col("T.TrackingKey"), F.lit("")) == "-1")
        )
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.ParentEntityID"), F.col("A.EntityID"),
            F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.col("A.TrackingKey"), F.col("T.Tag"),
            F.col("A.OriginalParentEntityID"), F.col("A.LTEntityID"),
            F.col("A.QuicklinkID"),
        )
    )

    # --- Tag split pass 3: LineID = -1, specific TrackingKey match ---
    tagged_3 = (
        alloc_input_df.alias("A")
        .join(
            tag_pct.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("T.TrackingKey"), F.lit(""))),
        )
        .join(
            exclude_reclass.alias("D"),
            (F.col("A.LineID") == F.col("D.ReclassLineID")) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.TrackingKey"), F.lit(""))) &
            (F.col("A.EntityID") == F.col("D.UnderlyingEntityID")) &
            (F.coalesce(F.col("A.Tag"), F.lit("")) == F.coalesce(F.col("D.Tag"), F.lit(""))),
            "left_anti",
        )
        .join(
            tagged_1.unionByName(tagged_2, allowMissingColumns=True).alias("T1"),
            (F.col("A.EntityID") == F.col("T1.EntityID")) &
            (F.col("A.LineID") == F.col("T1.LineID")) &
            (F.col("A.LineTypeID") == F.col("T1.LineTypeID")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T1.QuicklinkID"), F.lit(0))) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("T1.TrackingKey"), F.lit(""))),
            "left_anti",
        )
        .filter((F.col("T.WorkflowID") == inv_tag_wf_id) & (F.col("T.LineID") == -1))
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.ParentEntityID"), F.col("A.EntityID"),
            F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.col("A.TrackingKey"), F.col("T.Tag"),
            F.col("A.OriginalParentEntityID"), F.col("A.LTEntityID"),
            F.col("A.QuicklinkID"),
        )
    )

    # --- Tag split pass 4: LineID = -1, TrackingKey = '-1' (wildcard) ---
    all_tagged_so_far = tagged_1.unionByName(tagged_2, allowMissingColumns=True).unionByName(tagged_3, allowMissingColumns=True)

    tagged_4 = (
        alloc_input_df.alias("A")
        .join(
            tag_pct.alias("T"),
            (F.col("A.EntityID") == F.col("T.UnderlyingID")) &
            (F.col("A.LineTypeID") == F.col("T.Source")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T.FootnoteID"), F.lit(0))),
        )
        .join(
            exclude_reclass.alias("D"),
            (F.col("A.LineID") == F.col("D.ReclassLineID")) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.TrackingKey"), F.lit(""))) &
            (F.col("A.EntityID") == F.col("D.UnderlyingEntityID")) &
            (F.coalesce(F.col("A.Tag"), F.lit("")) == F.coalesce(F.col("D.Tag"), F.lit(""))),
            "left_anti",
        )
        .join(
            all_tagged_so_far.alias("T1"),
            (F.col("A.EntityID") == F.col("T1.EntityID")) &
            (F.col("A.LineID") == F.col("T1.LineID")) &
            (F.col("A.LineTypeID") == F.col("T1.LineTypeID")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("T1.QuicklinkID"), F.lit(0))) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("T1.TrackingKey"), F.lit(""))),
            "left_anti",
        )
        .filter(
            (F.col("T.WorkflowID") == inv_tag_wf_id) &
            (F.col("T.LineID") == -1) &
            (F.coalesce(F.col("T.TrackingKey"), F.lit("")) == "-1")
        )
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.ParentEntityID"), F.col("A.EntityID"),
            F.col("A.LineTypeID"), F.col("A.LineID"),
            F.round(F.col("A.Amount") * F.col("T.Percentage"), 0).alias("Amount"),
            (F.col("A.Amount") * F.col("T.Percentage")).alias("Unrounded"),
            F.col("T.Percentage"),
            F.col("A.TrackingKey"), F.col("T.Tag"),
            F.col("A.OriginalParentEntityID"), F.col("A.LTEntityID"),
            F.col("A.QuicklinkID"),
        )
    )

    # Combine all tagged passes
    all_tagged = all_tagged_so_far.unionByName(tagged_4, allowMissingColumns=True)

    # --- Rounding correction: plug difference into highest-percentage tag ---
    round_diff = (
        all_tagged
        .groupBy("TrackingKey", "SuperParentEntityID", "ParentEntityID", "EntityID",
                 "LineTypeID", "LineID", "QuicklinkID", "LTEntityID")
        .agg(
            (F.round(F.sum(F.coalesce(F.col("Unrounded"), F.lit(0))), 0) -
             F.sum(F.coalesce(F.col("Amount"), F.lit(0)))).alias("RoundDiff")
        )
    )

    # Rank by highest percentage
    w = Window.partitionBy(
        "TrackingKey", "SuperParentEntityID", "ParentEntityID", "EntityID",
        "LTEntityID", "LineTypeID", "LineID", "QuicklinkID"
    ).orderBy(F.col("Percentage").desc(), F.col("Tag").asc())

    ranked = all_tagged.withColumn("Rnk", F.row_number().over(w))

    # Apply plug to rank 1
    plugged = (
        ranked.alias("A")
        .join(
            round_diff.alias("D"),
            (F.coalesce(F.col("A.SuperParentEntityID"), F.lit(0)) == F.coalesce(F.col("D.SuperParentEntityID"), F.lit(0))) &
            (F.coalesce(F.col("A.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("D.ParentEntityID"), F.lit(0))) &
            (F.col("A.EntityID") == F.col("D.EntityID")) &
            (F.col("A.LineTypeID") == F.col("D.LineTypeID")) &
            (F.coalesce(F.col("A.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.TrackingKey"), F.lit(""))) &
            (F.col("A.LineID") == F.col("D.LineID")) &
            (F.coalesce(F.col("A.QuicklinkID"), F.lit(0)) == F.coalesce(F.col("D.QuicklinkID"), F.lit(0))),
            "left",
        )
        .withColumn(
            "Amount",
            F.when(F.col("Rnk") == 1, F.col("A.Amount") + F.coalesce(F.col("D.RoundDiff"), F.lit(0)))
             .otherwise(F.col("A.Amount"))
        )
        .select(
            F.col("A.SuperParentEntityID"), F.col("A.ParentEntityID"), F.col("A.EntityID"),
            F.col("A.LineTypeID"), F.col("A.LineID"), F.col("Amount"),
            F.col("A.TrackingKey"), F.col("A.Tag"),
            F.col("A.OriginalParentEntityID"), F.col("A.LTEntityID"),
            F.col("A.QuicklinkID"),
        )
    )

    # Remove original lines that were tagged, insert tagged lines
    alloc_input_df = alloc_input_df.join(
        all_tagged.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey").distinct().alias("T"),
        (alloc_input_df["EntityID"] == F.col("T.EntityID")) &
        (alloc_input_df["LineID"] == F.col("T.LineID")) &
        (alloc_input_df["LineTypeID"] == F.col("T.LineTypeID")) &
        (F.coalesce(alloc_input_df["QuicklinkID"], F.lit(0)) == F.coalesce(F.col("T.QuicklinkID"), F.lit(0))) &
        (F.coalesce(alloc_input_df["TrackingKey"], F.lit("")) == F.coalesce(F.col("T.TrackingKey"), F.lit(""))),
        "left_anti",
    )

    alloc_input_df = alloc_input_df.unionByName(plugged, allowMissingColumns=True)

    log_timing("apply_tag_percentages", t0)
    return alloc_input_df


# ---------------------------------------------------------------------------
# Section 19: apply_line_exclusions
# SQL lines ~3530-3570
# ---------------------------------------------------------------------------

def apply_line_exclusions(spark: SparkSession, cfg: dict,
                          alloc_input_df: DataFrame) -> DataFrame:
    """
    Remove Map_Form163J lines and excluded allocation lines.
    Returns updated alloc_input_df.
    """
    log_section("apply_line_exclusions")
    t0 = time.time()

    k1_lt = cfg["k1_line_type_id"]
    m1_lt = cfg.get("m1_line_type_id")

    line_type_filter = [k1_lt]
    if m1_lt:
        line_type_filter.append(m1_lt)

    # Delete Map_Form163J lines
    map_163j = tbl(spark, "Map_Form163J", cfg).select("OriginalAmountLineID")

    alloc_input_df = alloc_input_df.join(
        map_163j,
        (alloc_input_df["LineID"] == map_163j["OriginalAmountLineID"]) &
        alloc_input_df["LineTypeID"].isin(*line_type_filter),
        "left_anti",
    )

    # Delete excluded allocation lines (ENU_DF_DATALIST)
    excluded_lines = (
        tbl(spark, "ENU_DF_DATALIST", cfg)
        .filter(
            (F.lower(F.col("Category")) == "exclude-allocations") &
            (F.col("LookupValue") == "1")
        )
        .select("LookUpData")
    )

    k1_line_item = (
        tbl(spark, "K1LineItem", cfg)
        .withColumn(
            "ExclusionKey",
            F.concat_ws("-", F.col("LineNumber"), F.col("Box"), F.col("LineDescription"))
        )
    )

    excluded_line_ids = (
        k1_line_item.alias("M")
        .join(excluded_lines.alias("T"), F.col("M.ExclusionKey") == F.col("T.LookUpData"))
        .select(F.col("M.LineID"))
    )

    alloc_input_df = alloc_input_df.join(
        excluded_line_ids,
        (alloc_input_df["LineID"] == excluded_line_ids["LineID"]) &
        alloc_input_df["LineTypeID"].isin(*line_type_filter),
        "left_anti",
    )

    log_timing("apply_line_exclusions", t0)
    return alloc_input_df


# ---------------------------------------------------------------------------
# Section 20: validate_k3_rules
# SQL lines ~3575-3700
# ---------------------------------------------------------------------------

def validate_k3_rules(spark: SparkSession, cfg: dict,
                      alloc_input_df: DataFrame) -> str:
    """
    Validate K3 gain/loss rules. Writes errors to AllocationRunErrors.
    Returns 'SUCCESS', 'WARNING', or 'FAIL'.
    """
    log_section("validate_k3_rules")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    log_id = cfg.get("log_id")

    # Check if any K1 lines exist
    has_k1 = alloc_input_df.filter(F.col("LineTypeID") == k1_lt).select(F.lit(1)).first() is not None
    if not has_k1:
        log_timing("validate_k3_rules", t0)
        return "SUCCESS"

    # Aggregate K1 amounts by line
    k1_totals = (
        alloc_input_df
        .filter(F.col("LineTypeID") == k1_lt)
        .groupBy("LineID", "LineTypeID")
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount"))
    )

    # Join with validation rules — broadcast (small lookups)
    k1_line_item = F.broadcast(tbl(spark, "K1LineItem", cfg))
    validation_rules = F.broadcast(tbl(spark, "Enu_K1K3ValidationRule", cfg))

    errors = (
        k1_totals.alias("KA")
        .join(k1_line_item.alias("KL"), F.col("KL.LineID") == F.col("KA.LineID"))
        .join(validation_rules.alias("KR"), F.col("KL.K1K3ValidationRule") == F.col("KR.K1K3ValidationRuleID"))
        .filter(
            ((F.lower(F.col("KR.RuleDescription")) == "gain") & (F.col("KA.Amount") < 0)) |
            ((F.lower(F.col("KR.RuleDescription")) == "loss") & (F.col("KA.Amount") > 0))
        )
        .filter(F.lower(F.col("KL.TypeK1K3VldCalc")).isin("error", "warning"))
        .select(
            F.col("KA.LineID"),
            F.col("KA.LineTypeID"),
            F.when(
                (F.lower(F.col("KR.RuleDescription")) == "gain") & (F.col("KA.Amount") < 0),
                F.concat(
                    F.col("KL.LineNumber"), F.lit(" - "), F.col("KL.Box"), F.lit(" - "),
                    F.col("KL.LineDescription"), F.lit(" should be positive but currently it is "),
                    F.col("KA.Amount").cast("decimal(38,0)").cast("string"),
                )
            ).otherwise(
                F.concat(
                    F.col("KL.LineNumber"), F.lit(" - "), F.col("KL.Box"), F.lit(" - "),
                    F.col("KL.LineDescription"), F.lit(" should be negative but currently it is "),
                    F.col("KA.Amount").cast("decimal(38,0)").cast("string"),
                )
            ).alias("ErrorMessage"),
            F.col("KL.TypeK1K3VldCalc").alias("ErrororWarning"),
        )
    )

    # Write errors to AllocationRunErrors
    if errors.select(F.lit(1)).first() is not None:
        error_rows = (
            errors
            .withColumn("RunID", F.lit(run_id))
            .withColumn("EntityID", F.lit(entity_id))
            .withColumn("LogID", F.lit(log_id))
            .withColumn("ErrorType", F.lit("K3 Validations"))
            .select("RunID", "EntityID", "LineID", "LineTypeID", "ErrorMessage", "LogID", "ErrororWarning", "ErrorType")
        )

        # Align types to target Delta table schema
        target_schema = spark.table(tbl_name("AllocationRunErrors", cfg)).schema
        target_fields = {f.name: f.dataType for f in target_schema.fields}
        for col_name in error_rows.columns:
            if col_name in target_fields:
                error_rows = error_rows.withColumn(col_name, F.col(col_name).cast(target_fields[col_name]))

        error_rows.write.mode("append").saveAsTable(tbl_name("AllocationRunErrors", cfg))

        # Check if any are actual errors (not just warnings)
        has_errors = errors.filter(F.lower(F.col("ErrororWarning")) == "error").select(F.lit(1)).first() is not None
        if has_errors:
            # Update run status to FAIL (equivalent to EXEC uspUpdateAllocationRunStatus ... 'FAIL')
            logger.error("K3 Validations failed — hard errors found.")
            _update_allocation_run_status(spark, cfg, "FAIL", "K3 Validations failed")
            _update_k3_status(spark, cfg, "FAIL")
            log_timing("validate_k3_rules", t0)
            return "FAIL"
        else:
            _update_k3_status(spark, cfg, "WARNING")
            log_timing("validate_k3_rules", t0)
            return "WARNING"
    else:
        _update_k3_status(spark, cfg, "SUCCESS")
        log_timing("validate_k3_rules", t0)
        return "SUCCESS"


def _update_k3_status(spark: SparkSession, cfg: dict, status: str):
    """Update K3ValidationStatus on AllocationRun."""
    run_id = cfg["run_id"]
    spark.sql(f"""
        UPDATE {tbl_name('AllocationRun', cfg)}
        SET K3ValidationStatus = '{status}'
        WHERE RunID = {run_id}
    """)


def _update_allocation_run_status(spark: SparkSession, cfg: dict, status: str, message: str):
    """Update RunStatus on AllocationRun (equivalent to EXEC uspUpdateAllocationRunStatus)."""
    run_id = cfg["run_id"]
    spark.sql(f"""
        UPDATE {tbl_name('AllocationRun', cfg)}
        SET RunStatus = '{status}', RunEndDate = current_timestamp()
        WHERE RunID = {run_id}
    """)


# ---------------------------------------------------------------------------
# Section 21: write_final_output
# SQL lines ~3715-3750
# ---------------------------------------------------------------------------

def write_final_output(spark: SparkSession, cfg: dict,
                       alloc_input_df: DataFrame,
                       lower_tier_funds_df: DataFrame,
                       pfic_income_attr_df: DataFrame = None) -> None:
    """
    Write final output to LookThroughAllocationInput and SchKTaxableIncome.
    Also writes PFICtoK1IncomeAttributePercentages if available.
    """
    log_section("write_final_output")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    allocation_type = cfg.get("allocation_type", "")

    # --- Write LookThroughAllocationInput ---
    final_input = (
        alloc_input_df
        .filter(F.coalesce(F.col("Amount"), F.lit(0)) != 0)
        .groupBy(
            "ParentEntityID", "EntityID", "LineTypeID", "LineID",
            "QuicklinkID",
            F.coalesce(F.col("CategoryID"), F.lit(0)).alias("CategoryID"),
            "PeriodID",
            F.coalesce(F.col("LineCode"), F.lit("")).alias("LineCode"),
            F.coalesce(F.col("SuperParentEntityID"), F.lit(0)).alias("SuperParentEntityID"),
            F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
            "TrackingKey",
            F.when(
                F.lower(F.lit(allocation_type)) == "pro rata", F.lit("")
            ).otherwise(F.coalesce(F.col("Tag"), F.lit(""))).alias("Tag"),
            "OriginalParentEntityID",
        )
        .agg(
            F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount"),
        )
        .withColumn("RunID", F.lit(run_id).cast("bigint"))
        .withColumn("ClientID", F.lit(client_id).cast("bigint"))
        .withColumn("Amount704b", F.col("Amount"))
    )

    # Align schema to target table before writing
    target_schema = spark.table(tbl_name("LookThroughAllocationInput", cfg)).schema
    for field in target_schema:
        if field.name in final_input.columns:
            final_input = final_input.withColumn(field.name, F.col(field.name).cast(field.dataType))

    final_input.write.mode("append").saveAsTable(tbl_name("LookThroughAllocationInput", cfg))
    logger.info(f"Wrote {final_input.count()} rows to LookThroughAllocationInput.")

    # --- Write SchKTaxableIncome ---
    k1_line_item = tbl(spark, "K1LineItem", cfg)
    distinct_lt_funds = lower_tier_funds_df.select("RunID", "EntityID").distinct()

    sch_k_taxable = (
        alloc_input_df.alias("LI")
        .filter(F.col("LI.LineTypeID") == k1_lt)
        .join(k1_line_item.alias("KL"), F.col("LI.LineID") == F.col("KL.LineID"))
        .join(distinct_lt_funds.alias("LT"), F.col("LT.EntityID") == F.col("LI.LTEntityID"))
        .groupBy(F.col("LI.LTEntityID"), F.col("LT.RunID"))
        .agg(
            F.sum(
                F.when(F.lower(F.col("KL.TaxableIncomeRule")) == "subtract", -1 * F.col("LI.Amount"))
                 .when(F.lower(F.col("KL.TaxableIncomeRule")) == "add", F.col("LI.Amount"))
                 .otherwise(F.lit(0))
            ).alias("TaxableIncome")
        )
        .select(
            F.lit(run_id).alias("UpperTierRunID"),
            F.lit(entity_id).alias("UpperTierEntityID"),
            F.col("LT.RunID").alias("LowerTierRunID"),
            F.col("LI.LTEntityID").alias("LowerTierEntityID"),
            F.col("TaxableIncome"),
        )
    )

    sch_k_taxable.write.mode("append").saveAsTable(tbl_name("SchKTaxableIncome", cfg))
    logger.info(f"Wrote SchKTaxableIncome rows.")

    # --- Write PFICtoK1IncomeAttributePercentages if available ---
    if pfic_income_attr_df is not None:
        pfic_income_attr_df.write.mode("append").saveAsTable(
            tbl_name("PFICtoK1IncomeAttributePercentages", cfg)
        )
        logger.info("Wrote PFICtoK1IncomeAttributePercentages.")

    log_timing("write_final_output", t0)
