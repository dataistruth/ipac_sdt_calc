"""
cost_percentage.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Cost percentage snapshot building — including UDF inlining and 704c logic.
Conversion date: 2026-05-04

SQL lines: 1905-2400
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

logger = logging.getLogger(__name__)


def _tbl(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    return spark.table(f"{cfg['catalog']}.{cfg['schema']}.{name}")


def _log_timing(name, start):
    logger.info(f"[TIMING] {name}: {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# build_cost_percentage_function (inlines udfGetCostPercentageDetails)
# SQL lines: 1907-1918 + UDF body
# Row count: ALWAYS-NON-EMPTY for modes 1,2,3
# ---------------------------------------------------------------------------
def _build_cost_percentage_function(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Inline dbo.udfGetCostPercentageDetails TVF.

    Reads CostPercentage_Snapshot, validates investments via Entity/Enu_AssetClass,
    handles DealID-based percentages via entity hierarchy + Custom10.
    """
    t0 = time.time()
    logger.info("[SECTION] _build_cost_percentage_function")

    workflow_id = cfg["cost_percentage_workflow_id"]

    # Step 1: Base CostPercentage_Snapshot read
    base = (
        _tbl(spark, "CostPercentage_Snapshot", cfg).alias("C")
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg)).alias("E1"),
            F.col("C.EntityID") == F.col("E1.EntityID"),
        )
        .filter(F.col("C.WorkFlowID") == workflow_id)
        .select(
            F.col("C.WorkFlowID"), F.col("C.TransactionID"),
            F.col("C.ClientID"), F.col("C.TaxPeriodID"),
            F.col("C.EntityID"), F.col("C.InvestmentID"),
            F.col("C.PartnerNumber"), F.col("C.Quarter"),
            F.col("C.CommitmentPercent"), F.col("C.AllocationTypeID"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(F.col("C.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("C.UnderlyingType"), F.col("C.AllocatedAmount"),
            F.col("C.CostPercentageID"), F.col("C.DealID"),
        )
        .distinct()
    )

    enu_ut = F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg))

    # Non-Asset-Class with InvestmentID = -1
    part_inv_neg1 = (
        base.alias("C")
        .join(enu_ut.alias("EU"), F.col("C.UnderlyingType") == F.col("EU.UnderlyingTypeID"))
        .filter((F.col("EU.UnderlyingType") != "ASSET CLASS") & (F.col("C.InvestmentID") == -1))
        .select(
            "C.WorkFlowID", "C.TransactionID", "C.ClientID", "C.TaxPeriodID",
            "C.EntityID", "C.InvestmentID", "C.PartnerNumber", "C.Quarter",
            "C.CommitmentPercent", "C.AllocationTypeID", "C.Tag", "C.TrackingKey",
            "C.UnderlyingType", "C.AllocatedAmount", "C.CostPercentageID",
        )
        .distinct()
    )

    # Non-Asset-Class with InvestmentID NOT IN (-1, -2) — validate via Entity
    part_inv_valid = (
        base.alias("C")
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg)).alias("E2"),
            F.col("C.InvestmentID") == F.col("E2.EntityID"),
        )
        .join(enu_ut.alias("EU"), F.col("C.UnderlyingType") == F.col("EU.UnderlyingTypeID"))
        .filter(
            (F.col("EU.UnderlyingType") != "ASSET CLASS")
            & (~F.col("C.InvestmentID").isin([-1, -2]))
        )
        .select(
            "C.WorkFlowID", "C.TransactionID", "C.ClientID", "C.TaxPeriodID",
            "C.EntityID", "C.InvestmentID", "C.PartnerNumber", "C.Quarter",
            "C.CommitmentPercent", "C.AllocationTypeID", "C.Tag", "C.TrackingKey",
            "C.UnderlyingType", "C.AllocatedAmount", "C.CostPercentageID",
        )
        .distinct()
    )

    # Asset Class — validate via Enu_AssetClass
    part_asset_class = (
        base.alias("C")
        .join(enu_ut.alias("EU"), F.col("C.UnderlyingType") == F.col("EU.UnderlyingTypeID"))
        .join(
            F.broadcast(_tbl(spark, "Enu_AssetClass", cfg)).alias("EA"),
            F.col("C.InvestmentID") == F.col("EA.AssetClassID"),
        )
        .filter(F.col("EU.UnderlyingType") == "ASSET CLASS")
        .select(
            "C.WorkFlowID", "C.TransactionID", "C.ClientID", "C.TaxPeriodID",
            "C.EntityID", "C.InvestmentID", "C.PartnerNumber", "C.Quarter",
            "C.CommitmentPercent", "C.AllocationTypeID", "C.Tag", "C.TrackingKey",
            "C.UnderlyingType", "C.AllocatedAmount", "C.CostPercentageID",
        )
        .distinct()
    )

    # Deal-based percentages (InvestmentID = -2, non-Asset Class)
    pre_filtered = (
        base.alias("C")
        .join(enu_ut.alias("EU"), F.col("C.UnderlyingType") == F.col("EU.UnderlyingTypeID"))
        .filter(
            (F.col("C.InvestmentID") == -2)
            & (F.col("EU.UnderlyingType") != "ASSET CLASS")
        )
        .select("C.*")
    )

    # Build entity hierarchy for deal matching
    selected_entities = (
        base
        .filter(F.coalesce(F.col("DealID"), F.lit("")) != "")
        .select("EntityID")
        .distinct()
    )

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    er = (
        _tbl(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
    )

    entity_deals = (
        selected_entities.alias("T")
        .join(
            er.alias("ER"),
            F.col("ER.UpperTierEntityID") == F.col("T.EntityID"),
        )
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg)).alias("V"),
            F.col("ER.LowerTierEntityID") == F.col("V.EntityID"),
        )
        .filter(F.coalesce(F.col("V.Custom10"), F.lit("")) != "")
        .select(
            F.col("T.EntityID").alias("EntityID"),
            F.col("ER.UpperTierEntityID"),
            F.col("ER.LowerTierEntityID"),
            F.col("V.Custom10"),
        )
    )

    # Distinct records for checking if investment-level % already exists
    base_distinct = (
        base
        .filter(
            (F.coalesce(F.col("DealID"), F.lit("")) == "")
            & (F.col("InvestmentID").isNull())
        )
        .select("EntityID", "InvestmentID", "Quarter", "AllocationTypeID",
                F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
                F.coalesce(F.col("TrackingKey"), F.lit("")).alias("TrackingKey"),
                "UnderlyingType")
        .distinct()
    )

    # Deal-based expansion
    deal_based = (
        pre_filtered.alias("C")
        .join(
            entity_deals.alias("E2"),
            (F.col("C.DealID") == F.col("E2.Custom10"))
            & (F.col("C.EntityID") == F.col("E2.EntityID")),
        )
        .join(
            base_distinct.alias("C2"),
            (F.col("E2.LowerTierEntityID") == F.col("C2.InvestmentID"))
            & (F.col("C.AllocationTypeID") == F.col("C2.AllocationTypeID"))
            & (F.col("C.UnderlyingType") == F.col("C2.UnderlyingType"))
            & (F.col("C.Quarter") == F.col("C2.Quarter"))
            & (F.col("C.Tag") == F.col("C2.Tag"))
            & (F.col("C.TrackingKey") == F.col("C2.TrackingKey")),
            "left_anti",
        )
        .select(
            F.col("C.WorkFlowID"), F.col("C.TransactionID"),
            F.col("C.ClientID"), F.col("C.TaxPeriodID"),
            F.col("E2.UpperTierEntityID").alias("EntityID"),
            F.col("E2.LowerTierEntityID").alias("InvestmentID"),
            F.col("C.PartnerNumber"), F.col("C.Quarter"),
            F.col("C.CommitmentPercent"), F.col("C.AllocationTypeID"),
            F.col("C.Tag"), F.col("C.TrackingKey"),
            F.col("C.UnderlyingType"), F.col("C.AllocatedAmount"),
            F.col("C.CostPercentageID"),
        )
        .distinct()
    )

    result = (
        part_inv_neg1
        .unionByName(part_inv_valid)
        .unionByName(part_asset_class)
        .unionByName(deal_based)
    )

    _log_timing("_build_cost_percentage_function", t0)
    return result


# ---------------------------------------------------------------------------
# build_cost_percentage_snapshot (modes 1,2,3)
# SQL lines: 1905-1935
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_cost_percentage_snapshot_modes123(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Build #CostPercentage_Snapshot for modes 1,2,3.

    Converted from SQL lines 1905-1935.
    Joins cost percentage function result with ENU_UnderlyingType,
    excludes rows that have 704c entries.
    """
    t0 = time.time()
    logger.info("[SECTION] build_cost_percentage_snapshot_modes123")

    workflow_id = cfg["cost_percentage_workflow_id"]

    cost_fn = _build_cost_percentage_function(spark, cfg)

    # Join with ENU_UnderlyingType and exclude rows that have 704c entries
    cost_fn_filtered = (
        cost_fn.alias("C")
        .filter(F.col("C.WorkFlowID") == workflow_id)
        .join(
            _tbl(spark, "CostPercentage_704c_Snapshot", cfg).alias("CP704"),
            (F.col("C.WorkFlowID") == F.col("CP704.WorkFlowID"))
            & (F.col("C.CostPercentageID") == F.col("CP704.CostPercentageID")),
            "left_anti",
        )
    )

    result = (
        cost_fn_filtered.alias("C")
        .join(
            F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg)).alias("U"),
            F.col("C.UnderlyingType") == F.col("U.UnderlyingTypeID"),
        )
        .select(
            F.col("C.WorkFlowID"), F.col("C.TransactionID"),
            F.col("C.ClientID"), F.col("C.TaxPeriodID"),
            F.col("C.EntityID"), F.col("C.InvestmentID"),
            F.col("C.PartnerNumber"), F.col("C.Quarter"),
            F.col("C.CommitmentPercent"), F.col("C.AllocationTypeID"),
            F.col("C.Tag"), F.col("C.TrackingKey"),
            F.col("C.UnderlyingType"), F.col("C.AllocatedAmount"),
            F.col("C.CostPercentageID"),
            F.col("U.UnderlyingType").alias("EntityUnderlyingType"),
            F.lit(None).cast("int").alias("704cAllocationTypeID"),
            F.lit(None).cast("string").alias("704cPercentageType"),
            F.lit(None).cast("boolean").alias("GPPartnerReceivingCarry"),
        )
    )

    if result.isEmpty():
        logger.warning("build_cost_percentage_snapshot_modes123 produced 0 rows")

    _log_timing("build_cost_percentage_snapshot_modes123", t0)
    return result


# ---------------------------------------------------------------------------
# build_mode1_704c_pe_book_allocations
# SQL lines: 1941-2256 (Mode 1 + 704c PE Book custom allocation block)
# Row count: POSSIBLY-EMPTY (returns None when not applicable)
#
# Master-table side effects (intentional — see _persist_704c_custom_allocations):
#   The SQL INSERTs 'Special <Mapped704cField>' rows into ENU_CustomAllocations
#   and ENU_RuleGroup. Downstream reports (e.g. GetFundDataSummaryAll) join
#   ENU_CustomAllocations on AllocationTypeID to resolve the custom-rule names,
#   so these rows MUST be persisted. We mirror SQL's behavior with an
#   idempotent Delta MERGE keyed by AllocationType / RuleGroupName; on first
#   run new rows are inserted with assigned IDs (MAX(existing)+row_number),
#   on subsequent runs the MERGE is a no-op.
#
#   Concurrency: assumes serial execution per (client_id, tax_period_id).
#   Two concurrent runs for the SAME client+period that both observe the same
#   MAX(AllocationTypeID) could race; the SQL Server original has the same
#   risk (LEFT JOIN ... WHERE IS NULL is racy without a unique index).
# ---------------------------------------------------------------------------

# Fixed list of 704c value columns that can appear in the dynamic UNPIVOT.
# Order matches SQL lines 1992-1994 / 2079-2080.
_704C_VALUE_COLUMNS = [
    "TotalMgmtFees",
    "HotIssueGainLoss",
    "704cGainLoss",
    "GuaranteedPaymentsServices",
    "GuaranteedPaymentsCapital",
    "UsWithholding",
    "IncentiveFee",
    "ForeignTaxes",
    "SpecialAllocation1",
    "SpecialAllocation2",
]


def _persist_704c_custom_allocations(
    spark: SparkSession,
    cfg: dict,
    distinct_fields: list,
    client_id: int,
    tax_period_id: int,
):
    """Idempotently register 'Special <field>' rows in ENU_CustomAllocations
    and ENU_RuleGroup so downstream consumers (other SPs, reports) can
    resolve AllocationTypeID / RuleGroupID by name.

    Mirrors SQL lines 2140-2160:
        INSERT INTO ENU_CustomAllocations(AllocationType, ClientID, TaxPeriodID)
        SELECT 'Special ' + MD.Mapped704cField, @LocalClientID, @LocalTaxPeriodID
        FROM #Mapped704cFields MD
        LEFT JOIN ENU_CustomAllocations ET ON 'Special ' + MD.Mapped704cField = ET.AllocationType
        WHERE ET.AllocationType IS NULL
        (and same for ENU_RuleGroup)

    Returns:
        (alloc_type_lookup, rule_group_lookup) — DataFrames with columns
        (Mapped704cField, AllocationTypeID) and (Mapped704cField, RuleGroupID).
    """
    from pyspark.sql import Window

    # Candidate names — broadcast-eligible, small (≤ 10 rows).
    candidates = spark.createDataFrame(
        [(f,) for f in distinct_fields], "Mapped704cField string",
    ).withColumn(
        "AllocationType", F.concat(F.lit("Special "), F.col("Mapped704cField")),
    )

    # --- ENU_CustomAllocations ---------------------------------------------
    ca_tbl = _tbl(spark, "ENU_CustomAllocations", cfg)
    existing_ca = ca_tbl.select("AllocationType", "AllocationTypeID")

    new_ca = (
        candidates.alias("C")
        .join(
            existing_ca.alias("E"),
            F.col("C.AllocationType") == F.col("E.AllocationType"),
            "left_anti",
        )
        .select(F.col("C.AllocationType"), F.col("C.Mapped704cField"))
    )

    if new_ca.head(1):
        max_id_row = existing_ca.agg(
            F.coalesce(F.max("AllocationTypeID"), F.lit(0)).alias("m"),
        ).head(1)
        max_id = max_id_row[0]["m"] if max_id_row else 0

        # row_number() over a deterministic ORDER BY on identical input
        # is stable across re-evaluations, so Delta's two-pass MERGE is safe
        # without an explicit checkpoint. The DataFrame is ≤ len(_704C_VALUE_COLUMNS)
        # rows (max 10) so re-eval cost is negligible.
        new_ca_rows = new_ca.select(
            (F.row_number().over(Window.orderBy("AllocationType")) + F.lit(max_id))
                .cast("int").alias("AllocationTypeID"),
            F.col("AllocationType"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        )

        new_ca_rows.createOrReplaceTempView("_new_704c_custom_alloc")
        ca_fqn = f"{cfg['catalog']}.{cfg['schema']}.ENU_CustomAllocations"
        spark.sql(f"""
            MERGE INTO {ca_fqn} T
            USING _new_704c_custom_alloc S
              ON T.AllocationType = S.AllocationType
            WHEN NOT MATCHED THEN INSERT
              (AllocationTypeID, AllocationType, ClientID, TaxPeriodID)
              VALUES (S.AllocationTypeID, S.AllocationType, S.ClientID, S.TaxPeriodID)
        """)
        logger.info("[704c-PE-Book] MERGE into ENU_CustomAllocations completed")

    # Re-read for lookup
    alloc_type_lookup = (
        candidates.alias("C")
        .join(
            _tbl(spark, "ENU_CustomAllocations", cfg).alias("E"),
            F.col("C.AllocationType") == F.col("E.AllocationType"),
        )
        .select(
            F.col("C.Mapped704cField").alias("Mapped704cField"),
            F.col("E.AllocationTypeID").cast("int").alias("AllocationTypeID"),
        )
    )

    # --- ENU_RuleGroup -----------------------------------------------------
    rg_tbl = _tbl(spark, "ENU_RuleGroup", cfg)
    existing_rg = rg_tbl.select("RuleGroupName", "RuleGroupID")

    new_rg = (
        candidates.alias("C")
        .join(
            existing_rg.alias("E"),
            F.col("C.AllocationType") == F.col("E.RuleGroupName"),
            "left_anti",
        )
        .select(F.col("C.AllocationType").alias("RuleGroupName"),
                F.col("C.Mapped704cField"))
    )

    if new_rg.head(1):
        max_id_row = existing_rg.agg(
            F.coalesce(F.max("RuleGroupID"), F.lit(0)).alias("m"),
        ).head(1)
        max_id = max_id_row[0]["m"] if max_id_row else 0

        new_rg_rows = new_rg.select(
            (F.row_number().over(Window.orderBy("RuleGroupName")) + F.lit(max_id))
                .cast("int").alias("RuleGroupID"),
            F.col("RuleGroupName"),
            F.lit(None).cast("int").alias("DisplayOrder"),
        )

        new_rg_rows.createOrReplaceTempView("_new_704c_rule_group")
        rg_fqn = f"{cfg['catalog']}.{cfg['schema']}.ENU_RuleGroup"
        spark.sql(f"""
            MERGE INTO {rg_fqn} T
            USING _new_704c_rule_group S
              ON T.RuleGroupName = S.RuleGroupName
            WHEN NOT MATCHED THEN INSERT
              (RuleGroupID, RuleGroupName, DisplayOrder)
              VALUES (S.RuleGroupID, S.RuleGroupName, S.DisplayOrder)
        """)
        logger.info("[704c-PE-Book] MERGE into ENU_RuleGroup completed")

    rule_group_lookup = (
        candidates.alias("C")
        .join(
            _tbl(spark, "ENU_RuleGroup", cfg).alias("E"),
            F.col("C.AllocationType") == F.col("E.RuleGroupName"),
        )
        .select(
            F.col("C.Mapped704cField").alias("Mapped704cField"),
            F.col("E.RuleGroupID").cast("int").alias("RuleGroupID"),
        )
    )

    return alloc_type_lookup, rule_group_lookup


def build_mode1_704c_pe_book_allocations(
    spark: SparkSession, cfg: dict, cost_pct_function_df: DataFrame = None,
):
    """Mode 1 + 704c PE Book custom-allocation block.

    Converted from SQL lines 1941-2256.

    Builds:
      - #Mappings           — entity-specific (+ -1 fallback) MapDataRegister rows
      - #CostPercentage704cValues — CostPercentage_Function × 704c snapshot
      - #CostPercentage_Snapshot_UnPivoted / Merged — dynamic UNPIVOT result
      - Custom 'Special <field>' rules (synthetic IDs — see header comment)
      - #MapDefaultAllocRuleToLineItem rows (TransactionID = -2)
      - #DefaultAllocationRuleSetup rows  (TransactionID = -2)
      - Augmenting rows for #CostPercentage_Snapshot

    Returns: dict with keys
        mappings, snapshot_augment, map_dar_704c, dar_setup_704c
      OR None if the block does not apply (wrong mode, no 704c name,
      no mappings, or no 704c values).
    """
    mode = cfg.get("mode")
    _704c_name = cfg.get("_704c_allocation_type_name") or ""
    if mode != 1 or not _704c_name:
        return None

    t0 = time.time()
    logger.info("[SECTION] build_mode1_704c_pe_book_allocations")

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    workflow_id = cfg["cost_percentage_workflow_id"]
    k1_line_type_id = cfg["k1_line_type_id"]

    # ─── Resolve scalar config (RegisterTypeID, 704cSourceID) ─────────────
    register_row = (
        _tbl(spark, "GlobalMenu", cfg)
        .filter(F.col("MenuName") == "704c To K1 Line Mapping")
        .select("GlobalMenuID").head(1)
    )
    if not register_row:
        logger.warning("704c To K1 Line Mapping not found in GlobalMenu — skipping 704c PE-Book block")
        return None
    register_type_id = register_row[0]["GlobalMenuID"]

    src_row = (
        _tbl(spark, "ENU_MappingSource", cfg)
        .filter(F.col("SourceName") == "Tax Allocation Report - 704c")
        .select("SourceID").head(1)
    )
    if not src_row:
        logger.warning("Tax Allocation Report - 704c not found in ENU_MappingSource — skipping 704c PE-Book block")
        return None
    _704c_source_id = src_row[0]["SourceID"]

    # ─── Build #Mappings (entity-specific UNION -1 fallback) ──────────────
    # SQL lines 1956-1986
    map_data_register = _tbl(spark, "MapDataRegister", cfg).alias("MD").filter(
        (F.col("SourceTypeID") == _704c_source_id)
        & (F.col("ClientID") == client_id)
        & (F.col("TaxPeriodID") == tax_period_id)
        & (F.col("RegisterTypeID") == register_type_id)
    )

    mapping_line_item = _tbl(spark, "MappingLineItem", cfg).alias("ML").select(
        "LineID", "ClientID", "TaxPeriodID", "DatabaseName",
    )

    k1_line_item = (
        _tbl(spark, "K1LineItem", cfg).alias("KL")
        .filter(
            (F.col("LineDataType") == "Number")
            & (F.col("IsActive") == 1)
            & (F.col("IsVisible") == 1)
        )
        .select("LineID", "ClientID", "TaxPeriodID")
    )

    vw_entity = _tbl(spark, "Entity", cfg).alias("E").select(
        "EntityID", "ClientID", "TaxPeriodID",
    )

    def _build_mappings(entity_filter_expr):
        return (
            map_data_register.filter(entity_filter_expr).alias("MD")
            .join(
                F.broadcast(mapping_line_item).alias("ML"),
                (F.col("ML.LineID") == F.col("MD.MapLineID"))
                & (F.col("ML.ClientID") == F.col("MD.ClientID"))
                & (F.col("ML.TaxPeriodID") == F.col("MD.TaxPeriodID")),
            )
            .join(
                F.broadcast(k1_line_item).alias("KL"),
                (F.col("KL.LineID") == F.col("MD.RegisterLineID"))
                & (F.col("KL.ClientID") == F.col("MD.ClientID"))
                & (F.col("KL.TaxPeriodID") == F.col("MD.TaxPeriodID")),
            )
            .join(
                F.broadcast(vw_entity).alias("E"),
                (F.col("E.EntityID") == F.col("MD.EntityID"))
                & (F.col("E.ClientID") == F.col("MD.ClientID"))
                & (F.col("E.TaxPeriodID") == F.col("MD.TaxPeriodID")),
            )
            .select(
                F.col("E.EntityID").alias("EntityID"),
                F.col("MD.MapLineID").alias("MapLineID"),
                F.col("ML.DatabaseName").alias("DatabaseName"),
                F.col("MD.RegisterLineID").alias("RegisterLineID"),
                F.when(F.col("MD.OperationType") == "-", F.lit("SUBTRACT"))
                 .otherwise(F.lit("ADD")).alias("Formula"),
                F.col("MD.FieldSourceID").alias("FieldSourceID"),
            )
        )

    entity_mappings = _build_mappings(F.col("MD.EntityID") == entity_id)
    fallback_raw = _build_mappings(F.col("MD.EntityID") == -1)

    # Anti-join the fallback against entity-specific mappings by MapLineID
    fallback_mappings = fallback_raw.alias("F").join(
        entity_mappings.select("MapLineID").alias("MS"),
        F.col("F.MapLineID") == F.col("MS.MapLineID"),
        "left_anti",
    )

    mappings_df = entity_mappings.unionByName(fallback_mappings)

    # ─── Empty-check (SQL EXISTS gate) ────────────────────────────────────
    # Need to materialise distinct DatabaseName values for the dynamic UNPIVOT
    # column list. This collect IS necessary (driver-side stack() expression).
    distinct_field_rows = (
        mappings_df.select("DatabaseName").distinct()
        .filter(F.col("DatabaseName").isin(_704C_VALUE_COLUMNS))
        .collect()
    )
    if not distinct_field_rows:
        logger.info(
            "[704c-PE-Book] No 704c->K1 mappings found "
            f"(entity={entity_id}, client={client_id}, tax_period={tax_period_id}) — skipping block"
        )
        return None
    distinct_fields = [r["DatabaseName"] for r in distinct_field_rows]

    # ─── #CostPercentage704cValues  (SQL lines 1989-1998) ─────────────────
    if cost_pct_function_df is None:
        cost_pct_function_df = _build_cost_percentage_function(spark, cfg)

    cp_function = cost_pct_function_df.alias("C").filter(F.col("C.WorkFlowID") == workflow_id)

    cost_pct_704c_values = (
        cp_function
        .join(
            F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg)).alias("U"),
            F.col("C.UnderlyingType") == F.col("U.UnderlyingTypeID"),
        )
        .join(
            _tbl(spark, "CostPercentage_704c_Snapshot", cfg).alias("CP"),
            (F.col("C.WorkFlowID") == F.col("CP.WorkFlowID"))
            & (F.col("C.CostPercentageID") == F.col("CP.CostPercentageID")),
        )
        .filter(F.col("U.UnderlyingType") != "ASSET CLASS")
        .select(
            F.col("C.WorkFlowID"), F.col("C.TransactionID"),
            F.col("C.ClientID"), F.col("C.TaxPeriodID"),
            F.col("C.EntityID"), F.col("C.InvestmentID"),
            F.col("C.PartnerNumber"), F.col("C.Quarter"),
            F.col("C.CommitmentPercent"), F.col("C.AllocationTypeID"),
            F.col("C.Tag"), F.col("C.TrackingKey"),
            F.col("C.UnderlyingType"), F.col("C.AllocatedAmount"),
            F.col("C.CostPercentageID"),
            F.col("U.UnderlyingType").alias("EntityUnderlyingType"),
            F.col("CP.TotalMgmtFees"),
            F.col("CP.HotIssueGainLoss"),
            F.col("CP.`704cGainLoss`").alias("704cGainLoss"),
            F.col("CP.GuaranteedPaymentsServices"),
            F.col("CP.GuaranteedPaymentsCapital"),
            F.col("CP.UsWithholding"),
            F.col("CP.IncentiveFee"),
            F.col("CP.ForeignTaxes"),
            F.col("CP.SpecialAllocation1"),
            F.col("CP.SpecialAllocation2"),
            F.col("CP.GPPartnerReceivingCarry"),
        )
    )

    # Empty-check for the values table (SQL EXISTS gate, second half)
    if not cost_pct_704c_values.head(1):
        logger.info(
            "[704c-PE-Book] CostPercentage_704c_Snapshot has no rows for "
            f"workflow_id={workflow_id} — skipping block"
        )
        return None

    # ─── Dynamic UNPIVOT via stack()  (SQL lines 2064-2098) ───────────────
    stack_args = []
    for f in distinct_fields:
        stack_args.append(f"'{f}'")
        # 704cGainLoss column name starts with digit — backtick it.
        stack_args.append(f"`{f}`")
    stack_expr = f"stack({len(distinct_fields)}, " + ", ".join(stack_args) + ") as (Mapped704cField, AllocatedAmount)"

    unpivoted = cost_pct_704c_values.select(
        "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
        "EntityID", "InvestmentID", "PartnerNumber", "Quarter",
        "CommitmentPercent", "Tag", "TrackingKey",
        "UnderlyingType", "CostPercentageID", "EntityUnderlyingType",
        "GPPartnerReceivingCarry",
        F.expr(stack_expr),
    )

    # ─── Apply mapping (K1LineID + Formula sign flip)  SQL 2102-2127 ─────
    mapping_lookup = mappings_df.select(
        F.col("DatabaseName").alias("_M_DatabaseName"),
        F.col("RegisterLineID").alias("_M_RegisterLineID"),
        F.col("Formula").alias("_M_Formula"),
    ).distinct()

    unpivoted_mapped = (
        unpivoted.join(
            F.broadcast(mapping_lookup),
            F.col("Mapped704cField") == F.col("_M_DatabaseName"),
            "inner",
        )
        .withColumn("K1LineID", F.col("_M_RegisterLineID"))
        .withColumn(
            "AllocatedAmount",
            F.when(F.col("_M_Formula") == "SUBTRACT", -F.col("AllocatedAmount"))
             .otherwise(F.col("AllocatedAmount")),
        )
        .drop("_M_DatabaseName", "_M_RegisterLineID", "_M_Formula")
    )

    # ─── Persist 'Special <field>' rows in master tables  SQL 2140-2160 ──
    # MERGE INTO ENU_CustomAllocations + ENU_RuleGroup so downstream
    # consumers (other SPs, reports) can resolve the IDs by name. Returns
    # broadcastable lookup DataFrames keyed by Mapped704cField.
    alloc_type_lookup, rule_group_lookup = _persist_704c_custom_allocations(
        spark, cfg, distinct_fields, client_id, tax_period_id,
    )

    # ─── Merge: SUM(AllocatedAmount) by K1LineID  SQL 2131-2138 ──────────
    merged = (
        unpivoted_mapped
        .groupBy(
            "WorkFlowID", "TransactionID", "ClientID", "TaxPeriodID",
            "EntityID", "InvestmentID", "PartnerNumber", "Quarter",
            "CommitmentPercent", "Tag", "TrackingKey",
            "UnderlyingType", "CostPercentageID", "EntityUnderlyingType",
            "K1LineID",
            F.coalesce(F.col("GPPartnerReceivingCarry"), F.lit(False)).alias("_gp_carry_grp"),
        )
        .agg(
            F.sum("AllocatedAmount").alias("AllocatedAmount"),
            F.max("Mapped704cField").alias("Mapped704cField"),
        )
        .withColumnRenamed("_gp_carry_grp", "GPPartnerReceivingCarry")
    )

    # Resolve real (persisted) AllocationTypeID / RuleGroupID by name.
    merged = (
        merged.alias("M")
        .join(
            F.broadcast(alloc_type_lookup).alias("AT"),
            F.col("M.Mapped704cField") == F.col("AT.Mapped704cField"),
        )
        .join(
            F.broadcast(rule_group_lookup).alias("RG"),
            F.col("M.Mapped704cField") == F.col("RG.Mapped704cField"),
        )
        .select(
            F.col("M.WorkFlowID"), F.col("M.TransactionID"),
            F.col("M.ClientID"), F.col("M.TaxPeriodID"),
            F.col("M.EntityID"), F.col("M.InvestmentID"),
            F.col("M.PartnerNumber"), F.col("M.Quarter"),
            F.col("M.CommitmentPercent"), F.col("M.Tag"),
            F.col("M.TrackingKey"), F.col("M.UnderlyingType"),
            F.col("M.CostPercentageID"), F.col("M.EntityUnderlyingType"),
            F.col("M.K1LineID"), F.col("M.GPPartnerReceivingCarry"),
            F.col("M.AllocatedAmount"), F.col("M.Mapped704cField"),
            F.col("AT.AllocationTypeID").alias("AllocationTypeID"),
            F.col("RG.RuleGroupID").alias("Mapped704cFieldRuleGroupID"),
        )
    )

    # ─── Resolve scalar IDs for AllocationPercentageType / By / RuleType ─
    # SQL lines 2188-2191
    apt_row = (
        _tbl(spark, "ENU_AllocationPercentageType", cfg)
        .filter(F.col("AllocationPercentageType") == "N/A")
        .select("AllocationPercentageTypeID").head(1)
    )
    ab_row = (
        _tbl(spark, "ENU_AllocationBy", cfg)
        .filter(F.col("AllocationBy") == "AMOUNT")
        .select("AllocationByID").head(1)
    )
    rt_row = (
        _tbl(spark, "ENU_RuleType", cfg)
        .filter(F.col("RuleType") == "ENTITY")
        .select("RuleTypeID").head(1)
    )
    if not (apt_row and ab_row and rt_row):
        logger.warning("[704c-PE-Book] Missing ENU_AllocationPercentageType/By/RuleType — skipping block")
        return None
    alloc_pct_type_id = apt_row[0]["AllocationPercentageTypeID"]
    alloc_by_id = ab_row[0]["AllocationByID"]
    rule_type_id = rt_row[0]["RuleTypeID"]

    # ─── #MapDefaultAllocRuleToLineItem additions  SQL 2197-2200 ─────────
    map_dar_704c = (
        mappings_df.alias("MS")
        .join(
            merged.alias("CS"),
            F.col("MS.DatabaseName") == F.col("CS.Mapped704cField"),
        )
        .select(
            F.lit(-2).cast("int").alias("TransactionID"),
            F.lit(k1_line_type_id).cast("int").alias("SourceID"),
            F.lit(0).cast("int").alias("StateID"),
            F.col("MS.RegisterLineID").alias("SelectedMappingID"),
            F.col("CS.AllocationTypeID").alias("RuleID"),
            F.lit(0).cast("int").alias("ExcludeFromTransfers"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
        )
        .distinct()
    )

    # ─── #DefaultAllocationRuleSetup additions  SQL 2203-2205 ────────────
    dar_setup_704c = (
        mappings_df.alias("MS")
        .join(
            merged.alias("CS"),
            F.col("MS.DatabaseName") == F.col("CS.Mapped704cField"),
        )
        .select(
            F.lit(-2).cast("int").alias("TransactionID"),
            F.col("CS.AllocationTypeID").alias("RuleID"),
            F.lit(alloc_pct_type_id).cast("int").alias("AllocationPercentageTypeID"),
            F.lit(alloc_by_id).cast("int").alias("AllocationByID"),
            F.col("CS.UnderlyingType").alias("UnderlyingTypeID"),
            F.lit(rule_type_id).cast("int").alias("RuleTypeID"),
            F.col("CS.Mapped704cFieldRuleGroupID").alias("RuleGroupID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
        )
        .distinct()
    )

    # ─── Augmenting rows for #CostPercentage_Snapshot  SQL 2209-2213 ─────
    # Shape must match build_cost_percentage_snapshot_modes123 SELECT list.
    snapshot_augment = merged.select(
        F.col("WorkFlowID"), F.col("TransactionID"),
        F.col("ClientID"), F.col("TaxPeriodID"),
        F.col("EntityID"), F.col("InvestmentID"),
        F.col("PartnerNumber"), F.col("Quarter"),
        F.col("CommitmentPercent"),
        F.col("AllocationTypeID"),
        F.col("Tag"), F.col("TrackingKey"),
        F.col("UnderlyingType"), F.col("AllocatedAmount"),
        F.col("CostPercentageID"),
        F.col("EntityUnderlyingType"),
        F.lit(None).cast("int").alias("704cAllocationTypeID"),
        F.lit(None).cast("string").alias("704cPercentageType"),
        F.col("GPPartnerReceivingCarry"),
    )

    _log_timing("build_mode1_704c_pe_book_allocations", t0)

    return {
        "mappings": mappings_df,
        "snapshot_augment": snapshot_augment,
        "map_dar_704c": map_dar_704c,
        "dar_setup_704c": dar_setup_704c,
    }


# ---------------------------------------------------------------------------
# build_cost_percentage_snapshot (mode 4 — 704c)
# SQL lines: 2257-2350
# Row count: ALWAYS-NON-EMPTY for mode 4
# ---------------------------------------------------------------------------
def build_cost_percentage_snapshot_mode4(
    spark: SparkSession, cfg: dict
) -> DataFrame:
    """Build #CostPercentage_Snapshot for mode 4 (704c).

    Converted from SQL lines 2257-2350.
    UNPIVOTs AllocationPercentage704c, joins with CostPercentage_Snapshot + 704c snapshot.
    """
    t0 = time.time()
    logger.info("[SECTION] build_cost_percentage_snapshot_mode4")

    run_id = cfg["run_id"]
    workflow_id = cfg["cost_percentage_workflow_id"]

    # UNPIVOT AllocationPercentage704c (SQL lines 2259-2265)
    alloc_704c_raw = (
        _tbl(spark, "AllocationPercentage704c", cfg)
        .filter(F.col("RunID") == run_id)
    )

    # Stack-based UNPIVOT for the 4 percentage types
    alloc_704c = (
        alloc_704c_raw
        .select(
            "ClientID", "TaxPeriodID", "EntityID", "InvestmentID",
            "PartnerNumber", "Quarter", "AllocationTypeID",
            "UnderlyingType", "TrackingKey", "`704cAllocationTypeID`",
            F.expr("""stack(4,
                'OrdinaryPercentage', OrdinaryPercentage,
                'CapitalPercentage', CapitalPercentage,
                'CapitalGainPercentage', CapitalGainPercentage,
                'CapitalLossPercentage', CapitalLossPercentage
            ) as (`704cPercentageType`, CommitmentPercent)"""),
        )
    )

    # Join with CostPercentage_Snapshot + CostPercentage_704c_Snapshot (SQL lines 2270-2340)
    result = (
        _tbl(spark, "CostPercentage_Snapshot", cfg).alias("CS")
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg)).alias("E1"),
            F.col("CS.EntityID") == F.col("E1.EntityID"),
        )
        .join(
            _tbl(spark, "CostPercentage_704c_Snapshot", cfg).alias("CP"),
            (F.col("CS.WorkFlowID") == F.col("CP.WorkFlowID"))
            & (F.col("CS.CostPercentageID") == F.col("CP.CostPercentageID")),
        )
        .join(
            alloc_704c.alias("AC"),
            (F.col("CS.EntityID") == F.col("AC.EntityID"))
            & (F.coalesce(F.col("CS.InvestmentID"), F.lit(0)) == F.coalesce(F.col("AC.InvestmentID"), F.lit(0)))
            & (F.coalesce(F.col("CS.PartnerNumber"), F.lit("")) == F.coalesce(F.col("AC.PartnerNumber"), F.lit("")))
            & (F.coalesce(F.col("CS.Quarter"), F.lit("")) == F.coalesce(F.col("AC.Quarter"), F.lit("")))
            & (F.coalesce(F.col("CS.UnderlyingType"), F.lit(0)) == F.coalesce(F.col("AC.UnderlyingType"), F.lit(0)))
            & (F.coalesce(F.col("CS.AllocationTypeID"), F.lit(0)) == F.coalesce(F.col("AC.AllocationTypeID"), F.lit(0)))
            & (F.coalesce(F.col("CS.TrackingKey"), F.lit("")) == F.coalesce(F.col("AC.TrackingKey"), F.lit("")))
            & (F.coalesce(F.col("CP.`704cAllocationTypeID`"), F.lit(0)) == F.coalesce(F.col("AC.`704cAllocationTypeID`"), F.lit(0))),
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_UnderlyingType", cfg)).alias("U"),
            F.coalesce(F.col("CS.UnderlyingType"), F.lit(0)) == F.coalesce(F.col("U.UnderlyingTypeID"), F.lit(0)),
        )
        .filter(F.coalesce(F.col("CS.WorkFlowID"), F.lit(0)) == workflow_id)
        .select(
            F.col("CS.WorkFlowID"), F.col("CS.TransactionID"),
            F.col("CS.ClientID"), F.col("CS.TaxPeriodID"),
            F.col("CS.EntityID"), F.col("CS.InvestmentID"),
            F.col("CS.PartnerNumber"), F.col("CS.Quarter"),
            F.col("AC.CommitmentPercent"),
            F.col("CS.AllocationTypeID"),
            F.lit("").alias("Tag"),
            F.col("CS.TrackingKey"),
            F.col("CS.UnderlyingType"), F.col("CS.AllocatedAmount"),
            F.col("CS.CostPercentageID"),
            F.col("U.UnderlyingType").alias("EntityUnderlyingType"),
            F.col("CP.`704cAllocationTypeID`"),
            F.col("AC.`704cPercentageType`"),
            F.col("CP.GPPartnerReceivingCarry"),
        )
    )

    if result.isEmpty():
        logger.warning("build_cost_percentage_snapshot_mode4 produced 0 rows")

    _log_timing("build_cost_percentage_snapshot_mode4", t0)
    return result


# ---------------------------------------------------------------------------
# build_temp_cost_percentage
# SQL lines: 2395-2410
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_temp_cost_percentage(
    spark: SparkSession, cfg: dict, cost_pct_snapshot: DataFrame
) -> DataFrame:
    """Build #TempCostPercentage from CostPercentage_Snapshot.

    Converted from SQL lines 2395-2410.
    Filters to K-1 ONLY entity underlying type, InvestmentID != -1.
    Also unions yearly prorata percentages.
    """
    t0 = time.time()
    logger.info("[SECTION] build_temp_cost_percentage")

    entity_ut_id = cfg["entity_underlying_type_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]

    # Part 1: Cost percentage snapshot (K-1 ONLY, InvestmentID != -1)
    p1 = (
        cost_pct_snapshot
        .filter(
            (F.col("InvestmentID") != -1)
            & (
                F.coalesce(F.col("UnderlyingType"), F.lit(entity_ut_id).cast("long"))
                == entity_ut_id
            )
        )
        .select(
            F.col("InvestmentID").alias("DealId"),
            F.col("PartnerNumber").alias("Partnernumber"),
            F.col("Quarter"),
            F.coalesce(F.col("CommitmentPercent"), F.lit(0.0)).alias("CommitmentPercent"),
            F.coalesce(F.col("AllocationTypeID"), F.lit(cost_alloc_type_id).cast("long")).alias("TypeId"),
            F.coalesce(F.col("TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.coalesce(F.col("Tag"), F.lit("")).alias("Tag"),
            F.col("`704cAllocationTypeID`"),
            F.col("`704cPercentageType`"),
            F.col("GPPartnerReceivingCarry"),
        )
    )

    _log_timing("build_temp_cost_percentage", t0)
    return p1

