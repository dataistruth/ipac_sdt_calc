"""
entity_hierarchy.py

Converted from: dbo.uspGetFinalEffectivePercentage.sql
Entity hierarchy traversal and underlying type resolution.
Conversion date: 2026-05-04

SQL lines: 2370-2550 (entity hierarchy WHILE loop),
           1820-1845 (entity underlyings, partner list),
           2350-2400 (#EntityAssetClassRelationShip, udfGetAssetClassRelationship)
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
# build_entity_partners
# SQL lines: 1820-1835 (udf_PE_GetPartnersListForReports inlined)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_partners(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build #EntityPartners — list of partner numbers for the entity.

    Inlines dbo.udf_PE_GetPartnersListForReports using a single join-based
    query instead of sequential scalar lookups for performance.
    """
    t0 = time.time()
    logger.info("[SECTION] build_entity_partners")

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    phase_id = cfg["phase_id"]

    # Single query: join WorkFlow + TransactionLog to get latest workflow/txn,
    # then join Partner_Snapshot to get partner numbers.
    # This replaces 4 sequential .first()/.collect() calls.

    enu_event = F.broadcast(_tbl(spark, "ENU_Event", cfg))
    wf_status = F.broadcast(_tbl(spark, "WORKFLOWSTATUS", cfg))
    wf = _tbl(spark, "WorkFlow", cfg).alias("WF")
    tl = _tbl(spark, "TransactionLog", cfg).alias("TL")
    ps = _tbl(spark, "Partner_Snapshot", cfg).alias("PS")

    # Subquery: rejected status IDs
    rejected_ids = (
        wf_status.filter(F.col("EnumerationName") == "Rejected")
        .select("StatusID")
    )

    # Subquery: partner import event type ID
    # Inlines dbo.udf_PE_GetPartnerImportEventID:
    #   If GlobalMenu Partner Import Methodology = 'Master Import' → 'MasterImport_Partner'
    #   Else → 'Import_Partner'
    global_menu = F.broadcast(_tbl(spark, "GlobalMenu", cfg))
    enu_gmg = F.broadcast(_tbl(spark, "ENU_GlobalMenuGroup", cfg))
    import_type_row = (
        global_menu.alias("M")
        .join(
            enu_gmg.alias("GM"),
            (F.col("M.GlobalMenuGroupID") == F.col("GM.GlobalMenuGroupID"))
            & (F.col("GM.GroupName") == "Partner Import Methodology"),
        )
        .filter(
            (F.col("M.State") == "C")
            & (F.col("M.ClientID") == client_id)
            & (F.col("M.TaxPeriodID") == tax_period_id)
        )
        .select("M.MenuName")
        .first()
    )
    import_type = import_type_row["MenuName"] if import_type_row else None
    partner_event_name = "MasterImport_Partner" if import_type == "Master Import" else "Import_Partner"

    partner_evt = (
        enu_event.filter(F.col("EventName") == partner_event_name)
        .select("EventTypeID")
    )

    # Subquery: latest workflow for entity (max WorkflowID from non-rejected)
    latest_wf = (
        wf.join(
            tl,
            (F.col("TL.TransactionID") == F.col("WF.TransactionID"))
            & (F.col("TL.PhaseID") == F.col("WF.PhaseID")),
        )
        .join(partner_evt, F.col("TL.EventTypeID") == partner_evt["EventTypeID"])
        .join(rejected_ids, F.col("TL.StatusID") == rejected_ids["StatusID"], "left_anti")
        .filter(
            (F.col("TL.EntityID") == entity_id)
            & (F.col("TL.ClientID") == client_id)
            & (F.col("TL.TaxPeriodID") == tax_period_id)
            & (F.col("TL.PhaseID") == phase_id)
        )
        .select(
            F.max("WF.WorkflowID").alias("LatestWorkflowID"),
            F.max("TL.TransactionID").alias("LatestTransactionID"),
        )
    )

    # Join Partner_Snapshot with latest workflow/transaction
    df = (
        ps.filter(
            (F.col("PS.Clientid") == client_id)
            & (F.col("PS.TaxperiodID") == tax_period_id)
            & (F.col("PS.EntityID") == entity_id)
        )
        .crossJoin(latest_wf.alias("LW"))
        .filter(
            F.when(
                F.coalesce(F.col("PS.WorkFlowID"), F.lit(0)) != 0,
                F.col("PS.WorkFlowID") == F.col("LW.LatestWorkflowID"),
            ).otherwise(
                F.col("PS.Transactionid") == F.col("LW.LatestTransactionID"),
            )
        )
        .select(F.col("PS.PartnerNumber").alias("partnernumber"))
        .distinct()
    )

    _log_timing("build_entity_partners", t0)
    return df


# ---------------------------------------------------------------------------
# build_asset_class_relationship
# SQL lines: 2355-2370 (udfGetAssetClassRelationship inlined)
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_asset_class_relationship(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build #EntityAssetClassRelationShip — delegates to Common_V2.

    Uses the canonical Common_V2.domain.udf_asset_class_relationship
    implementation (single source of truth across all SPs that call
    udfGetAssetClassRelationship).

    Returns DataFrame with columns: LowerTierEntityID, AssetClassID, TrackingKey.
    """
    t0 = time.time()
    logger.info("[SECTION] build_asset_class_relationship")

    from Common_V2.domain.udf_asset_class_relationship import (
        udfGetAssetClassRelationship,
    )

    entity_id = cfg["entity_id"]
    udf = udfGetAssetClassRelationship(spark, cfg, entity_ids=str(entity_id))
    df = udf.execute()

    _log_timing("build_asset_class_relationship", t0)
    return df


# ---------------------------------------------------------------------------
# build_cost_underlying_types
# SQL lines: 2350-2365
# Row count: POSSIBLY-EMPTY (e.g., K-1 ONLY entity with no InvestmentID=-1)
# ---------------------------------------------------------------------------
def build_cost_underlying_types(
    spark: SparkSession, cfg: dict, cost_pct_snapshot: DataFrame
) -> DataFrame:
    """Build #TempCostUnderlyingTypes from CostPercentage_Snapshot.

    Converted from SQL lines 2350-2365.
    """
    t0 = time.time()
    logger.info("[SECTION] build_cost_underlying_types")

    entity_id = cfg["entity_id"]

    df = (
        cost_pct_snapshot
        .filter(
            (F.col("EntityUnderlyingType") != "K-1 ONLY")
            | (
                (F.col("EntityUnderlyingType") == "K-1 ONLY")
                & (F.col("InvestmentID") == -1)
            )
        )
        .select(
            F.col("EntityID").alias("EntityId"),
            F.when(F.col("InvestmentID") == -1, F.col("EntityID"))
            .otherwise(F.col("InvestmentID"))
            .alias("InvestmentID"),
            F.col("Quarter"),
            F.col("AllocationTypeID").alias("AllocationTypeId"),
            F.col("TrackingKey"),
            F.col("UnderlyingType").alias("Underlyingtype"),
            F.col("EntityUnderlyingType").alias("EntityUnderlyingtype"),
            F.coalesce(F.col("Tag"), F.lit("-1")).alias("TAG"),
            F.col("InvestmentID").alias("Cost_InvestmentID"),
        )
        .distinct()
    )

    _log_timing("build_cost_underlying_types", t0)
    return df


# ---------------------------------------------------------------------------
# build_entity_hierarchy
# SQL lines: 2370-2430 (WHILE loop for recursive hierarchy traversal)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_entity_hierarchy(
    spark: SparkSession,
    cfg: dict,
    cost_underlying_types: DataFrame,
) -> DataFrame:
    """Build #EntityHierarchy via iterative join (replaces SQL WHILE loop).

    Converted from SQL lines 2370-2430.
    Walks EntityRelationship from cost underlying investment IDs down to leaf entities.
    """
    t0 = time.time()
    logger.info("[SECTION] build_entity_hierarchy")

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    entity_rel = (
        _tbl(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("LowerTierEntityID", "UpperTierEntityID")
    )

    # Seed: SQL lines 2372-2392
    hierarchy = (
        cost_underlying_types.alias("TC")
        .join(
            entity_rel.alias("ER"),
            F.col("ER.UpperTierEntityID") == F.when(
                F.col("TC.EntityUnderlyingtype") == "ASSET CLASS",
                F.col("TC.EntityId"),
            ).otherwise(F.col("TC.InvestmentID")),
        )
        .select(
            F.col("ER.LowerTierEntityID"),
            F.col("ER.UpperTierEntityID"),
            F.col("ER.UpperTierEntityID").alias("CurrentEntityId"),
            F.when(F.col("ER.UpperTierEntityID") == entity_id, F.lit(10001))
            .otherwise(F.lit(2))
            .alias("HLevel"),
            F.col("TC.AllocationTypeId"),
            F.concat(
                F.lit("~"),
                F.when(
                    F.col("TC.EntityUnderlyingtype") == "ASSET CLASS",
                    F.concat(F.col("ER.LowerTierEntityID").cast("string"), F.lit("~")),
                ).otherwise(
                    F.concat(
                        F.when(
                            F.coalesce(F.col("TC.TrackingKey"), F.lit("")) == "",
                            F.col("TC.InvestmentID").cast("string"),
                        ).otherwise(F.col("TC.TrackingKey")),
                        F.lit("~"),
                    )
                ),
            ).alias("TrackingKey"),
            F.col("TC.InvestmentID").alias("AssetClassId"),
            F.col("ER.LowerTierEntityID").alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    # Iterative expansion — exact equivalent of SQL WHILE (1=1) ... IF
    # @@ROWCOUNT = 0 BREAK at SQL lines 2402-2430. No depth cap (SQL has
    # none); the loop terminates the moment a pass produces zero new rows.
    # A high sanity bound is logged but does not break the loop, mirroring
    # the SP semantics exactly.
    depth = 0
    while True:
        new_rows = (
            entity_rel.alias("ER")
            .join(
                hierarchy.alias("EH"),
                F.col("ER.UpperTierEntityID") == F.col("EH.LowerTierEntityID"),
            )
            .join(
                hierarchy.alias("T"),
                (F.col("ER.LowerTierEntityID") == F.col("T.LowerTierEntityID"))
                & (F.col("ER.UpperTierEntityID") == F.col("T.UpperTierEntityID"))
                & (F.col("EH.CurrentEntityId") == F.col("T.CurrentEntityId"))
                & (
                    F.when(
                        F.col("EH.CurrentEntityId") == entity_id,
                        F.lit(10001),
                    ).otherwise(F.col("EH.HLevel") + 1)
                    == F.col("T.HLevel")
                )
                & (F.col("EH.AllocationTypeId") == F.col("T.AllocationTypeId"))
                & (
                    F.coalesce(F.col("T.TrackingKey"), F.lit(""))
                    == F.coalesce(F.col("EH.TrackingKey"), F.lit(""))
                )
                & (F.col("T.AssetClassId") == F.col("EH.AssetClassId"))
                & (F.col("T.ImmediateLowerTierEntityID") == F.col("EH.ImmediateLowerTierEntityID")),
                "left_anti",
            )
            .select(
                F.col("ER.LowerTierEntityID"),
                F.col("ER.UpperTierEntityID"),
                F.col("EH.CurrentEntityId"),
                F.when(F.col("EH.CurrentEntityId") == entity_id, F.lit(10001))
                .otherwise(F.col("EH.HLevel") + 1)
                .alias("HLevel"),
                F.col("EH.AllocationTypeId"),
                F.col("EH.TrackingKey"),
                F.col("EH.AssetClassId"),
                F.col("EH.ImmediateLowerTierEntityID"),
            )
        )

        if new_rows.isEmpty():
            break

        hierarchy = hierarchy.unionByName(new_rows)
        depth += 1
        if depth % 50 == 0:
            logger.warning(
                f"build_entity_hierarchy: still expanding at depth={depth} "
                "— check for cycles in EntityRelationship if this grows much further."
            )

    logger.info(f"build_entity_hierarchy: converged after {depth} expansion passes")

    _log_timing("build_entity_hierarchy", t0)
    return hierarchy


# ---------------------------------------------------------------------------
# build_underlyings_combined
# SQL lines: 2435-2500
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_underlyings_combined(
    spark: SparkSession,
    cfg: dict,
    cost_underlying_types: DataFrame,
    entity_hierarchy: DataFrame,
    cost_pct_snapshot: DataFrame,
) -> DataFrame:
    """Build #TempAllUnderlyingsCombined — the core underlying entity set.

    Converted from SQL lines 2435-2500.
    Unions: hierarchy-based + K-1 ONLY + K-1 ONLY with InvestmentID=-1 +
            Asset Class at entity level + Entity Total.
    """
    t0 = time.time()
    logger.info("[SECTION] build_underlyings_combined")

    entity_id = cfg["entity_id"]
    cols = [
        "UnderlyingEntityID", "EntityID", "HLevel", "UnderlyingType",
        "AllocationTypeID", "TrackingKey", "AssetClassID",
        "ImmediateLowerTierEntityID", "Cost_Entity", "Cost_InvestmentID",
        "Cost_Quarter", "Cost_AllocationTypeID", "Cost_TrackingKey",
        "Cost_UnderlyingType",
    ]

    # Part 1: Hierarchy-based (non K-1 ONLY)
    p1 = (
        cost_underlying_types.alias("TC")
        .join(
            entity_hierarchy.alias("EH"),
            (
                F.col("EH.CurrentEntityId")
                == F.when(
                    F.col("TC.EntityUnderlyingtype") == "ASSET CLASS",
                    F.col("TC.EntityId"),
                ).otherwise(F.col("TC.InvestmentID"))
            )
            & (F.col("TC.AllocationTypeId") == F.col("EH.AllocationTypeId"))
            & (F.col("TC.InvestmentID") == F.col("EH.AssetClassId")),
        )
        .select(
            F.col("EH.LowerTierEntityID").alias("UnderlyingEntityID"),
            F.col("EH.CurrentEntityId").alias("EntityID"),
            F.col("EH.HLevel"),
            F.col("TC.Underlyingtype").alias("UnderlyingType"),
            F.col("TC.AllocationTypeId").alias("AllocationTypeID"),
            F.col("EH.TrackingKey"),
            F.col("EH.AssetClassId").alias("AssetClassID"),
            F.col("EH.ImmediateLowerTierEntityID"),
            F.col("TC.EntityId").alias("Cost_Entity"),
            F.col("TC.Cost_InvestmentID"),
            F.col("TC.Quarter").alias("Cost_Quarter"),
            F.col("TC.AllocationTypeId").alias("Cost_AllocationTypeID"),
            F.col("TC.TrackingKey").alias("Cost_TrackingKey"),
            F.col("TC.Underlyingtype").alias("Cost_UnderlyingType"),
        )
        .distinct()
    )

    # Part 2: K-1 ONLY (InvestmentID != -1)
    k1_only = cost_pct_snapshot.filter(F.col("EntityUnderlyingType") == "K-1 ONLY")

    p2 = (
        k1_only
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityID"),
            F.col("InvestmentID").alias("EntityID"),
            F.when(F.col("InvestmentID") == entity_id, F.lit(10001))
            .otherwise(F.lit(1))
            .alias("HLevel"),
            F.col("UnderlyingType"),
            F.col("AllocationTypeID"),
            F.when(
                F.coalesce(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
            ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassID"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
            F.col("EntityID").alias("Cost_Entity"),
            F.col("InvestmentID").alias("Cost_InvestmentID"),
            F.col("Quarter").alias("Cost_Quarter"),
            F.col("AllocationTypeID").alias("Cost_AllocationTypeID"),
            F.col("TrackingKey").alias("Cost_TrackingKey"),
            F.col("UnderlyingType").alias("Cost_UnderlyingType"),
        )
    )

    # Part 3: K-1 ONLY with InvestmentID = -1 (partnership level)
    p3 = (
        k1_only
        .filter(F.col("InvestmentID") == -1)
        .select(
            F.lit(entity_id).cast("long").alias("UnderlyingEntityID"),
            F.lit(entity_id).cast("long").alias("EntityID"),
            F.lit(10001).alias("HLevel"),
            F.col("UnderlyingType"),
            F.col("AllocationTypeID"),
            F.concat(F.lit("~"), F.lit(entity_id).cast("string"), F.lit("~")).alias("TrackingKey"),
            F.lit(entity_id).cast("long").alias("AssetClassID"),
            F.lit(entity_id).cast("long").alias("ImmediateLowerTierEntityID"),
            F.col("EntityID").alias("Cost_Entity"),
            F.col("InvestmentID").alias("Cost_InvestmentID"),
            F.col("Quarter").alias("Cost_Quarter"),
            F.col("AllocationTypeID").alias("Cost_AllocationTypeID"),
            F.col("TrackingKey").alias("Cost_TrackingKey"),
            F.col("UnderlyingType").alias("Cost_UnderlyingType"),
        )
    )

    # Part 4: Asset Class at entity level
    p4 = (
        cost_underlying_types
        .filter(F.col("EntityUnderlyingtype") == "ASSET CLASS")
        .select(
            F.col("EntityId").alias("UnderlyingEntityID"),
            F.col("EntityId").alias("EntityID"),
            F.when(F.col("EntityId") == entity_id, F.lit(10001))
            .otherwise(F.lit(1))
            .alias("HLevel"),
            F.col("Underlyingtype").alias("UnderlyingType"),
            F.col("AllocationTypeId").alias("AllocationTypeID"),
            F.concat(F.lit("~"), F.col("EntityId").cast("string"), F.lit("~")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassID"),
            F.col("EntityId").alias("ImmediateLowerTierEntityID"),
            F.col("EntityId").alias("Cost_Entity"),
            F.col("Cost_InvestmentID"),
            F.col("Quarter").alias("Cost_Quarter"),
            F.col("AllocationTypeId").alias("Cost_AllocationTypeID"),
            F.col("TrackingKey").alias("Cost_TrackingKey"),
            F.col("Underlyingtype").alias("Cost_UnderlyingType"),
        )
    )

    # Part 5: Entity Total
    p5 = (
        cost_underlying_types
        .filter(F.col("EntityUnderlyingtype") == "ENTITY TOTAL")
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityID"),
            F.col("InvestmentID").alias("EntityID"),
            F.when(F.col("InvestmentID") == entity_id, F.lit(10001))
            .otherwise(F.lit(1))
            .alias("HLevel"),
            F.col("Underlyingtype").alias("UnderlyingType"),
            F.col("AllocationTypeId").alias("AllocationTypeID"),
            F.when(
                F.coalesce(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")),
            ).otherwise(F.col("TrackingKey")).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassID"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
            F.col("EntityId").alias("Cost_Entity"),
            F.col("Cost_InvestmentID"),
            F.col("Quarter").alias("Cost_Quarter"),
            F.col("AllocationTypeId").alias("Cost_AllocationTypeID"),
            F.col("TrackingKey").alias("Cost_TrackingKey"),
            F.col("Underlyingtype").alias("Cost_UnderlyingType"),
        )
    )

    result = p1.unionByName(p2).unionByName(p3).unionByName(p4).unionByName(p5)

    _log_timing("build_underlyings_combined", t0)
    return result
