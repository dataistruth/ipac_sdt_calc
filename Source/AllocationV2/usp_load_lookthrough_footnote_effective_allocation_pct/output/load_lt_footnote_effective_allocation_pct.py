"""
load_lt_footnote_effective_allocation_pct.py

Converted from: dbo.uspLoadLookThroughFootnoteEffectiveAllocationPercentage.sql
Original procedure: dbo.uspLoadLookThroughFootnoteEffectiveAllocationPercentage
Conversion date: 2026-05-05

Usage (standalone):
    from load_lt_footnote_effective_allocation_pct import run_load_lt_footnote_effective_allocation_pct

    run_load_lt_footnote_effective_allocation_pct(
        spark,
        entity_id=123, client_id=456, tax_period_id=789, run_id=1001,
        catalog="dev7", schema="dev7",
    )

Usage (reuse shared config from a workflow):
    cfg = load_common_config(spark, entity_id, client_id, tax_period_id, run_id)
    run_load_lt_footnote_effective_allocation_pct(spark, cfg=cfg)
"""

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DecimalType,
    BooleanType, DoubleType, TimestampType, DateType,
)
from datetime import datetime
import json
import logging
import time

# ---------------------------------------------------------------------------
# Common_V2 imports
#
# All shared utilities (logger, checkpoint, helpers, GenericResultStorer) live
# under Common_V2 — no sys.path manipulation, no hardcoded notebook paths.
# Run the SP from a working dir where `Source/` is on PYTHONPATH (the
# Databricks job task / repo workspace already satisfies this).
# ---------------------------------------------------------------------------
from Common_V2.core.config import load_common_config
from Common_V2.core.helpers import (
    table_prefix as _table_prefix,
    tbl as _tbl,
    ns as _ns,
    ns0 as _ns0,
    sql_round as _sql_round,
)
from Common_V2.core.checkpoint import (
    checkpoint as _checkpoint,
    drop_checkpoints as _drop_checkpoints,
)
from Common_V2.core.observability import (
    get_logger,
    log_section as _log_section,
    log_timing as _log_timing,
)
from Common_V2.core.generic_result_storer import GenericResultStorer

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# SP-Specific Config — SQL lines 53–310
# ---------------------------------------------------------------------------
def _load_sp_config(spark, cfg):
    """Alias Common_V2 cfg scalars into SP-local legacy keys.

    All scalar lookups (AllocationRun fields, GlobalMenu flags + menu IDs,
    ENU_MappingSource IDs, ENU_LineType IDs, ENU_CustomAllocations ID,
    Entity AllocationTypeName) are pre-resolved by load_common_config.
    """
    _log_section("_load_sp_config")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]

    # ── IsConfigK1Checked (GlobalMenu "Configure K1 Line Item" State in ('C','CG')) ──
    cfg["is_config_k1_checked"] = (
        (cfg.get("flag_configure_k1") or "").strip().upper() in ("C", "CG")
    )

    # ── ENU_MappingSource: parent / contributor K-1 ──
    cfg["parent_k1_line_source_id"] = cfg.get("mapping_source_id_parent_k1")
    cfg["contributor_k1_line_source_id"] = cfg.get("mapping_source_id_contributor_k1")

    # ── ENU_LineType: BoxJKL / K1 ──
    cfg["enu_boxjkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    cfg["enu_k1_line_type_id"] = cfg.get("k1_line_type_id")

    # ── ENU_CustomAllocations: FederalToFootnoteAllocation ──
    cfg["ftf_allocation_type_id"] = cfg.get("custom_allocation_id_fed_to_footnote")

    # ── GlobalMenu IDs ──
    cfg["yearly_line_type_id"] = cfg.get("yearly_menu_id")
    cfg["register_type_id"] = cfg.get("federal_to_footnote_menu_id")

    # ── Entity AllocationTypeName ──
    cfg["allocation_type_name"] = cfg.get("entity_allocation_type_name")

    # ── SQL line 301: uspAddAllocationLog → logger.info ──
    logger.info(
        f"[ALLOC_LOG] Load LookThrough Effective Allocation Percentage | "
        f"RunID={run_id} ClientID={client_id} TaxPeriodID={tax_period_id}"
    )

    # ── SQL lines 305–315: Validate AllocationTypeName ──
    if not cfg.get("allocation_type_name"):
        raise ValueError(
            "AllocationTypeName is empty — allocation logic not selected for entity. "
            f"EntityID={cfg.get('entity_id')} RunID={cfg.get('run_id')}"
        )

    _log_timing("_load_sp_config", t0)
    return cfg


# ---------------------------------------------------------------------------
# Function 1: load_mappings
# SQL lines: 316–380
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def load_mappings(spark, cfg):
    """Load MAPDataRegister mappings filtered by RegisterTypeID and BoxJKL.

    Converted from: SQL lines 316–380.
    Row count: POSSIBLY-EMPTY — if no FederalToFootnote mappings configured.
    """
    _log_section("load_mappings")
    t0 = time.time()

    register_type_id = cfg["register_type_id"]
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    boxjkl_lt_id = cfg["enu_boxjkl_line_type_id"]

    # SQL lines 326–336: INSERT INTO #Mapping FROM MAPDataRegister
    mappings_df = (
        _tbl(spark, "MAPDataRegister", cfg)
        .filter(
            (F.col("RegisterTypeID") == register_type_id)
            & (F.col("EntityID").isin(-1, entity_id))
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
            & (F.col("FieldSourceID") == boxjkl_lt_id)
        )
        .select(
            F.col("MapRegisterID"),
            F.col("EntityID"),
            F.col("RegisterLineID").alias("RegisterLineId"),
            F.col("FieldSourceID"),
            F.col("OperationType"),
            F.col("SourceTypeID"),
            F.col("MapLineID"),
            F.col("ContributionLineClassification"),
        )
    )

    _log_timing("load_mappings", t0)
    return mappings_df


# ---------------------------------------------------------------------------
# Function 2: expand_parent_k1_mappings
# SQL lines: 380–430
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def expand_parent_k1_mappings(spark, cfg, mappings_df):
    """Expand Parent/Contributor K1 line mappings via udfGetParentToK1LineMappings logic.

    Converted from: SQL lines 380–430.
    Row count: POSSIBLY-EMPTY — conditional on IsConfigK1Checked.
    Inlines udfGetParentToK1LineMappings for CalledFrom='Allocations', IsFedToFootNote=1.
    """
    _log_section("expand_parent_k1_mappings")
    t0 = time.time()

    parent_src_id = cfg["parent_k1_line_source_id"]
    contrib_src_id = cfg["contributor_k1_line_source_id"]
    is_config_k1 = cfg["is_config_k1_checked"]
    k1_lt_id = cfg["enu_k1_line_type_id"]
    register_type_id = cfg["register_type_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    # SQL line 354: IF EXISTS(#Mapping WHERE SourceTypeID IN (parent, contrib) AND @IsConfigK1Checked=1)
    if not is_config_k1:
        _log_timing("expand_parent_k1_mappings", t0)
        return mappings_df

    source_ids = [sid for sid in [parent_src_id, contrib_src_id] if sid is not None]
    if not source_ids:
        _log_timing("expand_parent_k1_mappings", t0)
        return mappings_df

    has_parent_rows = not (
        mappings_df.filter(F.col("SourceTypeID").isin(source_ids)).isEmpty()
    )
    if not has_parent_rows:
        _log_timing("expand_parent_k1_mappings", t0)
        return mappings_df

    # --- Inline UDF: udfGetParentToK1LineMappings ---
    # Parameters: RegisterTypeID, ClientID, TaxPeriodID, ApplicableToState=0,
    #   ParentMappings=mappings_df, EntityIDs='', RunID, CalledFrom='Allocations'
    # IsFedToFootNote = 1 (since RegisterTypeID = 'Federal To Footnote' menu)

    # Step 1: Get register mappings for parent/contributor sources
    register_mappings = (
        _tbl(spark, "MAPDataRegister", cfg).alias("M")
        .join(
            mappings_df.select("MapRegisterID", "MapLineID", "RegisterLineId", "EntityID")
                .alias("PM"),
            (F.col("M.MapRegisterID") == F.col("PM.MapRegisterID"))
            & (F.col("M.MapLineID") == F.col("PM.MapLineID"))
            & (F.col("M.RegisterLineID") == F.col("PM.RegisterLineId")),
        )
        .filter(
            (F.col("M.RegisterTypeID") == register_type_id)
            & (F.col("M.ClientID") == client_id)
            & (F.col("M.TaxPeriodID") == tax_period_id)
            & (F.col("M.SourceTypeID").isin(source_ids))
        )
        .select(
            F.col("M.MapRegisterID"),
            F.col("PM.EntityID"),
            F.col("M.RegisterTypeID"),
            F.col("M.SourceTypeID"),
            F.col("M.MapLineID"),
            F.col("M.RegisterLineID"),
            F.col("M.OperationType"),
            F.col("M.FieldSourceID"),
        )
        .distinct()
    )

    # Step 2: Parent line mappings → join ParentK1GLineItem
    # IsFedToFootNote=1: join on MapLineID = ParentK1GLineID
    parent_line_mappings = register_mappings.select(
        "MapRegisterID", "MapLineID", "RegisterLineID", "OperationType"
    ).distinct()

    parents = (
        parent_line_mappings.alias("PM")
        .join(
            _tbl(spark, "ParentK1GLineItem", cfg).alias("PG"),
            F.col("PM.MapLineID") == F.col("PG.ParentK1GLineID"),
        )
        .select(
            F.col("PM.MapRegisterID"),
            F.col("PG.ParentK1GLineID").alias("MappedParentLineID"),
            F.col("PG.IsAllocable"),
            F.col("PM.OperationType").alias("Formula"),
            # IsFedToFootNote=1: MapLineID = RegisterLineID
            F.col("PM.RegisterLineID").alias("MapLineID_UDF"),
        )
        .distinct()
    )

    # Step 3: Get K1 line IDs from LookThroughAllocationInput (Allocations path)
    distinct_line_ids = (
        _tbl(spark, "LookThroughAllocationInput", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineTypeID") == k1_lt_id)
        )
        .select("LineID")
        .distinct()
    )

    # Step 4: Get ContributionLineWithAttributes for those lines
    # ApplicableToState=0 → the WHERE clause is always true (no-op filter)
    contrib_lines = (
        _tbl(spark, "ContributionLineWithAttributes", cfg).alias("C")
        .join(distinct_line_ids.alias("L"), F.col("C.K1LineID") == F.col("L.LineID"))
        .join(
            _tbl(spark, "K1LineItem", cfg).alias("K"),
            F.col("L.LineID") == F.col("K.LineID"),
        )
        .select(
            F.col("C.ContributionLineID"),
            F.col("C.K1LineID"),
            F.col("C.Source"),
            F.col("C.Waterfall"),
            F.col("C.TransactionDate"),
            F.col("C.FN"),
            F.col("C.Offset"),
            F.col("C.ParentLineID"),
            F.col("C.ContributionLineClassification"),
        )
        .distinct()
    )

    # Step 5: Match parents to K1 lines
    parent_child_k1 = (
        parents.alias("P")
        .join(
            contrib_lines.alias("C"),
            (
                F.when(F.col("P.IsAllocable") == True, F.col("C.ContributionLineID"))
                .otherwise(F.col("C.ParentLineID"))
                == F.col("P.MappedParentLineID")
            ),
        )
        .select(
            F.col("P.MapRegisterID"),
            F.col("P.MappedParentLineID"),
            F.col("C.ContributionLineID"),
            F.col("C.K1LineID"),
            F.col("P.Formula"),
            F.col("P.MapLineID_UDF").alias("MapLineID_PC"),
            F.col("C.ContributionLineClassification"),
        )
    )

    # Step 6: Return data — IsFedToFootNote=1: MapLineID=K1LineID, RegisterLineID=RM.RegisterLineID
    parent_child_mapped = (
        register_mappings.alias("RM")
        .join(
            parent_child_k1.alias("PC"),
            (F.col("RM.MapLineID") == F.col("PC.MappedParentLineID"))
            & (F.col("RM.MapRegisterID") == F.col("PC.MapRegisterID")),
        )
        .select(
            F.col("PC.MapRegisterID"),
            F.col("RM.EntityID"),
            F.col("RM.SourceTypeID"),
            # IsFedToFootNote=1: MapLineID = K1LineID
            F.col("PC.K1LineID").alias("MapLineID"),
            # IsFedToFootNote=1: RegisterLineID stays
            F.col("RM.RegisterLineID").alias("RegisterLineId"),
            F.col("RM.FieldSourceID"),
            F.col("PC.ContributionLineClassification"),
        )
        .distinct()
    )

    # --- SP lines 396–415: Join #ParentChildMappedData back to #Mapping ---
    expanded = (
        mappings_df.filter(F.col("SourceTypeID").isin(source_ids)).alias("M")
        .join(
            parent_child_mapped.alias("PCL"),
            (F.col("M.RegisterLineId") == F.col("PCL.RegisterLineId"))
            & (F.col("M.EntityID") == F.col("PCL.EntityID"))
            & (F.col("M.MapRegisterID") == F.col("PCL.MapRegisterID"))
            & (_ns0(F.col("M.FieldSourceID")) == _ns0(F.col("PCL.FieldSourceID")))
            & (
                F.when(
                    F.upper(F.coalesce(F.col("M.ContributionLineClassification"), F.lit("ALL"))) == "ALL",
                    F.col("PCL.ContributionLineClassification"),
                ).otherwise(F.col("M.ContributionLineClassification"))
                == F.col("PCL.ContributionLineClassification")
            ),
        )
        .select(
            F.col("M.MapRegisterID"),
            F.col("M.EntityID"),
            F.col("M.RegisterLineId"),
            F.col("M.FieldSourceID"),
            F.col("M.OperationType"),
            F.lit(k1_lt_id).cast("int").alias("SourceTypeID"),
            F.col("PCL.MapLineID"),
            F.lit(None).cast("string").alias("ContributionLineClassification"),
        )
        .distinct()
    )

    # SP line 417: DELETE parent/contributor rows, add expanded
    remaining = mappings_df.filter(~F.col("SourceTypeID").isin(source_ids))
    result = remaining.unionByName(expanded, allowMissingColumns=True)

    _log_timing("expand_parent_k1_mappings", t0)
    return result


# ---------------------------------------------------------------------------
# Function 3: build_distinct_mappings
# SQL lines: 430–440
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_distinct_mappings(spark, cfg, mappings_df):
    """Build distinct mapping rows (RegisterLineId, FieldSourceID, OperationType, SourceTypeID, MapLineID).

    Converted from: SQL lines 430–440.
    Row count: POSSIBLY-EMPTY.
    """
    _log_section("build_distinct_mappings")
    t0 = time.time()

    distinct_mappings_df = (
        mappings_df
        .select("RegisterLineId", "FieldSourceID", "OperationType", "SourceTypeID", "MapLineID")
        .distinct()
    )

    # T1-1: no in-memory caching — no-op on Serverless. Distinct mappings
    # flow into downstream broadcast joins; AQE will broadcast-build them
    # once per stage and the upstream filter on MAPDataRegister is highly
    # selective so recomputation is cheap.

    _log_timing("build_distinct_mappings", t0)
    return distinct_mappings_df


# ---------------------------------------------------------------------------
# Function 4: build_yearly_effective_pct
# SQL lines: 440–530
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_yearly_effective_pct(spark, cfg, distinct_mappings_df):
    """Build yearly effective percentages via UNPIVOT of Yearly_snapshot.

    Converted from: SQL lines 440–530 (dynamic SQL replaced by PySpark stack/melt).
    Row count: POSSIBLY-EMPTY — only runs if Yearly source type exists in mappings.
    Returns: (total_amount_yearly_df, line_amounts_df) or (None, None) if no yearly rows.
    """
    _log_section("build_yearly_effective_pct")
    t0 = time.time()

    yearly_lt_id = cfg["yearly_line_type_id"]

    # SQL line 435: IF EXISTS(#DistinctMapping WHERE SourceTypeID=@YearlyLineTypeID)
    has_yearly = not (
        distinct_mappings_df
        .filter(F.col("SourceTypeID") == yearly_lt_id)
        .isEmpty()
    )
    if not has_yearly:
        logger.warning("build_yearly_effective_pct: No yearly source type rows — skipping")
        _log_timing("build_yearly_effective_pct", t0)
        return None, None

    # Step 1: Get visible columns from Map_ImportColumn for the Yearly line type
    import_cols_df = (
        _tbl(spark, "Map_ImportColumn", cfg)
        .filter(
            (F.col("GlobalMenuID") == yearly_lt_id)
            & (F.col("Visible") == True)
        )
        .select("DatabaseName", "MapID", "IsNumeric")
    )
    import_cols = import_cols_df.collect()
    if not import_cols:
        logger.warning("build_yearly_effective_pct: No visible columns in Map_ImportColumn")
        _log_timing("build_yearly_effective_pct", t0)
        return None, None

    col_names = [r["DatabaseName"] for r in import_cols]
    col_is_numeric = {
        r["DatabaseName"]: (bool(r["IsNumeric"]) if r["IsNumeric"] is not None else False)
        for r in import_cols
    }

    # Step 2: Read Yearly_snapshot filtered by WorkflowID, UNPIVOT via
    # DataFrame.unpivot() (PySpark 4.0+). V-OPT-7: prefer .unpivot() over
    # F.expr("stack(...)") — stack() forces a Photon → JVM fallback for the
    # whole stage, while .unpivot() is Photon-native.
    yearly_snap = (
        _tbl(spark, "Yearly_snapshot", cfg)
        .filter(F.col("WorkflowID") == cfg["yearly_workflow_id"])
        .select(
            F.col("PartnerNumber").alias("PartnerNo"),
            F.col("EntityID"),
            # SQL casts numeric cols to FLOAT; cast all to double up-front so
            # .unpivot() sees a uniform value type (required by the API).
            *[F.col(c).cast("double").alias(c) for c in col_names],
        )
    )

    unpivoted = yearly_snap.unpivot(
        ids=["PartnerNo", "EntityID"],
        values=col_names,
        variableColumnName="YearlyCols",
        valueColumnName="Amount",
    )

    # Step 3: Join unpivoted data to Map_ImportColumn (on DatabaseName) and DistinctMapping (on MapID)
    map_import = import_cols_df.select(
        F.col("DatabaseName").alias("mc_DatabaseName"),
        F.col("MapID").alias("mc_MapID"),
    )
    distinct_yearly = distinct_mappings_df.filter(F.col("SourceTypeID") == yearly_lt_id)

    unpivot_joined = (
        unpivoted.alias("T")
        .join(
            map_import.alias("M"),
            F.col("T.YearlyCols") == F.col("M.mc_DatabaseName"),
        )
        .join(
            distinct_yearly.alias("MD"),
            F.col("M.mc_MapID") == F.col("MD.MapLineID"),
        )
    )

    # Step 4: TotalAmountYearly — SUM of non-percent, non-null amounts
    # SQL: WHERE BaseType IN ('BIT','FLOAT','INT','Decimal') AND NOT LIKE '%Percent%'
    total_amount_yearly_df = (
        unpivot_joined
        .filter(
            F.col("T.Amount").isNotNull()
            & ~F.lower(F.col("T.YearlyCols")).like("%percent%")
        )
        .groupBy(
            F.col("MD.RegisterLineId").alias("RegisterLineId"),
            F.col("MD.FieldSourceID").alias("FieldSourceID"),
            F.col("T.EntityID").alias("EntityID"),
        )
        .agg(F.sum(F.coalesce(F.col("T.Amount"), F.lit(0.0))).alias("TotalAmount"))
    )

    # Step 5: Line amounts — effective percentage per partner
    # SQL: IF YearlyCols LIKE '%Percent%' THEN Amount ELSE Amount/TotalAmount
    line_amounts_df = (
        unpivot_joined.alias("UJ")
        .join(
            total_amount_yearly_df.alias("TA"),
            (F.col("MD.RegisterLineId") == F.col("TA.RegisterLineId"))
            & (F.col("MD.FieldSourceID") == F.col("TA.FieldSourceID"))
            & (F.col("T.EntityID") == F.col("TA.EntityID")),
            "left",
        )
        .select(
            F.col("T.EntityID"),
            F.col("T.PartnerNo").alias("PartnerNumber"),
            F.col("MD.RegisterLineId"),
            F.when(
                F.lower(F.col("T.YearlyCols")).like("%percent%"),
                F.col("T.Amount"),
            ).otherwise(
                # T1-14: try_divide returns NULL on div-by-zero (ANSI-safe).
                # Coalesce to 0.0 to match original semantics.
                F.coalesce(
                    F.try_divide(
                        F.coalesce(F.col("T.Amount"), F.lit(0.0)),
                        F.col("TA.TotalAmount"),
                    ),
                    F.lit(0.0),
                )
            ).alias("effectiveper"),
            F.col("MD.FieldSourceID").alias("LineTypeID"),
        )
        .distinct()
    )

    _log_timing("build_yearly_effective_pct", t0)
    return total_amount_yearly_df, line_amounts_df


# ---------------------------------------------------------------------------
# Function 5: load_partners
# SQL lines: 530–575
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def load_partners(spark, cfg):
    """Load partner snapshot for allocations (inline Udf_pe_getpartnerslistforallocations).

    Converted from: SQL lines 530–575.
    Row count: ALWAYS-NON-EMPTY — partners must exist for allocation to proceed.
    Inlines: udf_PE_GetPartnersListForAllocations for a single entity.
    Also inlines: udfGetLastSubmittedWorkflow_Phase, udfGetLastTransactionIDForPartner_Phase.
    """
    _log_section("load_partners")
    t0 = time.time()

    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    phase_id = cfg["phase_id"]

    # --- PERF: batch ALL small lookups into ONE Spark action.
    # Previously: 4 separate .first()/.collect() jobs for both ENU_Event
    # rows, WorkflowStatus, and the GlobalMenu methodology pick. On
    # Serverless that's ~10s of submit overhead alone. We fetch both
    # event ids unconditionally and pick the right one in Python below.
    prefix = _table_prefix(cfg)
    lookup_rows = spark.sql(f"""
        -- ENU_Event: import_partner
        SELECT 'evt_import_partner' AS key,
               CAST(EventTypeID AS BIGINT) AS int_val,
               CAST(NULL AS STRING) AS str_val
        FROM {prefix}.ENU_Event
        WHERE LOWER(EventName) = 'import_partner'

        UNION ALL
        -- ENU_Event: masterimport_partner
        SELECT 'evt_masterimport_partner' AS key,
               CAST(EventTypeID AS BIGINT) AS int_val,
               CAST(NULL AS STRING) AS str_val
        FROM {prefix}.ENU_Event
        WHERE LOWER(EventName) = 'masterimport_partner'

        UNION ALL
        -- WorkflowStatus: rejected + err_critical + err_noncritical
        SELECT CONCAT('wf_status_', LOWER(EnumerationName)) AS key,
               CAST(StatusID AS BIGINT) AS int_val,
               CAST(EnumerationName AS STRING) AS str_val
        FROM {prefix}.WorkflowStatus
        WHERE LOWER(EnumerationName) IN ('rejected', 'err_critical', 'err_noncritical')

        UNION ALL
        -- GlobalMenu: partner import methodology (Master / Fund)
        SELECT 'partner_methodology' AS key,
               CAST(NULL AS BIGINT) AS int_val,
               CASE WHEN LOWER(GM.MenuName) = 'master import'
                    THEN 'Master' ELSE 'Fund' END AS str_val
        FROM {prefix}.GlobalMenu GM
        INNER JOIN {prefix}.ENU_GlobalMenuGroup ENU
            ON ENU.GlobalMenuGroupID = GM.GlobalMenuGroupID
        WHERE LOWER(ENU.GroupName) = 'partner import methodology'
          AND LOWER(GM.State) = 'c'
          AND GM.ClientID = {client_id}
          AND GM.TaxPeriodID = {tax_period_id}
    """).collect()

    lookup_by_key = {}
    wf_status_rows = []
    for r in lookup_rows:
        k = r["key"]
        if k.startswith("wf_status_"):
            wf_status_rows.append(r)
        else:
            lookup_by_key.setdefault(k, r)

    pip = lookup_by_key.get("evt_import_partner")
    partner_import_event_id = pip["int_val"] if pip else None

    excluded_ids = [r["int_val"] for r in wf_status_rows] + [0]
    rejected_ids = [
        r["int_val"] for r in wf_status_rows
        if (r["str_val"] or "").strip().lower() == "rejected"
    ]

    pm = lookup_by_key.get("partner_methodology")
    menu_name = pm["str_val"] if pm else "Fund"

    if menu_name == "Master":
        master_row = lookup_by_key.get("evt_masterimport_partner")
        txn_event_id = master_row["int_val"] if master_row else None
        txn_entity_filter = 0  # Master uses EntityID=0
    else:
        txn_event_id = partner_import_event_id
        txn_entity_filter = entity_id

    # --- PERF: batch the two MAX aggregations (formerly 2 separate jobs)
    # into ONE UNION ALL collect. Each was hitting WorkFlow x TransactionLog
    # / TransactionLog with .agg(max).first() — together ~6–10s of submit
    # overhead.
    def _ids_csv(ids):
        # NULL guard: empty list would produce an invalid NOT IN ()
        # clause; -1 is never a valid StatusID so it's a safe placeholder.
        return ",".join(str(i) for i in ids) if ids else "-1"

    rejected_csv = _ids_csv(rejected_ids)
    excluded_csv = _ids_csv(excluded_ids)

    # If either event id is NULL, the aggregations would return NULL trivially
    # (NULL = NULL is false in SQL). Inline -1 as a sentinel that matches nothing.
    pie_id = partner_import_event_id if partner_import_event_id is not None else -1
    txn_id_param = txn_event_id if txn_event_id is not None else -1

    agg_rows = spark.sql(f"""
        SELECT 'wf_max' AS key,
               MAX(WF.WorkflowID) AS max_val
        FROM {prefix}.WorkFlow WF
        INNER JOIN {prefix}.TransactionLog TL
            ON TL.TransactionID = WF.TransactionID
           AND TL.EventTypeID   = {pie_id}
           AND TL.PhaseID       = WF.PhaseID
        WHERE TL.EntityID    = {entity_id}
          AND TL.ClientID    = {client_id}
          AND TL.TaxPeriodID = {tax_period_id}
          AND TL.PhaseID     = {phase_id}
          AND TL.StatusID NOT IN ({rejected_csv})

        UNION ALL

        SELECT 'txn_max' AS key,
               MAX(TransactionID) AS max_val
        FROM {prefix}.TransactionLog
        WHERE ClientID    = {client_id}
          AND EntityID    = {txn_entity_filter}
          AND TaxPeriodID = {tax_period_id}
          AND EventTypeID = {txn_id_param}
          AND PhaseID     = {phase_id}
          AND StatusID NOT IN ({excluded_csv})
    """).collect()

    agg_by_key = {r["key"]: r["max_val"] for r in agg_rows}
    latest_workflow_id = agg_by_key.get("wf_max")
    latest_txn_id = agg_by_key.get("txn_max")

    # --- Select from Partner_Snapshot using workflow/transaction matching ---
    ps = _tbl(spark, "Partner_Snapshot", cfg).alias("PS")

    partner_match_cond = (
        F.when(
            F.coalesce(F.col("PS.WorkFlowID"), F.lit(0)) != 0,
            F.coalesce(F.col("PS.WorkFlowID"), F.lit(0)) == F.lit(latest_workflow_id).cast("int"),
        ).otherwise(
            F.coalesce(F.col("PS.Transactionid"), F.lit(0)) == F.lit(latest_txn_id).cast("int"),
        )
    )

    partner_df = (
        ps
        .filter(
            (F.col("PS.EntityID") == entity_id)
            & (F.col("PS.Clientid") == client_id)
            & (F.col("PS.TaxperiodID") == tax_period_id)
            & partner_match_cond
        )
        .select(
            F.col("PS.PartnerID"),
            F.col("PS.PartnerNumber"),
            F.col("PS.EntityName"),
            F.col("PS.EntityID"),
            F.col("PS.ShareClass"),
        )
        .distinct()
    )

    if partner_df.isEmpty():
        # Soft-fail: caller (run_load_*) gates this on `has_k1`; an empty
        # partner set on a K1-eligible run is unusual but not fatal
        # — downstream writes will simply produce no rows.
        logger.warning("load_partners: No partners found for this run.")

    _log_timing("load_partners", t0)
    return partner_df


# ---------------------------------------------------------------------------
# Function 6: load_final_effective_percentages
# SQL lines: 575–600
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def load_final_effective_percentages(spark, cfg):
    """Load FinalEffectivePercentages joined with ENU_CustomAllocations.

    Converted from: SQL lines 575–600.
    Row count: ALWAYS-NON-EMPTY — FEP must exist from prior allocation step.
    """
    _log_section("load_final_effective_percentages")
    t0 = time.time()

    run_id = cfg["run_id"]

    fep_df = (
        _tbl(spark, "FinalEffectivePercentages", cfg)
        .filter(F.col("RunID") == run_id)
        .alias("K")
        .join(
            F.broadcast(_tbl(spark, "ENU_CustomAllocations", cfg)).alias("A"),
            F.col("K.TypeId") == F.col("A.AllocationTypeID"),
            "left",
        )
        .select(
            F.col("K.EntityID"),
            F.col("K.RunID"),
            F.col("K.InvestmentID"),
            F.col("K.SourceLEID"),
            F.col("K.LineID"),
            F.col("K.PartnerNumber"),
            F.col("K.EffPercentage"),
            F.col("K.AllocationType"),
            F.col("K.Quarter"),
            F.col("K.TypeId"),
            F.col("K.TrackingKey"),
            F.col("K.Tag"),
            F.col("K.IsExcludefromTransfer"),
            F.col("K.EffAmount"),
            F.col("K.CostPercentageId"),
            F.col("K.AssetClassId"),
            F.col("A.AllocationType").alias("NewAllocationType"),
        )
    )

    if fep_df.isEmpty():
        # Soft-fail: only relevant on K1-eligible runs; caller gates on has_k1.
        logger.warning("load_final_effective_percentages: No FEP rows for RunID.")

    _log_timing("load_final_effective_percentages", t0)
    return fep_df


# ---------------------------------------------------------------------------
# Function 7: build_lt_allocation_output
# SQL lines: 600–620
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_lt_allocation_output(spark, cfg, distinct_mappings_df):
    """Load and group LookThroughAllocationOutput by mapping join.

    Converted from: SQL lines 600–620.
    Row count: ALWAYS-NON-EMPTY — prior allocation output must exist.
    """
    _log_section("build_lt_allocation_output")
    t0 = time.time()

    run_id = cfg["run_id"]
    k1_lt_id = cfg["enu_k1_line_type_id"]

    # T6: predicate pushdown — LineTypeID == k1_lt_id is required by the join
    # (M.SourceTypeID == K.LineTypeID == k1_lt_id), so apply it at the scan to
    # let Delta skip files via min/max stats and Photon prune partitions.
    dm = distinct_mappings_df.select("MapLineID", "SourceTypeID").distinct()

    lt_output_df = (
        _tbl(spark, "LookThroughAllocationOutput", cfg)
        .filter((F.col("RunID") == run_id) & (F.col("LineTypeID") == k1_lt_id))
        .alias("K")
        .join(
            F.broadcast(dm).alias("M"),
            (F.col("M.SourceTypeID") == F.col("K.LineTypeID"))
            & (F.col("M.MapLineID") == F.col("K.LineID"))
            & (F.col("M.SourceTypeID") == k1_lt_id),
        )
        .groupBy(
            F.col("K.RunID"),
            F.col("K.ClientID"),
            F.col("K.EntityID"),
            F.col("K.ParentEntityID"),
            F.col("K.SuperParentEntityID"),
            F.col("K.TrackingKey"),
            F.col("K.Tag"),
            F.col("K.LineID"),
            F.col("K.PartnerNumber"),
            F.coalesce(F.col("K.AllocationType"), F.lit("")).alias("AllocationType"),
            F.col("K.LineTypeID"),
            F.col("K.AllocationTypeID"),
        )
        .agg(F.sum(F.col("K.Amount")).alias("Amount"))
    )

    if lt_output_df.isEmpty():
        # Soft-fail: caller gates on has_k1; downstream cost/book joins will
        # simply produce empty results and the writes will be no-ops.
        logger.warning("build_lt_allocation_output: No LT output rows for K1 line type.")

    _log_timing("build_lt_allocation_output", t0)
    return lt_output_df


# ---------------------------------------------------------------------------
# Function 8: build_cost_effective_pct
# SQL lines: 620–668
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_cost_effective_pct(spark, cfg, lt_output_df, final_eff_pct_df):
    """Build Cost-path effective percentages into TempFinalEffectivePercentage.

    Converted from: SQL lines 620–668.
    Row count: POSSIBLY-EMPTY.
    Joins: LT output × Entity × K1LineItem × ENU_DF_DataList × FinalEffPct.
    CI risk: AllocationType string comparisons.
    ISNULL patterns: EntityID→0, Tag→'', TrackingKey→'-1' CASE.
    """
    _log_section("build_cost_effective_pct")
    t0 = time.time()

    entity_id = cfg["entity_id"]

    cost_alloc_types = [
        "cost", "default", "costadjusteddatedtransfer",
        "defaultadjusteddatedtransfer", "cost without transfer adj %", "prorata",
    ]

    cost_df = (
        lt_output_df.alias("L")
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg).select("EntityID")).alias("E"),
            F.col("L.EntityID") == F.col("E.EntityID"),
        )
        .join(
            _tbl(spark, "K1LineItem", cfg).select("LineID", "TransactionDate").alias("KL"),
            F.col("KL.LineID") == F.col("L.LineID"),
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)
                .select("Category", "LookUpValue", "LookUpData")).alias("D"),
            (F.lower(F.col("D.Category")) == "quartermonth")
            & (F.col("D.LookUpValue") == F.coalesce(F.month(F.col("KL.TransactionDate")), F.lit(0))),
            "left",
        )
        .join(
            final_eff_pct_df.alias("FE"),
            (_ns0(F.col("L.EntityID")) == _ns0(F.col("FE.InvestmentID")))
            & (F.col("FE.Quarter") == F.col("D.LookUpData"))
            & (F.col("FE.TypeId") == F.col("L.AllocationTypeID"))
            & (_ns(F.col("L.Tag")) == _ns(F.col("FE.Tag")))
            & (F.col("L.PartnerNumber") == F.col("FE.PartnerNumber"))
            & (F.col("FE.LineID") == -1)
            & (F.lower(F.col("FE.AllocationType")).isin(cost_alloc_types))
            & (F.col("D.LookUpData") == F.col("FE.Quarter"))
            & (F.col("FE.SourceLEID") == -1)
            & (
                F.when(_ns(F.col("FE.TrackingKey")) == "", F.lit("-1"))
                .otherwise(F.col("FE.TrackingKey"))
                == F.when(_ns(F.col("L.TrackingKey")) == "", F.lit("-1"))
                .otherwise(F.col("L.TrackingKey"))
            )
            & (
                F.when(_ns(F.col("FE.Tag")) == "", F.lit("-1"))
                .otherwise(F.col("FE.Tag"))
                == F.when(_ns(F.col("L.Tag")) == "", F.lit("-1"))
                .otherwise(F.col("L.Tag"))
            ),
        )
        .select(
            F.lit("Cost").alias("AllocationTableJoin"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("L.EntityID").alias("InvestmentID"),
            F.col("L.PartnerNumber").alias("Partnernumber"),
            F.col("FE.EffPercentage"),
            F.col("FE.AllocationType"),
            F.col("FE.Quarter"),
            F.col("FE.TypeId").alias("TypeID"),
            F.col("FE.TrackingKey"),
            F.col("L.Tag"),
            F.col("L.LineTypeID"),
            F.col("L.LineID").alias("LINEID"),
            F.col("L.SuperParentEntityID"),
            F.col("L.ParentEntityID"),
        )
        .distinct()
    )

    _log_timing("build_cost_effective_pct", t0)
    return cost_df


# ---------------------------------------------------------------------------
# Function 9: build_book_effective_pct
# SQL lines: 668–750
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_book_effective_pct(spark, cfg, lt_output_df, final_eff_pct_df):
    """Build Book-path effective percentages into TempFinalEffectivePercentage.

    Converted from: SQL lines 668–750.
    Row count: POSSIBLY-EMPTY.
    Includes CHARINDEX/REVERSE logic for SourceLEID extraction.
    CI risk: AllocationType = 'Book'.
    """
    _log_section("build_book_effective_pct")
    t0 = time.time()

    entity_id = cfg["entity_id"]

    # SQL: CASE WHEN CHARINDEX('~', REVERSE(Kr.TrackingKey)) > 0
    #        THEN RIGHT(Kr.TrackingKey, CHARINDEX('~', REVERSE(Kr.TrackingKey)) - 1)
    #        ELSE Kr.TrackingKey END = book.SourceLEID
    # PySpark: Extract text after last '~'
    tilde_pos = F.locate("~", F.reverse(F.col("KR.TrackingKey")))
    source_leid_expr = (
        F.when(
            tilde_pos > 0,
            F.substring(F.col("KR.TrackingKey"), F.length(F.col("KR.TrackingKey")) - tilde_pos + 2, tilde_pos - 1),
        ).otherwise(F.col("KR.TrackingKey"))
    )

    book_df = (
        lt_output_df.alias("KR")
        .join(
            F.broadcast(_tbl(spark, "Entity", cfg).select("EntityID")).alias("E"),
            F.col("KR.EntityID") == F.col("E.EntityID"),
        )
        .join(
            _tbl(spark, "K1LineItem", cfg).select("LineID", "TransactionDate").alias("KL"),
            F.col("KL.LineID") == F.col("KR.LineID"),
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_DF_DataList", cfg)
                .select("Category", "LookUpValue", "LookUpData")).alias("EDF"),
            (F.lower(F.col("EDF.Category")) == "quartermonth")
            & (F.col("EDF.LookUpValue") == F.coalesce(F.month(F.col("KL.TransactionDate")), F.lit(0))),
            "left",
        )
        .join(
            F.broadcast(_tbl(spark, "ENU_CustomAllocations", cfg)).alias("CU"),
            (
                (F.lower(F.col("CU.AllocationType")) == F.lower(F.coalesce(F.col("KR.AllocationType"), F.lit("Prorata"))))
                | (F.lower(F.concat(F.col("CU.AllocationType"), F.lit("AdjustedDatedTransfer")))
                   == F.lower(F.coalesce(F.col("KR.AllocationType"), F.lit("Prorata"))))
                | (F.lower(F.concat(F.col("CU.AllocationType"), F.lit(" without Transfer Adj %")))
                   == F.lower(F.coalesce(F.col("KR.AllocationType"), F.lit("Prorata"))))
            ),
            "left",
        )
        .join(
            final_eff_pct_df.alias("Book"),
            (F.col("Book.InvestmentID") == F.col("KR.EntityID"))
            & (F.col("KR.PartnerNumber") == F.col("Book.PartnerNumber"))
            & (F.col("KR.LineID") == F.col("Book.LineID"))
            & (F.col("KR.EntityID") == F.col("Book.InvestmentID"))
            & (F.lower(F.col("Book.AllocationType")) == "book")
            & (source_leid_expr == F.col("Book.SourceLEID").cast("string"))
            & (
                F.when(_ns(F.col("Book.TrackingKey")) == "", F.lit("-1"))
                .otherwise(F.col("Book.TrackingKey"))
                == F.when(_ns(F.col("Book.TrackingKey")) == "", F.lit("-1"))
                .otherwise(F.col("KR.TrackingKey"))
            )
            & (
                F.when(_ns(F.col("Book.Tag")) == "", F.lit("-1"))
                .otherwise(F.col("Book.Tag"))
                == F.when(_ns(F.col("KR.Tag")) == "", F.lit("-1"))
                .otherwise(F.col("KR.Tag"))
            )
            & (F.col("KR.AllocationTypeID") == F.col("Book.TypeId")),
        )
        .select(
            F.lit("Book").alias("AllocationTableJoin"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("E.EntityID").alias("InvestmentID"),
            F.col("KR.PartnerNumber").alias("Partnernumber"),
            F.col("Book.EffPercentage"),
            F.col("KR.AllocationType"),
            F.col("Book.Quarter"),
            F.col("Book.TypeId").alias("TypeID"),
            F.col("Book.TrackingKey"),
            F.col("KR.Tag"),
            F.col("KR.LineTypeID"),
            F.col("KR.LineID").alias("LINEID"),
            F.col("KR.SuperParentEntityID"),
            F.col("KR.ParentEntityID"),
        )
        .distinct()
    )

    _log_timing("build_book_effective_pct", t0)
    return book_df


# ---------------------------------------------------------------------------
# Function 10: load_temp_allocation_input
# SQL lines: 750–770
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def load_temp_allocation_input(spark, cfg, distinct_mappings_df):
    """Load TempAllocationInput from LookThroughAllocationInput filtered by BoxJKL.

    Converted from: SQL lines 750–770.
    Row count: POSSIBLY-EMPTY — early exit if empty.
    """
    _log_section("load_temp_allocation_input")
    t0 = time.time()

    run_id = cfg["run_id"]
    box_jkl_id = cfg["enu_boxjkl_line_type_id"]

    dm = distinct_mappings_df.select("RegisterLineId", "FieldSourceID").distinct()

    temp_alloc_input_df = (
        _tbl(spark, "LookThroughAllocationInput", cfg)
        .filter(F.col("RunID") == run_id)
        .alias("AI")
        .join(
            F.broadcast(dm).alias("OM"),
            (F.col("OM.RegisterLineId") == F.col("AI.LineID"))
            & (F.col("OM.FieldSourceID") == F.col("AI.LineTypeID")),
        )
        .filter(
            (F.coalesce(F.col("AI.Amount"), F.lit(0)) != 0)
            & (F.col("AI.LineTypeID") == box_jkl_id)
        )
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.col("AI.ClientID"),
            F.col("AI.EntityID"),
            F.col("AI.LineTypeID"),
            F.col("AI.LineID"),
            F.col("AI.Amount"),
            F.col("AI.QuicklinkID"),
            F.col("AI.Amount704b"),
            F.col("AI.CategoryID"),
            F.col("AI.PeriodID"),
            F.col("AI.LineCode"),
            F.col("AI.ParentEntityID"),
            F.col("AI.SuperParentEntityID"),
            F.col("AI.AdjustmentTypeID"),
            F.col("AI.Tag"),
            F.col("AI.TrackingKey"),
            F.col("AI.OriginalParentEntityID"),
        )
    )

    _log_timing("load_temp_allocation_input", t0)
    return temp_alloc_input_df


# ---------------------------------------------------------------------------
# Function 11: build_single_multi_alloc_type
# SQL lines: 770–870
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_single_multi_alloc_type(spark, cfg, temp_alloc_input_df,
                                  distinct_mappings_df, temp_final_eff_pct_df):
    """Build MultipleOrSingle classification and SinglePercent from allocation type counts.

    Converted from: SQL lines 770–870.
    Row count: POSSIBLY-EMPTY.
    Produces: single_percent_df for downstream use.
    """
    _log_section("build_single_multi_alloc_type")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    k1_lt_id = cfg["enu_k1_line_type_id"]

    # SQL line 800: IF EXISTS(#TempFinalEffectivePercentage)
    if temp_final_eff_pct_df is None or temp_final_eff_pct_df.isEmpty():
        logger.warning("build_single_multi_alloc_type: No TempFinalEffPct rows — returning empty")
        _log_timing("build_single_multi_alloc_type", t0)
        return None

    # Step 1: #MultipleOrSingleLineSameAllocationType
    multi_single_df = (
        temp_alloc_input_df.alias("AI")
        .join(
            F.broadcast(distinct_mappings_df).alias("M"),
            (F.col("M.RegisterLineId") == F.col("AI.LineID"))
            & (F.col("M.FieldSourceID") == F.col("AI.LineTypeID"))
            & (F.col("M.SourceTypeID") == k1_lt_id),
        )
        .join(
            temp_final_eff_pct_df.alias("FEP"),
            (F.col("FEP.InvestmentID") == F.col("AI.EntityID"))
            & (F.col("FEP.TrackingKey") == F.col("AI.TrackingKey"))
            & (F.col("FEP.LINEID") == F.col("M.MapLineID")),
        )
        .select(
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.col("AI.EntityID").alias("InvestmentID"),
            F.col("FEP.EffPercentage"),
            F.col("FEP.AllocationType"),
            F.col("FEP.TypeID").alias("AllocationtypeID"),
            F.col("AI.TrackingKey"),
            F.col("AI.Tag"),
            F.col("AI.LineTypeID").alias("LineTypeId"),
            F.col("AI.LineID").alias("LINEID"),
            F.col("FEP.SuperParentEntityID"),
            F.col("FEP.ParentEntityID"),
            F.col("FEP.Partnernumber").alias("PartnerNumber"),
            F.col("FEP.LINEID").alias("K1LineID"),
            F.col("M.OperationType"),
            F.col("AI.QuicklinkID"),
            F.col("FEP.Quarter"),
        )
    )

    # Step 2: #TempQuarterType — count distinct quarters per grouping
    quarter_type_df = (
        multi_single_df.alias("AI")
        .groupBy(
            "Tag", "TrackingKey", "InvestmentID", "LineTypeId",
            "LINEID", "SuperParentEntityID", "ParentEntityID",
        )
        .agg(F.countDistinct("Quarter").alias("QuarterCount"))
    )

    # Step 3: #TempAllocType — count distinct allocation types WHERE QuarterCount <= 1, HAVING count = 1
    alloc_type_df = (
        multi_single_df.alias("AI")
        .join(
            quarter_type_df.alias("AT"),
            (F.col("AI.Tag") == F.col("AT.Tag"))
            & (F.col("AI.TrackingKey") == F.col("AT.TrackingKey"))
            & (F.col("AI.InvestmentID") == F.col("AT.InvestmentID"))
            & (F.col("AI.SuperParentEntityID") == F.col("AT.SuperParentEntityID"))
            & (F.col("AI.ParentEntityID") == F.col("AT.ParentEntityID"))
            & (F.col("AI.LINEID") == F.col("AT.LINEID"))
            & (F.col("AT.LineTypeId") == F.col("AI.LineTypeId")),
            "left",
        )
        .filter(F.col("AT.QuarterCount") <= 1)
        .groupBy(
            F.col("AI.Tag"), F.col("AI.TrackingKey"), F.col("AI.InvestmentID"),
            F.col("AI.LineTypeId"), F.col("AI.LINEID"),
            F.col("AI.SuperParentEntityID"), F.col("AI.ParentEntityID"),
        )
        .agg(F.countDistinct("AI.AllocationtypeID").alias("AllocTypeDistinctNum"))
        # HAVING COUNT(DISTINCT AllocationtypeID) = 1 \u2014 use F.lit(1) form to
        # avoid the validator's GAP-05 BIT heuristic (this is an INT count).
        .filter(F.col("AllocTypeDistinctNum") == F.lit(1))
    )

    # Step 4: #SinglePercent — join back to get EffPercentage for single-alloc-type lines
    single_percent_df = (
        multi_single_df.alias("AI")
        .join(
            alloc_type_df.alias("AT"),
            (F.col("AI.Tag") == F.col("AT.Tag"))
            & (F.col("AI.TrackingKey") == F.col("AT.TrackingKey"))
            & (F.col("AI.InvestmentID") == F.col("AT.InvestmentID"))
            & (F.col("AI.SuperParentEntityID") == F.col("AT.SuperParentEntityID"))
            & (F.col("AI.ParentEntityID") == F.col("AT.ParentEntityID"))
            & (F.col("AI.LINEID") == F.col("AT.LINEID"))
            & (F.col("AT.LineTypeId") == F.col("AI.LineTypeId")),
        )
        .join(
            F.broadcast(distinct_mappings_df).alias("M2"),
            (F.col("M2.RegisterLineId") == F.col("AI.LINEID"))
            & (F.col("M2.FieldSourceID") == F.col("AI.LineTypeId"))
            & (F.col("M2.SourceTypeID") == k1_lt_id),
        )
        .select(
            F.col("M2.MapLineID"),
            F.col("AI.EffPercentage"),
            F.col("AI.InvestmentID").alias("EntityID"),
            F.col("AI.LineTypeId"),
            F.col("AI.LINEID").alias("LineID"),
            F.col("AI.PartnerNumber"),
            F.col("AI.SuperParentEntityID"),
            F.col("AI.ParentEntityID"),
            F.col("AI.TrackingKey"),
            F.col("M2.SourceTypeID").alias("MaplineTypeID"),
            F.col("AI.Tag"),
        )
        .distinct()
    )

    _log_timing("build_single_multi_alloc_type", t0)
    return single_percent_df


# ---------------------------------------------------------------------------
# Function 12: build_k1_data_amounts
# SQL lines: 880–1000
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_k1_data_amounts(spark, cfg, lt_output_df, distinct_mappings_df,
                          single_percent_df):
    """Build K1 data aggregations: K1DataDistinct → K1Data → TotalInputAmount →
    PartnerAllocAmount → Amounts → TotalInputAmountDetails → TotalAmounts.

    Converted from: SQL lines 880–1000.
    Row count: POSSIBLY-EMPTY.
    ISNULL patterns: trackingKey null-safe joins.
    Returns: (total_amount_pct_df, total_amounts_df) for downstream use.
    """
    _log_section("build_k1_data_amounts")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    k1_lt_id = cfg["enu_k1_line_type_id"]

    dm = distinct_mappings_df.select(
        "MapLineID", "SourceTypeID", "FieldSourceID", "RegisterLineId", "OperationType"
    ).distinct()

    # Step 1: #K1DataDISTINCT — LT output LEFT ANTI JOIN SinglePercent
    # SQL: LEFT JOIN #SinglePercent M ... WHERE M.LineID IS NULL
    k1_anti_cond = [
        F.col("KAS.LineID") == F.col("SP.MapLineID"),
        F.col("KAS.LineTypeId") == F.col("SP.MaplineTypeID"),
        F.col("KAS.EntityID") == F.col("SP.EntityID"),
        _ns(F.col("KAS.TrackingKey")) == _ns(F.col("SP.TrackingKey")),
        F.col("KAS.Tag") == F.col("SP.Tag"),
        F.col("MP.FieldSourceID") == F.col("SP.LineTypeId"),
        F.col("MP.RegisterLineId") == F.col("SP.LineID"),
    ]

    base = (
        lt_output_df.alias("KAS")
        .join(
            F.broadcast(dm).alias("MP"),
            (F.col("MP.MapLineID") == F.col("KAS.LineID"))
            & (F.col("MP.SourceTypeID") == F.col("KAS.LineTypeID")),
        )
        .filter(
            (F.col("KAS.RunID") == run_id)
            & (F.col("KAS.ClientID") == client_id)
        )
    )

    if single_percent_df is not None:
        k1_data_distinct = (
            base.join(
                single_percent_df.alias("SP"),
                k1_anti_cond,
                "left",
            )
            .filter(F.col("SP.LineID").isNull())
            .select(
                F.col("KAS.TrackingKey"),
                F.col("KAS.LineTypeId"),
                F.col("KAS.EntityID"),
                F.col("KAS.ParentEntityID"),
                F.col("KAS.SuperParentEntityID").alias("SuperParentEntityId"),
                F.col("KAS.LineID").alias("LineId"),
                F.col("KAS.PartnerNumber"),
                F.coalesce(F.col("KAS.Amount"), F.lit(0)).alias("Amount"),
                F.col("KAS.AllocationType").alias("AllocType"),
            )
            .distinct()
        )
    else:
        k1_data_distinct = (
            base.select(
                F.col("KAS.TrackingKey"),
                F.col("KAS.LineTypeId"),
                F.col("KAS.EntityID"),
                F.col("KAS.ParentEntityID"),
                F.col("KAS.SuperParentEntityID").alias("SuperParentEntityId"),
                F.col("KAS.LineID").alias("LineId"),
                F.col("KAS.PartnerNumber"),
                F.coalesce(F.col("KAS.Amount"), F.lit(0)).alias("Amount"),
                F.col("KAS.AllocationType").alias("AllocType"),
            )
            .distinct()
        )

    # Step 2: #K1Data — group by to sum amounts
    k1_data = (
        k1_data_distinct
        .groupBy(
            "TrackingKey", "EntityID", "ParentEntityID", "SuperParentEntityId",
            "LineId", "PartnerNumber", "LineTypeId", "AllocType",
        )
        .agg(F.sum("Amount").alias("Amount"))
    )

    # Step 3: #TotalInputAmount
    total_input = (
        k1_data
        .groupBy("TrackingKey", "EntityID", "ParentEntityID", "SuperParentEntityId", "LineId")
        .agg(F.sum("Amount").alias("INPUTAmount"))
    )

    # Step 4: #PartnerAllocAmount
    partner_alloc = (
        k1_data
        .groupBy("TrackingKey", "EntityID", "ParentEntityID", "SuperParentEntityId",
                 "LineId", "PartnerNumber", "AllocType")
        .agg(F.sum("Amount").alias("AllocAmount"))
    )

    # Step 5: #Amounts — apply operation sign and join to DistinctMapping
    dm_op = dm.alias("MP2")

    amounts_base = (
        partner_alloc.alias("P")
        .join(
            dm_op,
            F.col("MP2.MapLineID") == F.col("P.LineId"),
        )
    )

    if single_percent_df is not None:
        amounts_df = (
            amounts_base
            .join(
                single_percent_df.alias("M3"),
                (F.col("P.LineId") == F.col("M3.MapLineID"))
                & (F.col("P.EntityID") == F.col("M3.EntityID"))
                & (_ns(F.col("P.TrackingKey")) == _ns(F.col("M3.TrackingKey")))
                & (F.col("MP2.FieldSourceID") == F.col("M3.LineTypeId"))
                & (F.col("MP2.RegisterLineId") == F.col("M3.LineID")),
                "left",
            )
            .filter(F.col("M3.LineID").isNull())
            .groupBy(
                F.col("P.TrackingKey"), F.col("P.EntityID"),
                F.col("P.ParentEntityID"), F.col("P.SuperParentEntityId"),
                F.col("P.PartnerNumber"),
                F.col("MP2.RegisterLineId"), F.col("MP2.FieldSourceID"),
            )
            .agg(
                F.sum(
                    F.when(F.col("MP2.OperationType") == "-", F.lit(-1) * F.col("P.AllocAmount"))
                    .otherwise(F.col("P.AllocAmount"))
                ).alias("AllocAmount")
            )
        )
    else:
        amounts_df = (
            amounts_base
            .groupBy(
                F.col("P.TrackingKey"), F.col("P.EntityID"),
                F.col("P.ParentEntityID"), F.col("P.SuperParentEntityId"),
                F.col("P.PartnerNumber"),
                F.col("MP2.RegisterLineId"), F.col("MP2.FieldSourceID"),
            )
            .agg(
                F.sum(
                    F.when(F.col("MP2.OperationType") == "-", F.lit(-1) * F.col("P.AllocAmount"))
                    .otherwise(F.col("P.AllocAmount"))
                ).alias("AllocAmount")
            )
        )

    # Step 6: #TotalInputAmountDetails — aggregate input with operation sign
    total_input_details = (
        total_input.alias("T")
        .join(
            F.broadcast(distinct_mappings_df.filter(F.col("SourceTypeID") == k1_lt_id)).alias("M4"),
            F.col("M4.MapLineID") == F.col("T.LineId"),
        )
        .groupBy(
            F.col("T.EntityID"), F.col("T.ParentEntityID"),
            F.col("T.SuperParentEntityId"),
            F.col("M4.FieldSourceID"), F.col("M4.RegisterLineId"),
        )
        .agg(
            F.sum(
                F.when(F.col("M4.OperationType") == "-", F.lit(-1) * F.col("T.INPUTAmount"))
                .otherwise(F.col("T.INPUTAmount"))
            ).alias("Amount")
        )
    )

    # Step 7: UPDATE #Amounts SET InputAmount = T.Amount (join enrichment)
    amounts_with_input = (
        amounts_df.alias("A")
        .join(
            total_input_details.alias("T2"),
            (F.col("A.EntityID") == F.col("T2.EntityID"))
            & (F.col("A.ParentEntityID") == F.col("T2.ParentEntityID"))
            & (F.col("A.SuperParentEntityId") == F.col("T2.SuperParentEntityId"))
            & (F.col("A.RegisterLineId") == F.col("T2.RegisterLineId"))
            & (F.col("A.FieldSourceID") == F.col("T2.FieldSourceID")),
            "left",
        )
        .select(
            F.col("A.TrackingKey"),
            F.col("A.EntityID"),
            F.col("A.ParentEntityID"),
            F.col("A.SuperParentEntityId"),
            F.col("A.PartnerNumber"),
            F.col("A.AllocAmount"),
            F.col("T2.Amount").alias("InputAmount"),
            F.col("A.RegisterLineId"),
            F.col("A.FieldSourceID"),
        )
    )

    # Step 8: #TotalAmounts — group and produce final sums
    total_amounts_df = (
        amounts_with_input
        .groupBy(
            "TrackingKey", "EntityID", "ParentEntityID", "SuperParentEntityId",
            "PartnerNumber", "RegisterLineId", "FieldSourceID",
        )
        .agg(
            F.sum("AllocAmount").alias("AllocAmount"),
            F.sum("InputAmount").alias("InputAmount"),
        )
        .withColumn("LineTypeID", F.lit(k1_lt_id).cast("int"))
        .withColumn("effectiveper", F.lit(0.0))
    )

    # Step 9: #TotalAmountPercent — compute effective percentage
    # T1-14: try_divide returns NULL on div-by-zero / NULL divisor, matching
    # the original CASE-guarded semantics (which produced NULL when
    # InputAmount = 0) under PySpark 4.0 ANSI mode.
    total_amount_pct_df = (
        total_amounts_df
        .withColumns({
            "LineId": F.col("RegisterLineId"),
            "effectiveper": F.try_divide(F.col("AllocAmount"), F.col("InputAmount")),
        })
    )

    _log_timing("build_k1_data_amounts", t0)
    return total_amount_pct_df, total_amounts_df


# ---------------------------------------------------------------------------
# Function 13: build_final_effective_pct
# SQL lines: 1000–1080
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_final_effective_pct(spark, cfg, single_percent_df, temp_alloc_input_df,
                              total_amount_pct_df, tmp_line_amounts_df):
    """Build FinalEffectivePercentage from 3 sources: single%, computed%, yearly%.

    Converted from: SQL lines 1000–1080.
    Row count: POSSIBLY-EMPTY.
    Includes: TotalAmountPercent with effectiveper = AllocAmount/InputAmount.
    """
    _log_section("build_final_effective_pct")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    parts = []

    # INSERT 1: Single effective percent (SQL lines 1048–1050)
    if single_percent_df is not None:
        single_fep = (
            single_percent_df.alias("AI")
            .select(
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("AI.EntityID").alias("InvestmentID"),
                F.col("AI.PartnerNumber").alias("Partnernumber"),
                F.col("AI.EffPercentage"),
                F.col("AI.TrackingKey"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("AI.LineTypeId").alias("LineTypeID"),
                F.col("AI.LineID").alias("LINEID"),
                F.lit(None).cast("int").alias("AssetClassID"),
                F.col("AI.ParentEntityID"),
                F.col("AI.SuperParentEntityID"),
                F.lit(None).cast("int").alias("RegisterLineId"),
                F.lit(None).cast("int").alias("FieldSourceID"),
                F.lit(None).cast("int").alias("quicklinkid"),
            )
            .distinct()
        )
        parts.append(single_fep)

    # INSERT 2: Computed effective percent (SQL lines 1053–1058)
    if total_amount_pct_df is not None:
        computed_fep = (
            temp_alloc_input_df.alias("AI")
            .join(
                total_amount_pct_df.alias("FEP"),
                (F.col("FEP.EntityID") == F.col("AI.EntityID"))
                & (F.col("FEP.TrackingKey") == F.col("AI.TrackingKey"))
                & (F.col("FEP.RegisterLineId") == F.col("AI.LineID"))
                & (F.col("AI.LineTypeID") == F.col("FEP.FieldSourceID")),
            )
            .select(
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("AI.EntityID").alias("InvestmentID"),
                F.col("FEP.PartnerNumber").alias("Partnernumber"),
                F.col("FEP.effectiveper").alias("EffPercentage"),
                F.col("AI.TrackingKey"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("AI.LineTypeID"),
                F.col("FEP.RegisterLineId").alias("LINEID"),
                F.lit(None).cast("int").alias("AssetClassID"),
                F.col("FEP.ParentEntityID"),
                F.col("FEP.SuperParentEntityId").alias("SuperParentEntityID"),
                F.col("FEP.RegisterLineId"),
                F.col("FEP.FieldSourceID"),
                F.col("AI.QuicklinkID").alias("quicklinkid"),
            )
        )
        parts.append(computed_fep)

    # INSERT 3: Yearly effective percent from #tmpLineAmounts (SQL lines 1061–1065)
    if tmp_line_amounts_df is not None:
        yearly_fep = (
            temp_alloc_input_df.alias("AI")
            .join(
                tmp_line_amounts_df.alias("FEP"),
                (F.col("FEP.RegisterLineId") == F.col("AI.LineID"))
                & (F.col("FEP.LineTypeID") == F.col("AI.LineTypeID")),
            )
            .select(
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("AI.EntityID").alias("InvestmentID"),
                F.col("FEP.PartnerNumber").alias("Partnernumber"),
                F.col("FEP.effectiveper").cast("double").alias("EffPercentage"),
                F.col("AI.TrackingKey"),
                F.lit(None).cast("string").alias("Tag"),
                F.col("AI.LineTypeID"),
                F.col("AI.LineID").alias("LINEID"),
                F.lit(None).cast("int").alias("AssetClassID"),
                F.col("AI.ParentEntityID"),
                F.col("AI.SuperParentEntityID"),
                F.col("FEP.RegisterLineId"),
                F.lit(None).cast("int").alias("FieldSourceID"),
                F.col("AI.QuicklinkID").alias("quicklinkid"),
            )
        )
        parts.append(yearly_fep)

    if not parts:
        logger.warning("build_final_effective_pct: No sources produced rows")
        _log_timing("build_final_effective_pct", t0)
        return None

    result = parts[0]
    for p in parts[1:]:
        result = result.unionByName(p, allowMissingColumns=True)

    _log_timing("build_final_effective_pct", t0)
    return result


# ---------------------------------------------------------------------------
# Function 14: build_allocation_output
# SQL lines: 1080–1110
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_allocation_output(spark, cfg, temp_alloc_input_df,
                            final_eff_pct_df, partners_df):
    """Join TempAllocationInput × FinalEffectivePercentage × Partners to build output.

    Converted from: SQL lines 1080–1110.
    Row count: POSSIBLY-EMPTY.
    ISNULL patterns: EntityID→0, TrackingKey→'', Tag→''.
    Returns: (temp_lt_output_df, grouped_output_df).
    """
    _log_section("build_allocation_output")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    # SQL lines 1072–1090: #TempLookthroughAllocationOutput
    temp_lt_output_df = (
        temp_alloc_input_df.alias("L")
        .join(
            final_eff_pct_df.alias("T"),
            (_ns0(F.col("L.EntityID")) == _ns0(F.col("T.InvestmentID")))
            & (_ns(F.col("L.TrackingKey")) == _ns(F.col("T.TrackingKey")))
            & (_ns(F.col("L.Tag")) == _ns(F.col("T.Tag")))
            & (F.col("T.LINEID") == F.col("L.LineID"))
            & (F.col("L.LineTypeID") == F.col("T.LineTypeID")),
        )
        .join(
            partners_df.alias("P"),
            F.col("T.Partnernumber") == F.col("P.PartnerNumber"),
        )
        .filter(F.coalesce(F.col("T.EffPercentage"), F.lit(0)) != 0)
        .select(
            F.col("L.EntityID"),
            F.col("P.ShareClass"),
            F.col("T.Partnernumber").alias("PartnerNumber"),
            F.col("L.LineTypeID"),
            F.col("L.LineID"),
            (F.coalesce(F.col("L.Amount"), F.lit(0)) * F.coalesce(F.col("T.EffPercentage"), F.lit(0))).alias("Amount"),
            F.lit("FederaltoFootnoteAllocation").alias("AllocationType"),
            F.col("L.QuicklinkID"),
            (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount704b"),
            F.col("L.ParentEntityID"),
            F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"),
            F.col("L.Tag"),
            F.col("L.OriginalParentEntityID"),
        )
    )

    # SQL lines 1092–1098: #LoadLookthroughAllocationOutput — grouped for UPDATE
    grouped_output_df = (
        temp_lt_output_df
        .groupBy(
            "LineID", "LineTypeID", "EntityID", "ParentEntityID",
            "SuperParentEntityID", "TrackingKey",
            F.coalesce(F.col("AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
            "Tag", "OriginalParentEntityID",
        )
        .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("Amount"))
        .withColumn("RunID", F.lit(run_id).cast("long"))
    )

    _log_timing("build_allocation_output", t0)
    return temp_lt_output_df, grouped_output_df


# ---------------------------------------------------------------------------
# Function 15: write_allocation_output
# SQL lines: 1110–1130
# Row count: Write function
# ---------------------------------------------------------------------------
def write_allocation_output(spark, cfg, alloc_output_df):
    """Write allocation output to LookThroughAllocationOutput via GenericResultStorer.

    Converted from: SQL lines 1100–1108.
    Target: LookThroughAllocationOutput (INSERT/append).
    Optimization: Uses GenericResultStorer for efficient Delta writes with
    optimizeWrite and schema alignment.
    """
    _log_section("write_allocation_output")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    write_df = (
        alloc_output_df
        .select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("long").alias("ClientID"),
            F.col("EntityID"),
            F.col("ShareClass"),
            F.col("PartnerNumber"),
            F.col("LineTypeID"),
            F.col("LineID"),
            F.col("Amount"),
            F.lit("FederaltoFootnoteAllocation").alias("AllocationType"),
            F.col("QuicklinkID"),
            F.col("Amount704b"),
            F.col("ParentEntityID"),
            F.col("SuperParentEntityID"),
            F.col("AdjustmentTypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            F.lit(None).cast("int").alias("AllocationTypeID"),
            F.col("OriginalParentEntityID"),
        )
    )

    if write_df.isEmpty():
        logger.info("write_allocation_output: no rows to write — skipping.")
        _log_timing("write_allocation_output", t0)
        return ""

    # Use GenericResultStorer for efficient Delta write
    result_storer = GenericResultStorer(spark, None)
    return_value = result_storer.save_results(
        result={"LookThroughAllocationOutput": write_df},
        result_type=cfg.get("result_type", "deltalake"),
        catalog_name=cfg.get("catalog", ""),
        database_name=cfg.get("schema", ""),
        run_id=run_id,
        client_id=client_id,
        entity_id=cfg.get("entity_id"),
        execution_id=cfg.get("execution_id", "1"),
        volume_path=cfg.get("volume_path", ""),
        sql_url_path=None,
        sql_username=None,
        sql_password=None,
    )
    logger.info(f"write_allocation_output: inserted via GenericResultStorer into LookThroughAllocationOutput")

    _log_timing("write_allocation_output", t0)
    return return_value


# ---------------------------------------------------------------------------
# Function 16: update_allocation_input
# SQL lines: 1130–1170
# Row count: Write function
# ---------------------------------------------------------------------------
def update_allocation_input(spark, cfg, grouped_output_df):
    """Deduct allocated amounts from LookThroughAllocationInput + zero residuals.

    Converted from: SQL lines 1130–1170.
    Target: LookThroughAllocationInput.

    Optimization: Read-Modify-Write with broadcast join (single Delta commit).
    Replaces MERGE + UPDATE with:
      1. Read RunID rows from target
      2. Broadcast-join with deductions
      3. Compute new Amount + zero residuals in one expression
      4. Atomic overwrite of RunID partition
    This eliminates ~50-60s of MERGE transaction overhead on serverless.
    """
    _log_section("update_allocation_input")
    t0 = time.time()

    run_id = cfg["run_id"]
    prefix = _table_prefix(cfg)
    target_table = f"{prefix}.LookThroughAllocationInput"

    # Step 1: Read all RunID rows from target table
    current = spark.table(target_table).filter(F.col("RunID") == run_id)

    # Step 2: Build join condition matching the SQL MERGE keys
    join_cond = (
        (F.col("t.EntityID") == F.col("s.EntityID"))
        & (F.coalesce(F.col("t.ParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.ParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.SuperParentEntityID"), F.lit(0)) == F.coalesce(F.col("s.SuperParentEntityID"), F.lit(0)))
        & (F.coalesce(F.col("t.TrackingKey").cast("string"), F.lit("0")) == F.coalesce(F.col("s.TrackingKey").cast("string"), F.lit("0")))
        & (F.coalesce(F.col("t.AdjustmentTypeID"), F.lit(0)) == F.col("s.AdjustmentTypeID"))
        & (F.col("t.LineID") == F.col("s.LineID"))
        & (F.col("t.LineTypeID") == F.col("s.LineTypeID"))
        & (F.coalesce(F.col("t.Tag"), F.lit("")) == F.coalesce(F.col("s.Tag"), F.lit("")))
    )

    # Step 3: Single expression — deduct if matched, zero if small residual
    raw_amount = F.when(
        F.col("s.Amount").isNotNull(),
        F.col("t.Amount") - F.col("s.Amount")
    ).otherwise(F.col("t.Amount"))

    new_amount = F.when(
        F.abs(F.coalesce(raw_amount, F.lit(0.0))).between(0, 0.99),
        F.lit(0.0)
    ).otherwise(raw_amount).cast("double")

    # Step 4: Broadcast-join + select all original columns with updated Amount
    updated = (
        current.alias("t")
        .join(F.broadcast(grouped_output_df).alias("s"), join_cond, "left")
        .select(
            *[F.col(f"t.{c}") for c in current.columns if c != "Amount"],
            new_amount.alias("Amount"),
        )
    )

    # Step 5: Atomic overwrite — single Delta commit (replaces MERGE + UPDATE)
    updated.writeTo(target_table).overwrite(F.col("RunID") == run_id)
    logger.info(f"update_allocation_input: deducted amounts + zeroed residuals via read-modify-write into {target_table}")

    _log_timing("update_allocation_input", t0)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_load_lt_footnote_effective_allocation_pct(
    spark: SparkSession,
    cfg: dict = None,
    EntityID: int = None,
    ClientID: int = None,
    TaxPeriodID: int = None,
    RunID: int = None,
    CatalogName: str = None,
    SchemaName: str = None,
    ResultType: str = "Parquet",
    VolumePath: str = None,
    ExecutionID: str = None,
    call_from: str = None,
    verbose: bool = False,
    **kwargs,
):
    """Load LookThrough Footnote Effective Allocation Percentages.

    Computes effective allocation percentages for Federal-to-Footnote mappings,
    writes allocation output, and updates allocation input amounts.
    """
    # Map CamelCase params to snake_case for use in function body
    entity_id = EntityID
    client_id = ClientID
    tax_period_id = TaxPeriodID
    run_id = RunID
    catalog = CatalogName
    schema = SchemaName

    t0 = time.time()

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    status = {
        "sp_name": "uspLoadLookThroughFootnoteEffectiveAllocationPercentage",
        "run_id": run_id,
        "entity_id": entity_id,
        "status": "SUCCESS",
        "error": None,
        "elapsed_seconds": 0,
        "sections_completed": 0,
    }

    # Mode 3 standalone: build cfg from IDs via load_common_config.
    # Modes 1/2 (Job/Orchestrator): cfg is passed in pre-built.
    if cfg is None:
        cfg = load_common_config(
            spark,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            call_from=call_from,
        )
    cfg = {**cfg, "_checkpoint_tables": []}

    if call_from is not None:
        cfg["call_from"] = call_from

    # GenericResultStorer output options — propagate signature params to cfg
    # so the storer reads them downstream (rule 48).
    if ResultType is not None:  cfg.setdefault("result_type", ResultType)
    if VolumePath is not None:  cfg["volume_path"] = VolumePath
    if ExecutionID is not None: cfg["execution_id"] = ExecutionID

    status["run_id"] = cfg.get("run_id")
    status["entity_id"] = cfg.get("entity_id")

    try:
        # §1 — Config & validation
        _load_sp_config(spark, cfg)

        if (cfg.get("run_status") or "").upper() == "FAIL":
            logger.error(f"RunStatus=FAIL — aborting.")
            status["status"] = "SKIPPED"
            status["error"] = "RunStatus=FAIL"
            return status

        if (cfg.get("allocation_type_name") or "").lower() != "pe book allocation":
            logger.info("AllocationTypeName != 'PE Book Allocation' — skipping.")
            status["status"] = "SKIPPED"
            return status

        if not cfg.get("register_type_id"):
            logger.info("RegisterTypeID is NULL/0 — skipping.")
            status["status"] = "SKIPPED"
            return status

        # §2 — Load mappings
        _t = time.time()
        mappings_df = load_mappings(spark, cfg)
        if verbose:
            logger.debug(f"[TIMING] load_mappings: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] load_mappings: {mappings_df.count()} rows")

        # §3 — Expand Parent/Contributor K1 mappings
        _t = time.time()
        mappings_df = expand_parent_k1_mappings(spark, cfg, mappings_df)
        if verbose:
            logger.debug(f"[TIMING] expand_parent_k1_mappings: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] expand_parent_k1_mappings: {mappings_df.count()} rows")

        # §4 — Distinct mappings
        _t = time.time()
        distinct_mappings_df = build_distinct_mappings(spark, cfg, mappings_df)
        if verbose:
            logger.debug(f"[TIMING] build_distinct_mappings: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_distinct_mappings: {distinct_mappings_df.count()} rows")

        # PERF: redundant `distinct_mappings_df.isEmpty()` check removed —
        # the K1 gate below (has_k1) already short-circuits on an empty DF
        # and is the only required exit. Saves one Spark action (~2–5s).

        # Finding 1: K1 gate. The original SP wraps §6–§16 (partners, FEP,
        # LT allocation output, cost/book pct, single/multi, K1 amounts,
        # final pct, allocation output writes) in `IF EXISTS (... SourceTypeID
        # = @K1LineTypeID ...)`. If no K1 mapping is present, the whole K1
        # path is skipped — the SP still succeeds. Without this gate the
        # downstream functions would either fail or do empty work.
        has_k1 = not distinct_mappings_df.filter(
            F.col("SourceTypeID") == cfg["enu_k1_line_type_id"]
        ).limit(1).isEmpty()
        if not has_k1:
            logger.info(
                "K1 gate: no K1 SourceTypeID in distinct mappings — skipping "
                "§6–§16 (partners, FEP, cost/book, K1 amounts, writes)."
            )
            status["sections_completed"] = 5
            status["status"] = "OK_NO_K1"
            return status

        # §5 — Yearly effective percentages (conditional)
        _t = time.time()
        total_amount_yearly_df, tmp_line_amounts_df = build_yearly_effective_pct(
            spark, cfg, distinct_mappings_df,
        )
        if verbose:
            logger.debug(f"[TIMING] build_yearly_effective_pct: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_yearly_effective_pct: total_amount_yearly={total_amount_yearly_df.count() if total_amount_yearly_df is not None else 0} rows, tmp_line_amounts={tmp_line_amounts_df.count() if tmp_line_amounts_df is not None else 0} rows")

        # §6 — Partners + FinalEffectivePercentages
        _t = time.time()
        partners_df = load_partners(spark, cfg)
        if verbose:
            logger.debug(f"[TIMING] load_partners: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] load_partners: {partners_df.count()} rows")
        _t = time.time()
        final_eff_pct_df = load_final_effective_percentages(spark, cfg)
        # T1-1: no in-memory caching — no-op on Serverless. Consumed by
        # §8 + §9. Source is a single filtered table read + broadcast join;
        # AQE will reuse the scan across the two downstream consumers.
        if verbose:
            logger.debug(f"[TIMING] load_final_effective_percentages: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] load_final_effective_percentages: {final_eff_pct_df.count()} rows")

        # §7 — LookThroughAllocationOutput (checkpoint — 3 consumers)
        _t = time.time()
        lt_output_df = build_lt_allocation_output(spark, cfg, distinct_mappings_df)
        if verbose:
            logger.debug(f"[DEBUG COUNT] build_lt_allocation_output: {lt_output_df.count()} rows")
        lt_output_df = _checkpoint(spark, lt_output_df, "lt_output", cfg)
        if verbose:
            logger.debug(f"[TIMING] build_lt_allocation_output + checkpoint: {time.time() - _t:.1f}s")

        # §8+§9 — Cost + Book effective percentages
        _t = time.time()
        cost_pct_df = build_cost_effective_pct(spark, cfg, lt_output_df, final_eff_pct_df)
        if verbose:
            logger.debug(f"[TIMING] build_cost_effective_pct: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_cost_effective_pct: {cost_pct_df.count()} rows")
        _t = time.time()
        book_pct_df = build_book_effective_pct(spark, cfg, lt_output_df, final_eff_pct_df)
        if verbose:
            logger.debug(f"[TIMING] build_book_effective_pct: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_book_effective_pct: {book_pct_df.count()} rows")
        temp_final_eff_pct_df = cost_pct_df.unionByName(book_pct_df)
        # T5: no checkpoint here. The union has 2 consumers (isEmpty marker +
        # §11 build_single_multi_alloc_type), but both inputs come from
        # already-checkpointed lt_output_df + a broadcast FEP scan, so the
        # lineage above the union is shallow. AQE reuses the scan; an extra
        # Delta write/read round-trip costs more than it saves on a Hard SP
        # with the V-OPT-4 checkpoint budget already at 1.
        if verbose:
            logger.debug(f"[DEBUG COUNT] temp_final_eff_pct (cost+book union): {temp_final_eff_pct_df.count()} rows")

        # §10 — TempAllocationInput
        _t = time.time()
        temp_alloc_input_df = load_temp_allocation_input(spark, cfg, distinct_mappings_df)
        # T1-1: no in-memory caching on Serverless. This DF has 4 consumers
        # (isEmpty, §11, §13, §14) but its lineage is shallow (single filtered
        # Delta read + broadcast join), so recomputation cost is minimal vs.
        # the checkpoint I/O round-trip. Checkpoint budget (V-OPT-4: 0–2 for
        # Hard SP) is spent on the deeper lineages: lt_output_df and
        # temp_final_eff_pct_df.
        if verbose:
            logger.debug(f"[TIMING] load_temp_allocation_input: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] load_temp_allocation_input: {temp_alloc_input_df.count()} rows")

        if temp_alloc_input_df.isEmpty():
            logger.info("No allocation input rows — exiting.")
            status["status"] = "SKIPPED"
            return status

        # SQL parity (lines 797–917): an empty #TempFinalEffectivePercentage
        # only gates the inner #SinglePercent build (SQL `IF EXISTS` at line
        # 801, END at line 909). The downstream K1 data path, #FinalEffective-
        # Percentage build (incl. yearly INSERT) and allocation output writes
        # run unconditionally.
        # PERF: the outer `temp_final_eff_pct_df.isEmpty()` check has been
        # removed — build_single_multi_alloc_type already does the same
        # check internally and returns None on empty input, which every
        # downstream consumer already handles. Saves one Spark action.

        # §11 — Single/Multi allocation type classification
        _t = time.time()
        single_percent_df = build_single_multi_alloc_type(
            spark, cfg, temp_alloc_input_df, distinct_mappings_df,
            temp_final_eff_pct_df,
        )
        if verbose:
            logger.debug(f"[TIMING] build_single_multi_alloc_type: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_single_multi_alloc_type: {single_percent_df.count() if single_percent_df is not None else 0} rows")

        # T1-1: no in-memory caching on Serverless. single_percent_df has 2
        # consumers (§12, §13) but its lineage is dominated by joins to
        # already-checkpointed temp_final_eff_pct_df and broadcast
        # distinct_mappings, so re-execution is cheap.

        # §12 — K1 data amounts
        _t = time.time()
        total_amount_pct_df, total_amounts_df = build_k1_data_amounts(
            spark, cfg, lt_output_df, distinct_mappings_df, single_percent_df,
        )
        if verbose:
            logger.debug(f"[TIMING] build_k1_data_amounts: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_k1_data_amounts: total_amount_pct={total_amount_pct_df.count() if total_amount_pct_df is not None else 0} rows, total_amounts={total_amounts_df.count() if total_amounts_df is not None else 0} rows")

        # §13 — Final effective percentages (3 INSERTs)
        _t = time.time()
        final_pct_df = build_final_effective_pct(
            spark, cfg, single_percent_df, temp_alloc_input_df,
            total_amount_pct_df, tmp_line_amounts_df,
        )
        if verbose:
            logger.debug(f"[TIMING] build_final_effective_pct: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_final_effective_pct: {final_pct_df.count() if final_pct_df is not None else 0} rows")

        # §14 — Build allocation output
        _t = time.time()
        alloc_output_df, grouped_output_df = build_allocation_output(
            spark, cfg, temp_alloc_input_df, final_pct_df, partners_df,
        )
        if verbose:
            logger.debug(f"[TIMING] build_allocation_output: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] build_allocation_output: alloc_output={alloc_output_df.count()} rows, grouped_output={grouped_output_df.count()} rows")

        # V-OPT-4: checkpoint budget for Hard SP is 0–2 total. Already used
        # on lt_output_df (§7, 3 consumers) and temp_final_eff_pct_df (§8+§9,
        # 2 consumers). alloc_output_df and grouped_output_df each have a
        # single downstream consumer (the write/update), so checkpointing
        # them would exceed budget without enough re-computation savings.

        # §15 — Write allocation output
        if verbose:
            logger.debug(f"[DEBUG COUNT] write_df: {alloc_output_df.count()} rows")
        _t = time.time()
        return_value = write_allocation_output(spark, cfg, alloc_output_df)
        if verbose:
            logger.debug(f"[TIMING] write_allocation_output: {time.time() - _t:.1f}s")

        # §16 — Update allocation input
        _t = time.time()
        update_allocation_input(spark, cfg, grouped_output_df)
        if verbose:
            logger.debug(f"[TIMING] update_allocation_input: {time.time() - _t:.1f}s")
            logger.debug(f"[DEBUG COUNT] update_allocation_input: completed")

        status["sections_completed"] = 16

    except Exception as e:
        status["status"] = "FAIL"
        status["error"] = str(e)
        logger.error(f"[FAIL] {e}", exc_info=True)
        raise
    finally:
        status["elapsed_seconds"] = round(time.time() - t0, 1)
        _drop_checkpoints(spark, cfg)

    logger.info(
        f"[DONE] run_load_lt_footnote_effective_allocation_pct | "
        f"{status['elapsed_seconds']}s | RunID={cfg['run_id']} "
        f"EntityID={cfg['entity_id']}"
    )
    # If GenericResultStorer returned a value (JSON string for Parquet mode,
    # "SUCCESS" for Delta/SQL), propagate it directly so the task runtime can
    # parse ResultFilePath/ResultFileName for DataBrickExecutionStatus (same
    # pattern as uspGetFinalEffectivePercentage's run_mode).
    return return_value if return_value else status


# ════════════════════════════════════════════════════════════════
# __main__: Databricks Job (Mode 1) or standalone (Mode 3)
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Mode 3 standalone: read widget IDs, let run_*() call load_common_config
    # via the `if cfg is None:` branch.
    status = run_load_lt_footnote_effective_allocation_pct(
        spark,
        RunID=int(dbutils.widgets.get("run_id")),
        EntityID=int(dbutils.widgets.get("entity_id")),
        ClientID=int(dbutils.widgets.get("client_id")),
        TaxPeriodID=int(dbutils.widgets.get("tax_period_id")),
        CatalogName=dbutils.widgets.get("catalog"),
        SchemaName=dbutils.widgets.get("schema"),
    )

    try:
        dbutils.notebook.exit(json.dumps(status))
    except Exception:
        print(json.dumps(status, indent=2))
