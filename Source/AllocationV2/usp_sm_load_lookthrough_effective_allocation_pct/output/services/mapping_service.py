"""Mapping setup for SM LookThrough Effective Allocation Percentage.

Functions:
    build_mapping_data       — SQL lines 503-735: Build all mapping DataFrames
    build_parent_k1_mappings — SQL lines 619-661 + inline udfGetParentToK1LineMappings
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0, table_prefix
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_if_empty

logger = logging.getLogger(__name__)

# ── column sets for the #Mapping temp table ──
_MAPPING_COLS = [
    "MapRegisterID", "EntityID", "StateID", "StateFieldID",
    "RegisterLineID", "FieldSourceID", "MapLineSubType",
    "OperationType", "SourceTypeID", "ContributionLineClassification",
]

_DISTINCT_MAP_COLS = [
    "StateID", "StateFieldID", "RegisterLineID", "FieldSourceID",
    "MapLineSubType", "OperationType", "SourceTypeID",
]


def build_mapping_data(spark: SparkSession, cfg: dict) -> dict:
    """Build all mapping DataFrames for state-to-federal line mapping.

    Converted from: SQL lines 503-735.
    Row count: ALWAYS-NON-EMPTY — mapping is required for SP to operate.

    Returns dict with keys:
        - distinct_mappings:      DataFrame (#DistinctMappings)
        - distinct_ubti_mappings: DataFrame (#DistinctUBTIMappings)
        - state_mapped_lines:     DataFrame (#StateMappedLines)
        - mapping:                DataFrame (#Mapping — full, all source types)
    """
    log_section("build_mapping_data")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    register_type_id = cfg["register_type_id"]
    map_entity_id = entity_id  # @MapEntityID = @LocalEntityID

    # ── S4-line 525: #MAPDataRegister ──
    # FROM MAPDataRegister WHERE RegisterTypeID=@RegisterTypeID
    #   AND (EntityID=@MapEntityID OR EntityID=-1) AND ClientID AND TaxPeriodID
    map_data_reg = (
        read_table(spark, "MAPDataRegister", cfg)
        .filter(
            (F.col("RegisterTypeID") == register_type_id)
            & ((F.col("EntityID") == map_entity_id) | (F.col("EntityID") == -1))
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(
            "MapRegisterID", "EntityID", "RegisterLineID", "FieldSourceID",
            "MapLineSubType", "OperationType", "SourceTypeID", "MapLineID",
            "CategoryID", "StateID", "ContributionLineClassification",
        )
    )

    sm_state_lines = read_table(spark, "SM_StateLines", cfg)

    # PERF: Use the original ENU_StateDataList table read (T1-6 / T1-5)
    # instead of spark.createDataFrame() which uses Python workers
    enu_sdl_df = (
        read_table(spark, "ENU_StateDataList", cfg)
        .filter(F.lower(F.col("Category")).isin("fieldsource", "statecategories"))
        .select("ID", "Category", "Value")
    )

    # StateCategories subset
    state_cat_values = ["income", "deduction", "n/a", "apportionment", "credit"]
    enu_state_cat = enu_sdl_df.filter(
        (F.lower(F.col("Category")) == "statecategories")
        & F.lower(F.col("Value")).isin(state_cat_values)
    )

    # ── S4-line 540: #EntityMapping ──
    # MAPDataRegister JOIN SM_StateLines ON MapLineID=StateFieldID
    # JOIN ENU_StateDataList ON CategoryID=ID AND Category='StateCategories'
    # WHERE EntityID=@MapEntityID AND Value IN (...)
    entity_mapping = (
        map_data_reg.alias("M")
        .join(
            sm_state_lines.alias("S"),
            F.col("M.MapLineID") == F.col("S.StateFieldID"),
        )
        .join(
            F.broadcast(enu_state_cat).alias("enu"),
            (F.col("M.CategoryID") == F.col("enu.ID")),
        )
        .filter(F.col("M.EntityID") == map_entity_id)
        .select(
            F.col("M.MapRegisterID"),
            F.col("M.EntityID"),
            F.col("S.StateID"),
            F.col("S.StateFieldID"),
            F.col("M.RegisterLineID"),
            F.col("M.FieldSourceID"),
            F.col("M.MapLineSubType"),
            F.col("M.OperationType"),
            F.col("M.SourceTypeID"),
            F.col("M.ContributionLineClassification"),
        )
    )

    # ── S4-line 555: #Mapping (default: EntityID=-1, left_anti join) ──
    default_mapping = (
        map_data_reg.alias("M2")
        .join(
            sm_state_lines.alias("S2"),
            F.col("M2.MapLineID") == F.col("S2.StateFieldID"),
        )
        .join(
            F.broadcast(enu_state_cat).alias("enu2"),
            (F.col("M2.CategoryID") == F.col("enu2.ID")),
        )
        .join(
            entity_mapping.select(
                F.col("StateID").alias("_EM_StateID"),
                F.col("StateFieldID").alias("_EM_StateFieldID"),
            ).distinct().alias("EM"),
            (F.col("M2.StateID") == F.col("EM._EM_StateID"))
            & (F.col("M2.MapLineID") == F.col("EM._EM_StateFieldID")),
            "left_anti",
        )
        .filter(
            F.col("M2.EntityID") == -1
        )
        .select(
            F.col("M2.MapRegisterID"),
            F.col("M2.EntityID"),
            F.col("S2.StateID"),
            F.col("S2.StateFieldID"),
            F.col("M2.RegisterLineID"),
            F.col("M2.FieldSourceID"),
            F.col("M2.MapLineSubType"),
            F.col("M2.OperationType"),
            F.col("M2.SourceTypeID"),
            F.col("M2.ContributionLineClassification"),
        )
    )

    # ── S4-line 570: Combine entity + default → #Mapping ──
    mapping_df = entity_mapping.unionByName(default_mapping)

    # ── S4-line 619: Parent K1 expansion (conditional) ──
    mapping_df = build_parent_k1_mappings(spark, cfg, mapping_df)

    # ── S4-line 662: #DistinctMappings ──
    federal_ids = [
        cfg["federal_amount_id"],
        cfg["federal_adj_id"],
        cfg["alloc_only_federal_amount_id"],
    ]
    federal_ids = [x for x in federal_ids if x is not None]

    distinct_mappings = (
        mapping_df
        .filter(F.col("FieldSourceID").isin(federal_ids))
        .select(*_DISTINCT_MAP_COLS)
        .distinct()
    )

    # ── S4-line 668: #DistinctUBTIMappings ──
    ubti_ids = [
        cfg["federal_ubti_id"],
        cfg["federal_ubti_adj_id"],
        cfg["alloc_only_federal_ubti_id"],
    ]
    ubti_ids = [x for x in ubti_ids if x is not None]

    distinct_ubti_mappings = (
        mapping_df
        .filter(F.col("FieldSourceID").isin(ubti_ids))
        .select(*_DISTINCT_MAP_COLS)
        .distinct()
    )

    # ── S4-line 674: PE Book Allocation offset lines ──
    alloc_type = (cfg.get("allocation_type_name") or "").strip()
    if alloc_type.lower() == "pe book allocation":
        k1_line_type = cfg["k1_line_type"]
        k1_line_item = read_table(spark, "K1LineItem", cfg)

        # Self-join K1LineItem for offset: K2.LineDescription = K1.LineDescription + ' - Offset'
        # AND K1.Box = K2.Box AND K1.LineNumber = K2.LineNumber
        tmp_mapping = (
            distinct_mappings.alias("EM")
            .join(
                sm_state_lines.alias("S3"),
                F.col("EM.StateFieldID") == F.col("S3.StateFieldID"),
            )
            .join(
                k1_line_item.alias("K1"),
                F.col("K1.LineID") == F.col("EM.RegisterLineID"),
            )
            .join(
                k1_line_item.alias("K2"),
                (F.col("K2.LineDescription") == F.concat(F.col("K1.LineDescription"), F.lit(" - Offset")))
                & (F.col("K1.Box") == F.col("K2.Box"))
                & (F.col("K1.LineNumber") == F.col("K2.LineNumber")),
            )
            .filter(F.col("EM.SourceTypeID") == k1_line_type)
            .select(
                F.col("EM.StateID"),
                F.col("S3.StateFieldID"),
                F.col("EM.RegisterLineID"),
                F.col("EM.FieldSourceID"),
                F.col("EM.MapLineSubType"),
                F.col("EM.OperationType"),
                F.col("EM.SourceTypeID"),
            )
        )
        distinct_mappings = distinct_mappings.unionByName(tmp_mapping)

    # Tagged-union checkpoint: materialize both DFs in 1 Spark job.
    # These are broadcast-joined 8+ times downstream across flow-up, amount,
    # and state-mapping sections. Without checkpoint, each broadcast re-evaluates
    # the full mapping pipeline (MapDataRegister → entity/default merge →
    # parent K1 expansion → filter → distinct).
    _combined_chk = (
        distinct_mappings.withColumn("_tag", F.lit("K1"))
        .unionByName(
            distinct_ubti_mappings.withColumn("_tag", F.lit("UBTI")),
            allowMissingColumns=True,
        )
        .localCheckpoint(eager=True)
    )
    distinct_mappings = _combined_chk.filter(F.col("_tag") == "K1").drop("_tag")
    distinct_ubti_mappings = _combined_chk.filter(F.col("_tag") == "UBTI").drop("_tag")

    # ── S4-line 717: #StateMappedLines = UNION of distinct StateId, StateFieldId ──
    # PERF: Computed AFTER checkpoint so it reads from materialized data rather
    # than re-evaluating the full mapping pipeline (MAPDataRegister joins,
    # parent K1 expansion, etc.). Saves ~3-5s of redundant re-computation.
    state_mapped_lines = (
        distinct_mappings.select("StateID", "StateFieldID").distinct()
        .unionByName(
            distinct_ubti_mappings.select("StateID", "StateFieldID").distinct()
        )
        .distinct()
    )

    # No assert_non_empty — avoids 1 unnecessary Spark action.

    result = {
        "distinct_mappings": distinct_mappings,
        "distinct_ubti_mappings": distinct_ubti_mappings,
        "state_mapped_lines": state_mapped_lines,
        "mapping": mapping_df,
    }

    log_timing("build_mapping_data", t0)
    return result


def build_parent_k1_mappings(
    spark: SparkSession, cfg: dict, mapping_df: DataFrame,
) -> DataFrame:
    """Resolve parent K1 line mappings by inlining udfGetParentToK1LineMappings.

    Converted from: SQL lines 619-661 (calling code) +
                    dbo.udfGetParentToK1LineMappings (UDF, 483 lines).
    Only the 'Allocations' branch of the UDF is inlined.

    Row count: POSSIBLY-EMPTY — only runs when IsConfigK1Checked and
               ParentK1/Contributor source types exist in mapping.

    Args:
        mapping_df: The #Mapping DataFrame before parent K1 expansion.

    Returns:
        Updated mapping_df with parent K1 lines expanded and original
        parent/contributor source type rows removed.
    """
    log_section("build_parent_k1_mappings")
    t0 = time.time()

    parent_src_id = cfg.get("parent_k1_line_source_id")
    contrib_src_id = cfg.get("contributor_k1_line_source_id")
    is_k1_checked = cfg.get("is_config_k1_checked", False)

    # S4-line 619: IF EXISTS(... SourceTypeID IN (...) AND @IsConfigK1Checked=1)
    if not is_k1_checked or parent_src_id is None:
        logger.info("[SKIP] build_parent_k1_mappings: IsConfigK1Checked=False or no ParentK1SourceID")
        log_timing("build_parent_k1_mappings", t0)
        return mapping_df

    source_ids = [parent_src_id]
    if contrib_src_id is not None:
        source_ids.append(contrib_src_id)

    # PERF: Removed head(1) existence check — it triggers a full evaluation of
    # the lazy mapping pipeline (~2-3s). Instead, proceed unconditionally:
    # if no parent rows exist, expanded_k1 is empty and the union is a no-op.
    # The checkpoint at the end evaluates everything in one pass.

    # ── UDF inlined: 'Allocations' path, @ApplicableToState=1 ──
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    register_type_id = cfg["register_type_id"]
    k1_line_type = cfg["k1_line_type"]

    # UDF-line 145: @IsFedToFootNote — pre-resolved in load_sp_config
    _ftf_id = cfg.get("_fed_to_footnote_menu_id")
    is_fed_to_footnote = (_ftf_id is not None and _ftf_id == register_type_id)

    # UDF-line 150: @K1inputsourceTypeID — pre-resolved in load_sp_config
    k1_input_source_type_id = cfg.get("k1_input_source_type_id")

    # UDF-line 167: @RegisterMappings from MAPDataRegister JOIN parent_mappings
    # parent_mappings = @K1MappedData(MapRegisterID, EntityID, MapLineID, RegisterLineID)
    parent_mappings = mapping_df.select(
        F.col("MapRegisterID"),
        F.col("EntityID").alias("PM_EntityID"),
        F.col("StateFieldID").alias("MapLineID_PM"),
        F.col("RegisterLineID").alias("RegisterLineID_PM"),
    )

    mdr = read_table(spark, "MAPDataRegister", cfg).alias("MDR")

    register_mappings = (
        mdr
        .join(
            parent_mappings.alias("PM"),
            (F.col("MDR.MapRegisterID") == F.col("PM.MapRegisterID"))
            & (F.col("MDR.MapLineID") == F.col("PM.MapLineID_PM"))
            & (F.col("MDR.RegisterLineID") == F.col("PM.RegisterLineID_PM")),
        )
        .filter(
            (F.col("MDR.RegisterTypeID") == register_type_id)
            & (F.col("MDR.ClientID") == client_id)
            & (F.col("MDR.TaxPeriodID") == tax_period_id)
            & F.col("MDR.SourceTypeID").isin(source_ids)
        )
        .select(
            F.col("MDR.MapRegisterID").alias("RM_MapRegisterID"),
            F.col("PM.PM_EntityID").alias("RM_EntityID"),
            F.col("MDR.RegisterTypeID").alias("RM_RegisterTypeID"),
            F.col("MDR.SourceTypeID").alias("RM_SourceTypeID"),
            F.col("MDR.MapLineID").alias("RM_MapLineID"),
            F.col("MDR.RegisterLineID").alias("RM_RegisterLineID"),
            F.col("MDR.OperationType").alias("RM_OperationType"),
            F.col("MDR.CategoryID").alias("RM_CategoryID"),
            F.col("MDR.MapLineSubType").alias("RM_MapLineSubType"),
            F.col("MDR.StateID").alias("RM_StateID"),
            F.col("MDR.AdjustmentTypeID").alias("RM_AdjustmentTypeID"),
            F.col("MDR.FieldSourceID").alias("RM_FieldSourceID"),
            F.col("MDR.LineDescription").alias("RM_LineDescription"),
            F.col("MDR.PeriodID").alias("RM_PeriodID"),
            F.col("MDR.LevelTypeID").alias("RM_LevelTypeID"),
            F.col("MDR.IsEntityLevel").alias("RM_IsEntityLevel"),
            F.col("MDR.CurrencyCode").alias("RM_CurrencyCode"),
            F.col("MDR.AdjustmentSourceID").alias("RM_AdjustmentSourceID"),
            F.col("MDR.Quarter").alias("RM_Quarter"),
            F.col("MDR.TaxTreatment").alias("RM_TaxTreatment"),
            F.col("MDR.Waterfall").alias("RM_Waterfall"),
            F.col("MDR.OffsetType").alias("RM_OffsetType"),
            F.col("MDR.TransactionDate").alias("RM_TransactionDate"),
            F.col("MDR.LineType").alias("RM_LineType"),
        )
        .distinct()
    )

    # UDF-line 185: @ParentLineMappings
    parent_line_mappings = (
        register_mappings
        .filter(F.col("RM_SourceTypeID").isin(source_ids))
        .select(
            F.col("RM_MapRegisterID").alias("PL_MapRegisterID"),
            F.col("RM_MapLineID").alias("PL_MapLineID"),
            F.col("RM_RegisterLineID").alias("PL_RegisterLineID"),
            F.col("RM_TaxTreatment").alias("PL_TaxTreatment"),
            F.col("RM_Waterfall").alias("PL_Waterfall"),
            F.col("RM_OffsetType").alias("PL_OffsetType"),
            F.col("RM_TransactionDate").alias("PL_QTD"),
            F.col("RM_LineType").alias("PL_LineType"),
            F.col("RM_OperationType").alias("PL_OperationType"),
        )
        .distinct()
    )

    # UDF-line 253: @Parents
    pk1g = read_table(spark, "ParentK1GLineItem", cfg).alias("PG")

    # Join key depends on @IsFedToFootNote
    if is_fed_to_footnote:
        join_cond = F.col("PL.PL_MapLineID") == F.col("PG.ParentK1GLineID")
    else:
        join_cond = F.col("PL.PL_RegisterLineID") == F.col("PG.ParentK1GLineID")

    parents = (
        parent_line_mappings.alias("PL")
        .join(pk1g, join_cond)
        .select(
            F.col("PL.PL_MapRegisterID").alias("P_MapRegisterID"),
            F.col("PG.ParentK1GLineID").alias("P_MappedParentLineID"),
            F.col("PL.PL_TaxTreatment").alias("P_TaxTreatment"),
            F.col("PL.PL_Waterfall").alias("P_Waterfall"),
            F.col("PL.PL_OffsetType").alias("P_OffsetType"),
            F.col("PL.PL_QTD").alias("P_QTD"),
            F.col("PL.PL_LineType").alias("P_LineType"),
            F.col("PG.IsAllocable").alias("P_IsAllocable"),
            F.col("PL.PL_OperationType").alias("P_Formula"),
            (
                F.when(F.lit(is_fed_to_footnote), F.col("PL.PL_RegisterLineID"))
                .otherwise(F.col("PL.PL_MapLineID"))
            ).alias("P_MapLineID"),
        )
        .distinct()
    )

    # UDF-line 269 (Allocations path): Distinct LineIDs from LookThroughAllocationInput
    lt_alloc_input = read_table(spark, "LookThroughAllocationInput", cfg)
    distinct_line_ids = (
        lt_alloc_input
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineTypeID") == k1_line_type)
        )
        .select("LineID")
        .distinct()
    )

    # UDF-line 275: ContributionLinesWithAttributes
    # JOIN DistinctLineIDs ON K1LineID=LineID JOIN K1LineItem ON LineID
    # WHERE ApplicableToStates IN ('S','SF') (since @ApplicableToState=1)
    cla = read_table(spark, "ContributionLineWithAttributes", cfg).alias("C")
    k1li = read_table(spark, "K1LineItem", cfg).alias("K")

    contrib_lines = (
        cla
        .join(distinct_line_ids.alias("L"),
              F.col("C.K1LineID") == F.col("L.LineID"))
        .join(k1li,
              F.col("L.LineID") == F.col("K.LineID"))
        .filter(
            F.upper(F.coalesce(F.col("K.ApplicableToStates"), F.lit(""))).isin("S", "SF")
        )
        .select(
            F.col("C.ContributionLineID"),
            F.col("C.K1LineID"),
            F.col("C.Source"),
            F.col("C.Waterfall").alias("C_Waterfall"),
            F.col("C.TransactionDate").alias("C_TransactionDate"),
            F.col("C.FN"),
            F.col("C.Offset").alias("C_Offset"),
            F.col("C.ParentLineID"),
            F.col("C.ContributionLineClassification").alias("C_CLC"),
        )
        .distinct()
    )

    # UDF-line 384: @ParentChildK1Lines - complex join Parents × ContribLines
    fn_foreign_len = len("Foreign")  # 7

    # CASE WHEN P.IsAllocable = 1 THEN C.ContributionLineID ELSE C.ParentLineID END = P.MappedParentLineID
    parent_join_cond = (
        F.when(F.col("PA.P_IsAllocable") == True, F.col("CL.ContributionLineID"))
        .otherwise(F.col("CL.ParentLineID"))
        == F.col("PA.P_MappedParentLineID")
    )

    # Source matching (TaxTreatment)
    source_cond = (
        F.when(
            F.upper(F.col("PA.P_TaxTreatment")) == "ALL", F.lit(True)
        ).when(
            (F.coalesce(F.col("CL.Source"), F.lit("")) == "Foreign")
            & (F.substring(F.coalesce(F.col("PA.P_TaxTreatment"), F.lit("")), 1, fn_foreign_len) == "Foreign"),
            F.lit(True),
        ).otherwise(
            F.col("CL.Source") == F.col("PA.P_TaxTreatment")
        )
    )

    # Waterfall matching
    waterfall_cond = (
        F.when(
            F.upper(F.coalesce(F.col("PA.P_Waterfall"), F.lit(""))).isin("ALL", ""),
            F.lit(True),
        ).otherwise(
            F.coalesce(F.col("CL.C_Waterfall"), F.lit(""))
            == F.coalesce(F.col("PA.P_Waterfall"), F.lit(""))
        )
    )

    # TransactionDate matching
    txn_date_cond = (
        F.when(
            F.coalesce(F.col("PA.P_QTD"), F.lit("Q0")) == "ALL",
            F.lit(True),
        ).otherwise(
            F.coalesce(F.col("CL.C_TransactionDate"), F.lit("Q0"))
            == F.coalesce(F.col("PA.P_QTD"), F.lit("Q0"))
        )
    )

    # FN / LineType matching
    fn_cond = F.coalesce(F.col("CL.FN"), F.lit("K-1")) == F.col("PA.P_LineType")

    # Offset matching
    offset_cond = (
        F.when(
            F.upper(F.coalesce(F.col("PA.P_OffsetType"), F.lit(""))).isin("ALL", ""),
            F.lit(True),
        ).otherwise(
            F.coalesce(F.col("CL.C_Offset"), F.lit(""))
            == F.col("PA.P_OffsetType")
        )
    )

    parent_child_k1 = (
        parents.alias("PA")
        .join(contrib_lines.alias("CL"), parent_join_cond)
        .filter(source_cond & waterfall_cond & txn_date_cond & fn_cond & offset_cond)
        .select(
            F.col("PA.P_MapRegisterID").alias("PCK_MapRegisterID"),
            F.col("PA.P_MappedParentLineID").alias("PCK_MappedParentLineID"),
            F.col("CL.ContributionLineID").alias("PCK_ContributionLineID"),
            F.col("CL.K1LineID").alias("PCK_K1LineID"),
            F.col("PA.P_Formula").alias("PCK_Formula"),
            F.col("PA.P_MapLineID").alias("PCK_MapLineID"),
            F.col("CL.C_CLC").alias("PCK_CLC"),
        )
    )

    # UDF-line 409: Return @ParentK1MappedData
    if is_fed_to_footnote:
        rm_join_key = F.col("RM2.RM_MapLineID")
    else:
        rm_join_key = F.col("RM2.RM_RegisterLineID")

    parent_k1_mapped = (
        register_mappings.alias("RM2")
        .join(
            parent_child_k1.alias("PC"),
            (rm_join_key == F.col("PC.PCK_MappedParentLineID"))
            & (F.col("RM2.RM_MapRegisterID") == F.col("PC.PCK_MapRegisterID")),
        )
        .select(
            F.col("PC.PCK_MapRegisterID").alias("UDF_MapRegisterID"),
            F.col("RM2.RM_EntityID").alias("UDF_EntityID"),
            F.col("RM2.RM_SourceTypeID").alias("UDF_SourceTypeID"),
            (
                F.when(F.lit(is_fed_to_footnote), F.col("PC.PCK_K1LineID"))
                .otherwise(F.col("RM2.RM_MapLineID"))
            ).alias("UDF_MapLineID"),
            (
                F.when(F.lit(is_fed_to_footnote), F.col("RM2.RM_RegisterLineID"))
                .otherwise(F.col("PC.PCK_K1LineID"))
            ).alias("UDF_RegisterLineID"),
            F.col("RM2.RM_FieldSourceID").alias("UDF_FieldSourceID"),
            F.col("RM2.RM_StateID").alias("UDF_StateID"),
            F.col("PC.PCK_CLC").alias("UDF_CLC"),
        )
        .distinct()
    )

    # ── Back in calling SP: SQL lines 635-660 ──
    # INSERT INTO #Mapping ... FROM #Mapping M JOIN #ParentChildMappedData PCL
    # ON M.StateFieldId=PCL.MapLineID AND M.EntityID=PCL.EntityID
    # AND M.MapRegisterID=PCL.MapRegisterID
    # AND ISNULL(M.FieldSourceID,0)=ISNULL(PCL.FieldSourceID,0)
    # AND M.StateID=PCL.StateID
    # AND CASE WHEN ISNULL(M.ContributionLineClassification,'ALL')='ALL'
    #       THEN PCL.ContributionLineClassification
    #       ELSE M.ContributionLineClassification END = PCL.ContributionLineClassification
    # WHERE M.SourceTypeID IN (@ParentK1LineSourceID, @ContributorK1LineSourceID)

    parent_rows_in_mapping = mapping_df.filter(
        F.col("SourceTypeID").isin(source_ids)
    )

    clc_cond = (
        F.when(
            F.coalesce(F.col("MP.ContributionLineClassification"), F.lit("ALL")) == "ALL",
            F.col("PCL.UDF_CLC"),
        ).otherwise(F.col("MP.ContributionLineClassification"))
        == F.col("PCL.UDF_CLC")
    )

    expanded_k1 = (
        parent_rows_in_mapping.alias("MP")
        .join(
            parent_k1_mapped.alias("PCL"),
            (F.col("MP.StateFieldID") == F.col("PCL.UDF_MapLineID"))
            & (F.col("MP.EntityID") == F.col("PCL.UDF_EntityID"))
            & (F.col("MP.MapRegisterID") == F.col("PCL.UDF_MapRegisterID"))
            & (
                F.coalesce(F.col("MP.FieldSourceID"), F.lit(0))
                == F.coalesce(F.col("PCL.UDF_FieldSourceID"), F.lit(0))
            )
            & (F.col("MP.StateID") == F.col("PCL.UDF_StateID")),
        )
        .filter(clc_cond)
        .select(
            F.col("MP.MapRegisterID"),
            F.col("MP.EntityID"),
            F.col("MP.StateID"),
            F.col("MP.StateFieldID"),
            F.col("PCL.UDF_RegisterLineID").alias("RegisterLineID"),
            F.col("MP.FieldSourceID"),
            F.col("MP.MapLineSubType"),
            F.col("MP.OperationType"),
            F.lit(k1_line_type).alias("SourceTypeID"),
            F.col("MP.ContributionLineClassification"),
        )
        .distinct()
    )

    # DELETE parent/contributor rows + add expanded K1 rows
    non_parent_mapping = mapping_df.filter(
        ~F.col("SourceTypeID").isin(source_ids)
    )
    mapping_df = non_parent_mapping.unionByName(expanded_k1)

    # No warn_if_empty — avoids 1 unnecessary Spark action.

    log_timing("build_parent_k1_mappings", t0)
    return mapping_df
