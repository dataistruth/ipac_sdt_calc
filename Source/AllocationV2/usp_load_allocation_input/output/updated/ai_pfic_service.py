"""
ai_pfic_service.py

PFIC (Passive Foreign Investment Company) snapshot building, election processing,
and allocation input construction for uspLoadAllocationInput.

SQL lines: 2500-3400
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import time

from Common_V2.core.helpers import table_prefix, read_table, log_section, log_timing
from . import checkpoint as _ckpt


def current_run_scoped(df, cfg):
    fn = getattr(_ckpt, "current_run_scoped", None)
    if fn:
        return fn(df, cfg)
    if "ClientID" in df.columns:
        df = df.filter(F.col("ClientID") == cfg["client_id"])
    if "TaxPeriodID" in df.columns:
        df = df.filter(F.col("TaxPeriodID") == cfg["tax_period_id"])
    if "RunID" in df.columns:
        df = df.filter(F.col("RunID") == cfg["run_id"])
    return df


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


def build_pfic_snapshot(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build PFIC snapshot DataFrame from PFICFootnoteInput_Snapshot.

    SQL lines: 2500-2600. Reads approved PFIC data for the entity's workflows.
    Applies blocked/unblocked filter chain: excludes PFICs that are classified
    as 'Blocked' in PficForeignCorpClassificationInput.
    """
    log_section("build_pfic_snapshot")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    pfic_snapshot = (
        read_table(spark, "PFICFootnoteInput_Snapshot", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )
    )

    k1_wf_df = spark.table(f"_k1_workflow_{run_id}")
    result = (
        pfic_snapshot.alias("P")
        .join(k1_wf_df.alias("KW"), F.col("P.WorkflowID") == F.col("KW.WorkflowID"), "inner")
        .select(
            F.col("P.WorkflowID"), F.col("P.PFICFootnoteID"), F.col("P.LineID"),
            F.col("P.Amount"), F.col("P.TextValue"),
            F.col("P.ClientID"), F.col("P.TaxPeriodID"),
            F.col("KW.EntityID").alias("SourceEntityID"),
        )
        .distinct()
    )

    if pfic_investment_line_id:
        pfic_class_df = current_run_scoped(
            read_table(spark, "PficForeignCorpClassificationInput", cfg), cfg
        )
        blocked_set = (
            pfic_class_df
            .filter(
                (F.lower(F.col("FootnoteClassification")) == "blocked") &
                (F.col("SourceEntityID") == entity_id)
            )
            .select(
                F.col("EntityID").alias("BlockedEntityID"),
                F.coalesce(F.col("PFICSourceEntityID"), F.lit(0)).alias("BlockedPFICSourceEntityID"),
                F.col("PficFootnoteID").alias("BlockedPficFootnoteID"),
                F.col("TrackingKey").alias("BlockedTrackingKey"),
            )
            .distinct()
        )

        # Anti-join: exclude rows where snapshot investment line matches a blocked entry
        # Match: TextValue == Blocked.EntityID AND SourceEntityID == BlockedPFICSourceEntityID
        #         AND LineID == @PFICInvestmentLineID
        investment_rows = result.filter(F.col("LineID") == pfic_investment_line_id)

        unblocked_keys = (
            investment_rows.alias("INV")
            .join(
                blocked_set.alias("B"),
                (F.col("INV.TextValue") == F.col("B.BlockedEntityID").cast("string")) &
                (F.coalesce(F.col("INV.SourceEntityID"), F.lit(0)) ==
                 F.coalesce(F.col("B.BlockedPFICSourceEntityID"), F.lit(0))),
                "left_anti"
            )
            .select(
                F.col("INV.PFICFootnoteID").alias("_ub_footnote"),
                F.coalesce(F.col("INV.SourceEntityID"), F.lit(0)).alias("_ub_src"),
            )
            .distinct()
        )
        snap_cols = result.columns
        result = (
            result.alias("R")
            .join(
                unblocked_keys.alias("U"),
                (F.col("R.PFICFootnoteID") == F.col("U._ub_footnote")) &
                (F.coalesce(F.col("R.SourceEntityID"), F.lit(0)) == F.col("U._ub_src")),
                "inner"
            )
            .select(*[F.col(f"R.{c}") for c in snap_cols])
        )

    log_timing("build_pfic_snapshot", t0)
    return result


def build_pfic_elections(spark: SparkSession, cfg: dict, pfic_snapshot_df: DataFrame) -> dict:
    """Build PFIC election lookup from snapshot data.

    SQL lines: 2600-2800. Determines election types (1291/1293/1296) per entity.
    Also builds #NonQEFElectedPFIC, #SubpartFElectedPFIC, and #QFCandCFCPFICs.
    Returns dict with election DataFrames.
    """
    log_section("build_pfic_elections")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]
    is_foreign_entity = cfg.get("is_foreign_entity", False)

    type_of_pfic_line_id = cfg.get("type_of_pfic_line_id")
    qef_election_line_id = cfg.get("qef_election_line_id")
    election_d_line_id = cfg.get("election_d_line_id")
    type_of_foreign_corp_line_id = cfg.get("type_of_foreign_corp_line_id")
    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    elections = {
        "type_of_pfic": None,
        "qef_elections": None,
        "election_d": None,
    }

    if type_of_pfic_line_id:
        elections["type_of_pfic"] = (
            pfic_snapshot_df
            .filter(F.col("LineID") == type_of_pfic_line_id)
            .select("PFICFootnoteID", "SourceEntityID", "TextValue")
        )


    if qef_election_line_id:
        qef_from_line = (
            pfic_snapshot_df
            .filter(
                (F.col("LineID") == qef_election_line_id) &
                (F.lower(F.col("TextValue")) == "true")
            )
            .select("PFICFootnoteID", "SourceEntityID")
        )
        # Domestic-only branch: is1293EligibleNoDeemed on type_of_pfic_line_id when entity is domestic
        if type_of_pfic_line_id and not is_foreign_entity:
            qef_from_1293 = (
                pfic_snapshot_df
                .filter(
                    (F.col("LineID") == type_of_pfic_line_id) &
                    (F.lower(F.col("TextValue")) == "is1293eligiblenodeemed")
                )
                .select("PFICFootnoteID", "SourceEntityID")
            )
            elections["qef_elections"] = qef_from_line.unionByName(qef_from_1293).distinct()
        else:
            elections["qef_elections"] = qef_from_line

    if election_d_line_id:
        elections["election_d"] = (
            pfic_snapshot_df
            .filter(
                (F.col("LineID") == election_d_line_id) &
                (F.lower(F.col("TextValue")) == "yes")
            )
            .select("PFICFootnoteID", "SourceEntityID")
        )

    log_timing("build_pfic_elections", t0)
    return elections


def build_pfic_allocation_input(
    spark: SparkSession, cfg: dict,
    pfic_snapshot_df: DataFrame, pfic_elections: dict,
) -> DataFrame:
    """Build PFIC allocation input rows from snapshot.

    SQL lines: 2800-3100. Converts PFIC amounts into AllocationInput rows
    with proper LineTypeID.
    """
    log_section("build_pfic_allocation_input")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    entity_id = cfg["entity_id"]
    tax_period_id = cfg["tax_period_id"]
    pfic_lt = cfg.get("pfic_footnote_line_type_id")
    is_tracking = cfg.get("is_tracking_key", "C") == "C"

    if not pfic_lt:
        from pyspark.sql.types import StructType
        log_timing("build_pfic_allocation_input", t0)
        return spark.createDataFrame([], StructType())

    # Get PFIC line items that are numeric and allocated.
    pfic_line_item_df = spark.table("_pfic_line_item")
    numeric_lines = (
        pfic_line_item_df
        .filter(
            # SQL `LineDataType = 'NUMBER'` is case-insensitive; pficfootnotelineitem
            # stores 'NUMBER' (upper). Spark `==` is case-sensitive, so "Number"
            # matched 0 rows and zeroed out all PFIC (LineType 8) AllocationInput.
            (F.upper(F.col("LineDataType")) == "NUMBER") &
            (F.col("IsAllocated") == True) &
            (F.col("IsActive") == True)
        )
        .select("LineID", "ShortName")
    )

    pct_share_names = ['OwnershipPercentage', 'NumberofSharesBeginningofYear',
                       'NumberofSharesEndofYear', 'Part_5_G', 'CFCPartnershipownership']

    # Join snapshot with numeric allocated lines
    entity_df = spark.table("_entity")
    fx_avg_rate_df = spark.table("_fx_avg_rate")

    result = (
        pfic_snapshot_df.alias("P")
        .join(numeric_lines.alias("NL"), F.col("P.LineID") == F.col("NL.LineID"), "inner")
        .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("P.SourceEntityID"), "inner")
        .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
        .select(
            F.col("P.SourceEntityID").alias("EntityID"),
            F.lit(pfic_lt).cast("int").alias("LineTypeID"),
            F.col("P.LineID"),
            F.when(F.lower(F.col("NL.ShortName")).isin([s.lower() for s in pct_share_names]), F.col("P.Amount"))
             .otherwise(F.round(F.col("P.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)).alias("Amount"),
            F.col("P.PFICFootnoteID").alias("QuicklinkID"),
            F.lit(0).cast("int").alias("CategoryID"),
            F.lit(0).cast("int").alias("ParentEntityID"),
            F.lit(0).cast("int").alias("SuperParentEntityID"),
            F.when(F.lit(is_tracking), F.col("P.SourceEntityID").cast("string"))
             .otherwise(F.lit(None).cast("string")).alias("TrackingKey"),
            F.lit(None).cast("string").alias("Tag"),
            F.lit(None).cast("int").alias("SchID"),
            F.lit(None).cast("int").alias("OriginalParentEntityID"),
        )
    )

    store_qef = cfg.get("store_qef_for_allocation")
    qef_election_line_id = cfg.get("qef_election_line_id")
    type_of_pfic_line_id = cfg.get("type_of_pfic_line_id")
    is_foreign = cfg.get("is_foreign_entity", False)
    if store_qef == "U":
        qef_parts = []
        if qef_election_line_id:
            qef_parts.append(
                pfic_snapshot_df
                .filter(
                    (F.col("LineID") == qef_election_line_id) &
                    (F.lower(F.col("TextValue")) == "true")
                )
                .select(F.col("PFICFootnoteID").alias("qef_id"))
            )
        # Source #1 (domestic only): TypeofPFIC line, TextValue='is1293EligibleNoDeemed'.
        if not is_foreign and type_of_pfic_line_id:
            qef_parts.append(
                pfic_snapshot_df
                .filter(
                    (F.col("LineID") == type_of_pfic_line_id) &
                    (F.lower(F.col("TextValue")) == "is1293eligiblenodeemed")
                )
                .select(F.col("PFICFootnoteID").alias("qef_id"))
            )
        if qef_parts:
            qef_elected_snapshot = qef_parts[0]
            for part in qef_parts[1:]:
                qef_elected_snapshot = qef_elected_snapshot.unionByName(part)
            qef_elected_snapshot = qef_elected_snapshot.distinct()
            result = (
                result.alias("R")
                .join(
                    qef_elected_snapshot.alias("QEF"),
                    F.col("R.QuicklinkID") == F.col("QEF.qef_id"),
                    "left_anti"
                )
            )

    pfic_classification = cfg.get("pfic_classification", "")
    is_foreign = cfg.get("is_foreign_entity", False)
    k1_line_type = cfg.get("k1_line_type_id")

    if k1_line_type:
        try:
            reclass_data = spark.table("_reclass_data")
        except Exception:
            reclass_data = None

        if reclass_data is not None:
            pfic_pkg_df = read_table(spark, "PFICFootnotePackage", cfg)
            k1_pkg_df = spark.table("_k1_package")
            pfic_line_item_df = spark.table("_pfic_line_item")

            from .ai_pfic_flowup_service import register_reclass_unblocked, _filter_reclass_to_unblocked
            register_reclass_unblocked(spark, cfg)
            reclass_data = _filter_reclass_to_unblocked(spark, cfg, reclass_data)

            # Base reclass PFIC data: unblocked footnotes with allocated PFIC lines
            reclass_pfic_base = (
                reclass_data.alias("RFA")
                .filter(F.col("RFA.LineTypeID") == pfic_lt)
                .join(
                    pfic_line_item_df.filter(
                        (F.col("IsAllocated") == True)
                        & (F.upper(F.col("LineDataType")) == "NUMBER")
                        & (F.col("IsActive") == True)
                    ).alias("PL"),
                    F.col("RFA.LineID") == F.col("PL.LineID"),
                    "inner"
                )
                .join(
                    pfic_pkg_df.alias("PP"),
                    F.col("PP.PFICFootnoteID") == F.col("RFA.FootnoteID"),
                    "inner"
                )
                .join(
                    k1_pkg_df.alias("K1P"),
                    F.col("K1P.K1PackageID") == F.col("PP.K1PackageID"),
                    "inner"
                )
            )

            if pfic_classification == "C" and not is_foreign:
                footnote_elected = _build_footnote_elected(
                    spark,
                    cfg,
                    prune_to_lower_tier_runs(
                        read_table(
                            spark, "PFICFootnoteFlowupWithTrackingKey", cfg
                        ),
                        spark,
                        cfg,
                    ),
                )
                footnote_pfic_insert = (
                    reclass_pfic_base
                    .join(
                        footnote_elected.alias("FP"),
                        (F.col("RFA.FootnoteID") == F.col("FP.PFICFootnoteID")) &
                        (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) ==
                         F.coalesce(F.col("FP.TrackingKey"), F.lit(""))) &
                        (F.col("RFA.LTEntityID") == F.col("FP.FlowupEntityID")),
                        "inner"
                    )
                    .groupBy(
                        F.col("K1P.LowerTierEntityID"),
                        F.col("RFA.LineID"),
                        F.col("RFA.FootnoteID"),
                        F.coalesce(F.col("RFA.ParentEntityID"), F.lit(0)).alias("_parent"),
                        F.col("RFA.LTEntityID"),
                        F.col("RFA.TrackingKey").alias("_tk"),
                        F.coalesce(F.col("RFA.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
                    )
                    .agg(F.sum("RFA.FlowupAmount").alias("Amount"))
                    .select(
                        F.col("K1P.LowerTierEntityID").alias("EntityID"),
                        F.lit(pfic_lt).cast("int").alias("LineTypeID"),
                        F.col("RFA.LineID"),
                        F.col("Amount"),
                        F.col("RFA.FootnoteID").alias("QuicklinkID"),
                        F.lit(0).cast("int").alias("CategoryID"),
                        F.when(
                            F.col("_parent") == 0,
                            F.when(F.col("RFA.LTEntityID") == F.col("K1P.LowerTierEntityID"), F.lit(0))
                            .otherwise(F.col("RFA.LTEntityID"))
                        ).otherwise(F.col("_parent")).cast("int").alias("ParentEntityID"),
                        F.col("RFA.LTEntityID").cast("int").alias("SuperParentEntityID"),
                        F.col("_tk").alias("TrackingKey"),
                        F.lit(None).cast("string").alias("Tag"),
                        F.lit(None).cast("int").alias("SchID"),
                        F.col("_orig_parent").cast("int").alias("OriginalParentEntityID"),
                    )
                )
                result = result.unionByName(footnote_pfic_insert)

            if is_foreign:
                foreign_pfic_insert = (
                    reclass_pfic_base
                    .groupBy(
                        F.col("K1P.LowerTierEntityID"),
                        F.col("RFA.LineID"),
                        F.col("RFA.FootnoteID"),
                        F.coalesce(F.col("RFA.ParentEntityID"), F.lit(0)).alias("_parent"),
                        F.col("RFA.LTEntityID"),
                        F.coalesce(F.col("RFA.TrackingKey"), F.lit("")).alias("_tk"),
                        F.coalesce(F.col("RFA.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
                    )
                    .agg(F.sum("RFA.FlowupAmount").alias("Amount"))
                    .select(
                        F.col("K1P.LowerTierEntityID").alias("EntityID"),
                        F.lit(pfic_lt).cast("int").alias("LineTypeID"),
                        F.col("RFA.LineID"),
                        F.col("Amount"),
                        F.col("RFA.FootnoteID").alias("QuicklinkID"),
                        F.lit(0).cast("int").alias("CategoryID"),
                        F.when(
                            F.col("_parent") == 0,
                            F.when(F.col("RFA.LTEntityID") == F.col("K1P.LowerTierEntityID"), F.lit(0))
                            .otherwise(F.col("RFA.LTEntityID"))
                        ).otherwise(F.col("_parent")).cast("int").alias("ParentEntityID"),
                        F.col("RFA.LTEntityID").cast("int").alias("SuperParentEntityID"),
                        F.col("_tk").alias("TrackingKey"),
                        F.lit(None).cast("string").alias("Tag"),
                        F.lit(None).cast("int").alias("SchID"),
                        F.col("_orig_parent").cast("int").alias("OriginalParentEntityID"),
                    )
                )
                result = result.unionByName(foreign_pfic_insert)

    log_timing("build_pfic_allocation_input", t0)
    return result


def apply_pfic_election_deletes(
    spark: SparkSession, cfg: dict,
    allocation_input_df: DataFrame, pfic_flowup_df: DataFrame,
    pfic_elections: dict, lower_tier_df: DataFrame,
) -> tuple:
    """Apply PFIC election-based deletes (1291/1293/1296 rules).

    SQL lines: 5669-6190. Builds election classification tables (#1291Elections,
    #1293Elections, #1296MTMElections), then removes allocation and flowup rows
    based on entity foreign/domestic status and PFIC type.
    Returns: (updated_allocation_input_df, updated_pfic_flowup_df)
    """
    log_section("apply_pfic_election_deletes")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    is_foreign_entity = cfg.get("is_foreign_entity", False)
    pfic_lt = cfg.get("pfic_footnote_line_type_id")
    type_of_pfic_line_id = cfg.get("type_of_pfic_line_id")
    part_vii_indicator = cfg.get("part_vii_indicator", 0)

    if not pfic_lt or not type_of_pfic_line_id:
        log_timing("apply_pfic_election_deletes", t0)
        return allocation_input_df, pfic_flowup_df

    # Build lower tier funds with IsForeign flag (includes current entity)
    current_entity_row = spark.createDataFrame(
        [(entity_id, bool(is_foreign_entity))],
        ["EntityID", "IsForeign"]
    ).select(
        F.col("EntityID").cast("int"),
        F.col("IsForeign").cast("boolean"),
    )
    ltf_with_foreign = (
        lower_tier_df.select(
            F.col("EntityID").cast("int"),
            F.col("IsForeign").cast("boolean"),
        )
        .unionByName(current_entity_row)
        .dropDuplicates(["EntityID"])
    )

    # --- Build #1291Elections ---
    # PFICs where TypeOfPFIC is IS1291NoDistribution or IS1291AnyDistribution
    elections_1291 = (
        pfic_flowup_df
        .filter(
            (F.col("RunID") == run_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("LineID") == type_of_pfic_line_id) &
            F.lower(F.col("TextValue")).isin("is1291nodistribution", "is1291anydistribution")
        )
        .join(ltf_with_foreign.alias("I"), F.col("FlowupEntityID") == F.col("I.EntityID"), "inner")
        .select(
            F.col("FlowupEntityID"), F.col("SourceEntityID"),
            F.col("PFICFootnoteID"),
            F.col("I.IsForeign").cast("int").alias("IsForeign"),
            F.lower(F.col("TextValue")).alias("PFICTYPE"),
            F.col("TrackingKey"),
        )
        .distinct()
    )

    type_of_foreign_corp_line_id = cfg.get("type_of_foreign_corp_line_id")
    if type_of_foreign_corp_line_id:
        qfc_cfc_for_1291 = (
            pfic_flowup_df
            .filter(
                (F.col("RunID") == run_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.col("LineID") == type_of_foreign_corp_line_id) &
                F.lower(F.col("TextValue")).isin("foreign corporation", "controlled foreign corporation")
            )
            .select(
                F.col("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("PFICFootnoteID"),
                F.lit(0).cast("int").alias("IsForeign"),
                F.lower(F.col("TextValue")).alias("PFICTYPE"),
                F.col("TrackingKey"),
            )
            .distinct()
        )
        elections_1291 = elections_1291.unionByName(qfc_cfc_for_1291).distinct()

    # --- Build #1293Elections ---
    elections_1293 = (
        pfic_flowup_df
        .filter(
            (F.col("RunID") == run_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("LineID") == type_of_pfic_line_id) &
            F.lower(F.col("TextValue")).isin("is1293eligiblenodeemed", "is1293eligibledeemed")
        )
        .join(ltf_with_foreign.alias("I"), F.col("FlowupEntityID") == F.col("I.EntityID"), "inner")
        .select(
            F.col("FlowupEntityID"), F.col("SourceEntityID"),
            F.col("PFICFootnoteID"),
            F.col("I.IsForeign").cast("int").alias("IsForeign"),
            F.lower(F.col("TextValue")).alias("PFICTYPE"),
            F.col("TrackingKey"),
        )
        .distinct()
    )

    # --- Build #1296MTMElections ---
    entity_rel_df = read_table(spark, "EntityRelationship", cfg)
    entity_src_df = read_table(spark, "Entity", cfg)   # ESRC: source entity (separate read avoids self-join ambiguity)
    vw_entity_df = read_table(spark, "Entity", cfg)     # E1: upper-tier entity
    inv_entity_type_id = cfg.get("inv_entity_type_id")

    elections_1296 = (
        pfic_flowup_df
        .filter(
            (F.col("RunID") == run_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("LineID") == type_of_pfic_line_id) &
            (F.lower(F.col("TextValue")) == "ismtom")
        )
        .join(ltf_with_foreign.alias("IE"), F.col("FlowupEntityID") == F.col("IE.EntityID"), "inner")
        .alias("PF")
        .join(
            entity_src_df.alias("ESRC"),
            F.col("PF.SourceEntityID") == F.col("ESRC.EntityID"),
            "inner"
        )
        .join(
            entity_rel_df.alias("ER"),
            (F.col("PF.SourceEntityID") == F.col("ER.LowerTierEntityID")) &
            (F.col("ER.ClientID") == client_id) &
            (F.col("ER.TaxPeriodID") == tax_period_id) &
            (F.col("ESRC.FundOrInvestmentID") == F.lit(inv_entity_type_id)),
            "left"
        )
        .join(
            vw_entity_df.alias("E1"),
            F.col("ER.UpperTierEntityID") == F.col("E1.EntityID"),
            "left"
        )
        .select(
            F.col("PF.FlowupEntityID"),
            F.col("PF.PFICFootnoteID"),
            # After .alias("PF") above, the ltf_with_foreign (IE) columns are
            # rolled into the PF namespace; reference IsForeign via PF.
            F.col("PF.IsForeign").cast("int").alias("IsForeign"),
            F.lower(F.col("PF.TextValue")).alias("PFICTYPE"),
            F.col("PF.TrackingKey"),
            F.when(
                F.col("E1.EntityID").isNotNull(),
                F.col("E1.IsForeign").cast("int")
            ).otherwise(F.col("ESRC.IsForeign").cast("int")).alias("IsSourceEntityForeign"),
        )
        .distinct()
    )

    # --- Build #12931291and1296Elections (union) ---
    # Normalize schemas for union
    elections_combined = (
        elections_1293.select("FlowupEntityID", "SourceEntityID", "PFICFootnoteID", "PFICTYPE", "IsForeign", "TrackingKey")
        .union(elections_1296.select(
            F.col("FlowupEntityID"), F.lit(None).cast("int").alias("SourceEntityID"),
            F.col("PFICFootnoteID"), F.col("PFICTYPE"), F.col("IsForeign"), F.col("TrackingKey")
        ))
        .union(elections_1291.select("FlowupEntityID", "SourceEntityID", "PFICFootnoteID", "PFICTYPE", "IsForeign", "TrackingKey"))
        .distinct()
    )
    elections_combined.createOrReplaceTempView(f"_elections_combined_{run_id}")

    # --- Build #1293ElectionsExclude (PFICs reclassed via lookthrough adjustments) ---
    lookthrough_reclass_wf = cfg.get("lookthrough_reclass_workflow_id", 0)
    if lookthrough_reclass_wf:
        lt_adj_df = (
            read_table(spark, "LookthroughAdjustments_Snapshot", cfg)
            .filter(
                (F.col("WorkflowID") == lookthrough_reclass_wf) &
                (F.col("SourceID") == pfic_lt)
            )
        )
        elections_1293_exclude = (
            pfic_flowup_df
            .filter(
                (F.col("RunID") == run_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.col("LineID") == type_of_pfic_line_id)
            )
            .alias("PF")
            .join(ltf_with_foreign.alias("I"), F.col("PF.FlowupEntityID") == F.col("I.EntityID"), "inner")
            .join(
                lt_adj_df.alias("LTR"),
                (F.col("LTR.SuperParentEntityID") == F.col("PF.FlowupEntityID")) &
                (F.col("LTR.SourceEntityID") == F.col("PF.SourceEntityID")) &
                (F.coalesce(F.col("LTR.TrackingKey"), F.lit("")) == F.coalesce(F.col("PF.TrackingKey"), F.lit(""))) &
                (F.col("PF.PFICFootnoteID") == F.col("LTR.FootNoteID")) &
                (F.lower(F.col("LTR.AdjustmentAmount")) == "is1293eligiblenodeemed") &
                (F.col("LTR.SourceID") == pfic_lt) &
                (F.col("LTR.EntityID") == entity_id),
                "inner"
            )
            .select(
                F.col("PF.FlowupEntityID"), F.col("PF.SourceEntityID"),
                F.col("PF.PFICFootnoteID"),
                F.col("I.IsForeign").cast("int").alias("IsForeign"),
                F.lower(F.col("PF.TextValue")).alias("PFICTYPE"),
                F.col("PF.TrackingKey"),
            )
            .distinct()
        )
    else:
        elections_1293_exclude = spark.createDataFrame(
            [], elections_1293.schema
        )

    # --- Delete is1293EligibleNoDeemed PFICs flowing up from Domestic ---
    # DELETE from #PFICFootnoteFlowup WHERE 1293.IsForeign=0 AND PFICTYPE='is1293EligibleNoDeemed'
    # AND FlowupEntityID <> @LocalEntityID, excluding 1293ElectionsExclude
    delete_1293_noDeemed = (
        elections_1293
        .filter(
            (F.col("IsForeign") == 0) &
            (F.col("PFICTYPE") == "is1293eligiblenodeemed") &
            (F.col("FlowupEntityID") != entity_id)
        )
        .select(
            F.col("FlowupEntityID").alias("d_FlowupEntityID"),
            F.col("SourceEntityID").alias("d_SourceEntityID"),
            F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
            F.col("TrackingKey").alias("d_TrackingKey"),
        )
    )
    # Exclude entries in elections_1293_exclude
    if lookthrough_reclass_wf:
        excl_keys = elections_1293_exclude.select(
            F.col("FlowupEntityID").alias("ex_FlowupEntityID"),
            F.col("SourceEntityID").alias("ex_SourceEntityID"),
            F.col("PFICFootnoteID").alias("ex_PFICFootnoteID"),
            F.col("TrackingKey").alias("ex_TrackingKey"),
        )
        delete_1293_noDeemed = (
            delete_1293_noDeemed.alias("D")
            .join(
                excl_keys.alias("EX"),
                (F.col("D.d_FlowupEntityID") == F.col("EX.ex_FlowupEntityID")) &
                (F.col("D.d_SourceEntityID") == F.col("EX.ex_SourceEntityID")) &
                (F.col("D.d_PFICFootnoteID") == F.col("EX.ex_PFICFootnoteID")) &
                (F.coalesce(F.col("D.d_TrackingKey"), F.lit("")) == F.coalesce(F.col("EX.ex_TrackingKey"), F.lit(""))),
                "left_anti"
            )
        )

    # Apply flowup delete
    pfic_flowup_df = (
        pfic_flowup_df.alias("PF")
        .join(
            delete_1293_noDeemed.alias("D"),
            (F.col("PF.FlowupEntityID") == F.col("D.d_FlowupEntityID")) &
            (F.col("PF.SourceEntityID") == F.col("D.d_SourceEntityID")) &
            (F.col("PF.PFICFootnoteID") == F.col("D.d_PFICFootnoteID")) &
            (F.coalesce(F.col("PF.TrackingKey"), F.lit("")) == F.coalesce(F.col("D.d_TrackingKey"), F.lit(""))) &
            (F.col("PF.RunID") == run_id),
            "left_anti"
        )
    )

    # --- For FOREIGN entity: Delete 1296 MToM from AllocationInput where IsForeign=0 ---
    if is_foreign_entity:
        delete_1296_foreign = (
            elections_1296
            .filter(
                (F.col("IsForeign") == 0) &
                (F.col("FlowupEntityID") == entity_id)
            )
            .select(
                F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
                F.col("TrackingKey").alias("d_TrackingKey"),
            )
        )
        allocation_input_df = (
            allocation_input_df.alias("A")
            .join(
                delete_1296_foreign.alias("D"),
                (F.col("A.QuicklinkID") == F.col("D.d_PFICFootnoteID")) &
                ((F.col("A.TrackingKey") == F.col("D.d_TrackingKey"))) &
                (F.col("A.LineTypeID") == pfic_lt) &
                (F.col("A.OriginalParentEntityID").isNull()),
                "left_anti"
            )
        )
    else:
        # --- For DOMESTIC entity ---
        # Delete 1296 MToM
        delete_1296_domestic = (
            elections_1296
            .filter(F.col("FlowupEntityID") == entity_id)
            .select(
                F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
                F.col("TrackingKey").alias("d_TrackingKey"),
            )
        )
        allocation_input_df = (
            allocation_input_df.alias("A")
            .join(
                delete_1296_domestic.alias("D"),
                (F.col("A.QuicklinkID") == F.col("D.d_PFICFootnoteID")) &
                ((F.col("A.TrackingKey") == F.col("D.d_TrackingKey"))) &
                (F.col("A.LineTypeID") == pfic_lt),
                "left_anti"
            )
        )

        # Delete IS1291NoDistribution unless FootnoteElected or PartVII indicator enabled
        # Build #FootnoteElectedPFIC
        footnote_elected = _build_footnote_elected(
            spark,
            cfg,
            prune_to_lower_tier_runs(
                read_table(spark, "PFICFootnoteFlowupWithTrackingKey", cfg),
                spark,
                cfg,
            ),
        )

        delete_1291_domestic = (
            elections_1291
            .filter(
                (F.col("PFICTYPE") == "is1291nodistribution")
            )
            .select(
                F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
                F.col("TrackingKey").alias("d_TrackingKey"),
            )
        )
        if part_vii_indicator != 1:
            # Only delete if PartVII indicator is NOT enabled
            delete_1291_domestic_filtered = (
                delete_1291_domestic.alias("D")
                .join(
                    footnote_elected.select(
                        F.col("PFICFootnoteID").alias("fe_PFICFootnoteID"),
                        F.col("TrackingKey").alias("fe_TrackingKey"),
                    ).alias("FE"),
                    (F.col("D.d_PFICFootnoteID") == F.col("FE.fe_PFICFootnoteID")) &
                    (F.coalesce(F.col("D.d_TrackingKey"), F.lit("")) ==
                     F.coalesce(F.col("FE.fe_TrackingKey"), F.lit(""))),
                    "left_anti"
                )
            )
            allocation_input_df = (
                allocation_input_df.alias("A")
                .join(
                    delete_1291_domestic_filtered.alias("D"),
                    (F.col("A.QuicklinkID") == F.col("D.d_PFICFootnoteID")) &
                    ((F.col("A.TrackingKey") == F.col("D.d_TrackingKey"))) &
                    (F.col("A.LineTypeID") == pfic_lt),
                    "left_anti"
                )
            )

        reclass_base_g3 = _build_reclass_pfic_base(spark, cfg, pfic_lt)
        if reclass_base_g3 is not None:
            type_filter = F.col("PFICTYPE").isin(
                "is1291anydistribution", "foreign corporation", "controlled foreign corporation"
            )
            if part_vii_indicator == 1:
                type_filter = type_filter | (F.col("PFICTYPE") == "is1291nodistribution")
            elec_1291_gate = (
                elections_1291
                .filter((F.col("FlowupEntityID") != entity_id) & type_filter)
                .select(
                    F.col("PFICFootnoteID").alias("e_FootnoteID"),
                    F.coalesce(F.col("TrackingKey"), F.lit("")).alias("e_TrackingKey"),
                    F.col("FlowupEntityID").alias("e_FlowupEntityID"),
                )
                .distinct()
            )
            fe_anti_g3 = footnote_elected.select(
                F.col("FlowUpEntityID").alias("fe_FlowupEntityID"),
                F.col("PFICFootnoteID").alias("fe_FootnoteID"),
                F.coalesce(F.col("TrackingKey"), F.lit("")).alias("fe_TrackingKey"),
            )
            gate3 = (
                reclass_base_g3
                .join(
                    elec_1291_gate,
                    (F.col("RFA.FootnoteID") == F.col("e_FootnoteID")) &
                    (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.col("e_TrackingKey")) &
                    (F.col("RFA.LTEntityID") == F.col("e_FlowupEntityID")),
                    "inner"
                )
                .join(
                    fe_anti_g3,
                    (F.col("e_FlowupEntityID") == F.col("fe_FlowupEntityID")) &
                    (F.col("RFA.FootnoteID") == F.col("fe_FootnoteID")) &
                    (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.col("fe_TrackingKey")),
                    "left_anti"
                )
            )
            gate3_rows = _pfic_k1_conversion_rows(gate3, entity_id, pfic_lt)
            allocation_input_df = allocation_input_df.unionByName(gate3_rows, allowMissingColumns=True)

        # Delete IS1293EligibleNoDeemed from AllocationInput for domestic entity
        delete_1293_ai = (
            elections_1293
            .filter(
                (F.col("FlowupEntityID") == entity_id) &
                (F.col("PFICTYPE") == "is1293eligiblenodeemed")
            )
            .select(
                F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
                F.col("TrackingKey").alias("d_TrackingKey"),
            )
        )
        allocation_input_df = (
            allocation_input_df.alias("A")
            .join(
                delete_1293_ai.alias("D"),
                (F.col("A.QuicklinkID") == F.col("D.d_PFICFootnoteID")) &
                ((F.col("A.TrackingKey") == F.col("D.d_TrackingKey"))) &
                (F.col("A.LineTypeID") == pfic_lt),
                "left_anti"
            )
        )

        # Delete IS1293EligibleDeemed exclusion lines from AllocationInput
        # Get 1293 PFIC exclusion lines from ENU_DataList
        pfic_line_item_df = spark.table("_pfic_line_item")
        excl_line_ids_df = (
            read_table(spark, "ENU_DataList", cfg)
            .filter(F.lower(F.col("Category")) == "1293pficlineexclude")
            .select(F.lower(F.col("Value")).alias("_sn_lc"))
            .join(
                pfic_line_item_df.select(F.lower(F.col("ShortName")).alias("_sn_lc"), F.col("LineID")),
                "_sn_lc", "inner"
            )
            .select("LineID")
        )
        elections_1293_deemed = (
            elections_1293
            .filter(F.col("PFICTYPE") == "is1293eligibledeemed")
            .select(
                F.col("PFICFootnoteID").alias("d_PFICFootnoteID"),
                F.col("SourceEntityID").alias("d_SourceEntityID"),
                F.col("TrackingKey").alias("d_TrackingKey"),
            )
        )
        _fe_keys_1293 = footnote_elected.select(
            F.col("PFICFootnoteID").alias("fe_FootnoteID"),
            F.coalesce(F.col("TrackingKey"), F.lit("")).alias("fe_TrackingKey"),
        )
        elections_1293_deemed = (
            elections_1293_deemed.alias("D")
            .join(
                _fe_keys_1293.alias("FE"),
                (F.col("D.d_PFICFootnoteID") == F.col("FE.fe_FootnoteID")) &
                (F.coalesce(F.col("D.d_TrackingKey"), F.lit("")) == F.col("FE.fe_TrackingKey")),
                "left_anti"
            )
        )
        # Delete rows matching 1293 Deemed + exclusion line IDs (not footnote-elected)
        if elections_1293_deemed.head(1):
            excl_ids = [r["LineID"] for r in excl_line_ids_df.collect()]
            if excl_ids:
                allocation_input_df = (
                    allocation_input_df.alias("A")
                    .join(
                        elections_1293_deemed.alias("D"),
                        (F.col("A.QuicklinkID") == F.col("D.d_PFICFootnoteID")) &
                        ((F.col("A.TrackingKey") == F.col("D.d_TrackingKey"))) &
                        (F.col("A.EntityID") == F.col("D.d_SourceEntityID")) &
                        (F.col("A.LineTypeID") == pfic_lt) &
                        F.col("A.LineID").isin(excl_ids),
                        "left_anti"
                    )
                )

        # --- 1293-Deemed PFIC->K-1 conversion insert (domestic) ---
        reclass_base_g4 = _build_reclass_pfic_base(spark, cfg, pfic_lt)
        if reclass_base_g4 is not None:
            elec_1293_gate = (
                elections_1293
                .filter(
                    (F.col("PFICTYPE") == "is1293eligibledeemed") &
                    (F.col("FlowupEntityID") != entity_id)
                )
                .select(
                    F.col("PFICFootnoteID").alias("e_FootnoteID"),
                    F.coalesce(F.col("TrackingKey"), F.lit("")).alias("e_TrackingKey"),
                    F.col("FlowupEntityID").alias("e_FlowupEntityID"),
                )
                .distinct()
            )
            fe_anti_g4 = footnote_elected.select(
                F.col("FlowUpEntityID").alias("fe_FlowupEntityID"),
                F.col("PFICFootnoteID").alias("fe_FootnoteID"),
                F.coalesce(F.col("TrackingKey"), F.lit("")).alias("fe_TrackingKey"),
            )
            gate4 = (
                reclass_base_g4
                .join(
                    elec_1293_gate,
                    (F.col("RFA.FootnoteID") == F.col("e_FootnoteID")) &
                    (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.col("e_TrackingKey")) &
                    (F.col("RFA.LTEntityID") == F.col("e_FlowupEntityID")),
                    "inner"
                )
                .join(
                    fe_anti_g4,
                    (F.col("e_FlowupEntityID") == F.col("fe_FlowupEntityID")) &
                    (F.col("RFA.FootnoteID") == F.col("fe_FootnoteID")) &
                    (F.coalesce(F.col("RFA.TrackingKey"), F.lit("")) == F.col("fe_TrackingKey")),
                    "left_anti"
                )
            )
            # Exclude #tmp1293PFICExclusion lines (E.LineID = PL.LineID == RFA.LineID)
            g4_excl_ids = [r["LineID"] for r in excl_line_ids_df.collect()]
            if g4_excl_ids:
                gate4 = gate4.filter(~F.col("RFA.LineID").isin(g4_excl_ids))
            gate4_rows = _pfic_k1_conversion_rows(gate4, entity_id, pfic_lt)
            allocation_input_df = allocation_input_df.unionByName(gate4_rows, allowMissingColumns=True)

    log_timing("apply_pfic_election_deletes", t0)
    return allocation_input_df, pfic_flowup_df


def _build_footnote_elected(spark: SparkSession, cfg: dict, pfic_flowup_df: DataFrame) -> DataFrame:
    """Build #FootnoteElectedPFIC: PFICs classified as Footnote/DeemedElection/Footnote-Amounts Only."""
    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]
    pfic_investment_line_id = cfg.get("pfic_investment_line_id")

    if not pfic_investment_line_id:
        return spark.createDataFrame([], "FlowUpEntityID int, PFICFootnoteID int, TrackingKey string")

    pfic_class_df = current_run_scoped(
        read_table(spark, "PficForeignCorpClassificationInput", cfg), cfg
    )

    footnote_elected = (
        pfic_class_df.alias("PC")
        .join(
            pfic_flowup_df.filter(
                (F.col("LineID") == pfic_investment_line_id) &
                (F.col("RunID") == run_id)
            ).alias("PF"),
            (F.col("PC.EntityID") == F.col("PF.TextValue")) &
            (F.col("PC.TrackingKey").isNull() |
             (F.col("PC.TrackingKey") ==
              F.concat(F.col("PF.TrackingKey"), F.lit("~"), F.lit(entity_id).cast("string")))) &
            (F.col("PC.PficFootnoteID").isNull() |
             (F.col("PC.PficFootnoteID").cast("int") == F.col("PF.PFICFootnoteID"))) &
            (F.col("PF.LineID") == pfic_investment_line_id),
            "inner"
        )
        .filter(
            (F.col("PC.SourceEntityID") == entity_id) &
            F.lower(F.col("PC.FootnoteClassification")).isin("footnote", "deemedelection", "footnote-amounts only")
        )
        .select(
            F.col("PC.FlowupEntityID"),
            F.col("PF.PFICFootnoteID"),
            F.col("PF.TrackingKey"),
        )
        .distinct()
    )
    return footnote_elected


def _build_reclass_pfic_base(spark: SparkSession, cfg: dict, pfic_lt: int):
    """Build the reclass PFIC base for the PFIC->K-1 conversion inserts (C1 Gates 3 & 4).

    #ReclassFootnoteAllocationUnblockedData joined to PFICFootnoteLineItem
    (NUMBER, allocated, active) + PFICFootnotePackage + K1Package. _reclass_data is
    already scoped to the current RunID/ClientID/TaxPeriodID. Returns the aliased
    DataFrame (RFA/PL/PP/K1P) or None if reclass data is absent.
    """
    try:
        reclass_data = spark.table("_reclass_data")
    except Exception:
        return None
    pfic_pkg_df = read_table(spark, "PFICFootnotePackage", cfg)
    k1_pkg_df = spark.table("_k1_package")
    pfic_line_item_df = spark.table("_pfic_line_item")
    from .ai_pfic_flowup_service import _filter_reclass_to_unblocked
    reclass_data = _filter_reclass_to_unblocked(spark, cfg, reclass_data)
    return (
        reclass_data.alias("RFA")
        .filter(F.col("RFA.LineTypeID") == pfic_lt)
        .join(
            pfic_line_item_df.filter(
                (F.col("IsAllocated") == True) &
                (F.upper(F.col("LineDataType")) == "NUMBER") &
                (F.col("IsActive") == True)
            ).alias("PL"),
            F.col("RFA.LineID") == F.col("PL.LineID"),
            "inner"
        )
        .join(
            pfic_pkg_df.alias("PP"),
            F.col("PP.PFICFootnoteID") == F.col("RFA.FootnoteID"),
            "inner"
        )
        .join(
            k1_pkg_df.alias("K1P"),
            F.col("K1P.K1PackageID") == F.col("PP.K1PackageID"),
            "inner"
        )
    )


def _pfic_k1_conversion_rows(reclass_base: DataFrame, entity_id: int, pfic_lt: int) -> DataFrame:
    """Apply the PFIC->K-1 SUM(FlowupAmount) groupBy + ParentEntityID CASE select.

    Shared by C1 Gate 3 (1291 elections) and Gate 4 (1293-Deemed). reclass_base must
    still carry the RFA (reclass) and K1P (K1 package) aliases. Post-groupBy columns
    are referenced by their simple names.
    """
    return (
        reclass_base
        .groupBy(
            F.col("K1P.LowerTierEntityID"),
            F.col("RFA.LineID"),
            F.col("RFA.FootnoteID"),
            F.coalesce(F.col("RFA.ParentEntityID"), F.lit(0)).alias("_parent"),
            F.col("RFA.LTEntityID"),
            F.coalesce(F.col("RFA.TrackingKey"), F.lit("")).alias("_tk"),
            F.coalesce(F.col("RFA.OriginalParentEntityID"), F.lit(entity_id)).alias("_orig_parent"),
        )
        .agg(F.sum(F.col("RFA.FlowupAmount")).alias("Amount"))
        .select(
            F.col("LowerTierEntityID").alias("EntityID"),
            F.lit(pfic_lt).cast("int").alias("LineTypeID"),
            F.col("LineID"),
            F.col("Amount"),
            F.col("FootnoteID").alias("QuicklinkID"),
            F.lit(0).cast("int").alias("CategoryID"),
            F.when(
                F.col("_parent") == 0,
                F.when(F.col("LTEntityID") == F.col("LowerTierEntityID"), F.lit(0))
                .otherwise(F.col("LTEntityID"))
            ).otherwise(F.col("_parent")).cast("int").alias("ParentEntityID"),
            F.col("LTEntityID").cast("int").alias("SuperParentEntityID"),
            F.col("_tk").alias("TrackingKey"),
            F.lit(None).cast("string").alias("Tag"),
            F.lit(None).cast("int").alias("SchID"),
            F.col("_orig_parent").cast("int").alias("OriginalParentEntityID"),
        )
    )


def apply_part_v_vii_flags(
    spark: SparkSession, cfg: dict, pfic_flowup_df: DataFrame,
) -> DataFrame:
    """Apply Part V / Part VII flag updates on PFIC flowup TextValue.

    SQL lines: 6259-6420. Uses the #12931291and1296Elections table to determine
    IsPart_5 and IsPart_7 values based on PFIC type and entity domicile.
    Updates TextValue on the matching IsPart_5/IsPart_7 line rows.
    """
    log_section("apply_part_v_vii_flags")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    is_foreign_entity = cfg.get("is_foreign_entity", False)
    part_vii_indicator = cfg.get("part_vii_indicator", 0)
    type_of_pfic_line_id = cfg.get("type_of_pfic_line_id")
    pfic_lt = cfg.get("pfic_footnote_line_type_id")
    is_part_5_line_id = cfg.get("pfic_is_part_5_line_id")
    is_part_7_line_id = cfg.get("pfic_is_part_7_line_id")

    if not type_of_pfic_line_id or not pfic_lt:
        log_timing("apply_part_v_vii_flags", t0)
        return pfic_flowup_df

    # Build elections_combined from the temp view registered in apply_pfic_election_deletes
    try:
        elections_combined = spark.table(f"_elections_combined_{run_id}")
    except Exception:
        # If elections weren't built (no pfic_lt), skip
        log_timing("apply_part_v_vii_flags", t0)
        return pfic_flowup_df

    if not is_part_5_line_id and not is_part_7_line_id:
        log_timing("apply_part_v_vii_flags", t0)
        return pfic_flowup_df

    # --- Build #DomesticEntity and #LowerTiersThroughDomesticEntity ---
    alloc_run_df = read_table(spark, "AllocationRun", cfg)
    vw_entity_full = read_table(spark, "Entity", cfg)
    enu_tax_class = read_table(spark, "ENU_TaxClass", cfg)
    try:
        entity_hier_df = spark.table(f"_entity_hierarchy_{run_id}")
    except Exception:
        entity_hier_df = (
            read_table(spark, "EntityRelationship", cfg)
            .filter((F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id))
            .select("UpperTierEntityID", "LowerTierEntityID")
        )

    max_run_per_entity = (
        alloc_run_df
        .groupBy("EntityID")
        .agg(F.max("RunID").alias("MaxRunID"))
    )

    # #DomesticEntity: hierarchy upper-tier entities that are domestic + not Disregarded,
    # carrying their global MAX RunID (no RunStatus='SUCCESS' filter — matches SP L3105-3110).
    domestic_entities = (
        entity_hier_df
        .select(F.col("UpperTierEntityID"))
        .distinct()
        .join(
            vw_entity_full.alias("VE"),
            F.col("UpperTierEntityID") == F.col("VE.EntityID"),
            "inner"
        )
        .join(
            enu_tax_class.alias("TC"),
            F.col("VE.TaxClassID") == F.col("TC.TaxClassID"),
            "left"
        )
        .join(
            max_run_per_entity.alias("AR"),
            F.col("VE.EntityID") == F.col("AR.EntityID"),
            "inner"
        )
        .filter(
            (F.coalesce(F.col("VE.IsForeign"), F.lit(False)) == False) &
            (F.lower(F.coalesce(F.col("TC.TaxClassName"), F.lit(""))) != "disregarded entity")
        )
        .select(
            F.col("VE.EntityID").alias("DomesticUpperTierID"),
            F.col("AR.MaxRunID").alias("DomRunID"),
        )
        .distinct()
    )

    lower_tiers_through_domestic = (
        pfic_flowup_df.alias("P")
        .join(
            domestic_entities.alias("D"),
            (F.col("D.DomRunID") == F.col("P.RunID")) &
            (F.col("P.EntityID") == F.col("D.DomesticUpperTierID")),
            "inner"
        )
        .join(
            elections_combined.alias("EC"),
            (F.col("EC.SourceEntityID") == F.col("P.SourceEntityID")) &
            (F.col("EC.PFICFootnoteID") == F.col("P.PFICFootnoteID")) &
            (F.coalesce(F.col("EC.TrackingKey"), F.lit("")) ==
             F.coalesce(F.col("P.TrackingKey"), F.lit(""))) &
            (F.col("EC.FlowupEntityID") != F.col("EC.SourceEntityID")),
            "inner"
        )
        .select(
            F.col("EC.PFICFootnoteID"),
            F.col("EC.TrackingKey"),
            F.col("EC.FlowupEntityID"),
            F.col("EC.SourceEntityID"),
            F.col("EC.PFICTYPE"),
        )
        .distinct()
    )

    # Override IsForeign=0 for elections flowing through domestic
    elections_combined = (
        elections_combined.alias("E")
        .join(
            lower_tiers_through_domestic.alias("LTD"),
            (F.col("E.PFICFootnoteID") == F.col("LTD.PFICFootnoteID")) &
            (F.coalesce(F.col("E.TrackingKey"), F.lit("")) == F.coalesce(F.col("LTD.TrackingKey"), F.lit(""))) &
            (F.col("E.FlowupEntityID") == F.col("LTD.FlowupEntityID")) &
            # SQL UPDATE (SP L3126-3127) uses plain = on SourceEntityID and PFICTYPE;
            # T-SQL `=` is NULL-intolerant, so a NULL SourceEntityID (1296 election rows
            # carry SourceEntityID=NULL) never matches — preserve that semantics with
            # plain == (no coalesce) rather than over-matching NULL=NULL.
            (F.col("E.SourceEntityID") == F.col("LTD.SourceEntityID")) &
            (F.col("E.PFICTYPE") == F.col("LTD.PFICTYPE")),
            "left"
        )
        .select(
            F.col("E.FlowupEntityID"),
            F.col("E.SourceEntityID"),
            F.col("E.PFICFootnoteID"),
            F.col("E.PFICTYPE"),
            F.when(F.col("LTD.PFICFootnoteID").isNotNull(), F.lit(0))
             .otherwise(F.col("E.IsForeign")).alias("IsForeign"),
            F.col("E.TrackingKey"),
        )
    )

    # --- Determine domestic lower-tier entities that have flowed up ---
    lower_tier_df = (
        vw_entity_full
        .filter(F.col("IsForeign") == False)
        .select("EntityID", "IsForeign")
    )

    # --- Update Part V (IsPart_5) ---
    if is_part_5_line_id:
        # Build Part V flag based on PFIC type
        part5_elections = (
            pfic_flowup_df
            .filter(
                (F.col("RunID") == run_id) &
                (F.col("LineID") == is_part_5_line_id)
            )
            .alias("P")
            .join(
                elections_combined.alias("E"),
                (F.col("P.PFICFootnoteID") == F.col("E.PFICFootnoteID")) &
                (F.coalesce(F.col("P.TrackingKey"), F.lit("")) == F.coalesce(F.col("E.TrackingKey"), F.lit(""))),
                "left"
            )
            .select(
                F.col("P.PFICFootnoteID"), F.col("P.EntityID"),
                F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),
                F.col("P.LineID"), F.col("P.TrackingKey"),
                F.col("P.TextValue").alias("OrigTextValue"),
                F.col("E.PFICTYPE"), F.col("E.IsForeign").alias("E_IsForeign"),
                F.when(F.col("E.PFICTYPE") == "is1291nodistribution", F.lit("False"))
                .when(F.col("E.PFICTYPE") == "is1291anydistribution", F.lit("True"))
                .when(
                    F.col("E.PFICTYPE").isin("foreign corporation", "controlled foreign corporation"),
                    F.when(F.lower(F.col("P.TextValue")).isin("x", "true"), F.lit("True")).otherwise(F.lit("False"))
                )
                .when(
                    (F.lit(not is_foreign_entity)) &
                    F.col("E.PFICTYPE").isin("ismtom", "is1293eligiblenodeemed", "is1293eligibledeemed"),
                    F.lit("False")
                )
                .when(
                    F.lit(is_foreign_entity) & (F.col("E.IsForeign") == 0) &
                    F.col("E.PFICTYPE").isin("ismtom", "is1293eligiblenodeemed", "is1293eligibledeemed"),
                    F.lit("False")
                )
                .when(
                    F.lit(is_foreign_entity) & (F.col("E.IsForeign") == 1) &
                    F.col("E.PFICTYPE").isin("ismtom", "is1293eligiblenodeemed", "is1293eligibledeemed"),
                    F.lit("True")
                )
                # Non-election (LEFT-join unmatched) IsPart_5 rows keep their original value here;
                # the zero-amount override below then forces 'False' when Part-5 amounts sum to 0.
                .otherwise(F.col("P.TextValue"))
                .alias("IsPart5")
            )
        )

        # Also compute Part 5 sum for zero-amount override
        pfic_line_item_df = spark.table("_pfic_line_item")
        part5_amount_lines = (
            pfic_line_item_df
            .filter(
                F.col("ShortName").isin(
                    "Part_5_E", "Part_5_F", "Part_5_H", "Part_5_I",
                    "Part_5_E_AmountOfNiiPtep", "Part_5_G_AmountOfNii"
                )
            )
            .select("LineID")
        )
        part5_sums = (
            pfic_flowup_df
            .filter(F.col("RunID") == run_id)
            .join(part5_amount_lines, "LineID", "inner")
            .groupBy("PFICFootnoteID", "EntityID", "FlowupEntityID", "SourceEntityID")
            .agg(F.sum(F.coalesce(F.col("Amount"), F.lit(0))).alias("TotalAmount"))
        )

        # Join Part 5 flag with sum to override to False when TotalAmount=0
        part5_updates = (
            part5_elections.alias("F5")
            .join(
                part5_sums.alias("S"),
                (F.col("F5.PFICFootnoteID") == F.col("S.PFICFootnoteID")) &
                (F.col("F5.EntityID") == F.col("S.EntityID")) &
                (F.col("F5.FlowupEntityID") == F.col("S.FlowupEntityID")) &
                (F.col("F5.SourceEntityID") == F.col("S.SourceEntityID")),
                "left"
            )
            .select(
                F.col("F5.PFICFootnoteID"), F.col("F5.EntityID"),
                F.col("F5.FlowupEntityID"), F.col("F5.SourceEntityID"),
                F.col("F5.LineID"), F.col("F5.TrackingKey"),
                F.when(
                    F.coalesce(F.col("S.TotalAmount"), F.lit(0)) == 0,
                    F.lit("False")
                ).otherwise(F.col("F5.IsPart5")).alias("FinalPart5"),
            )
            .dropDuplicates(["PFICFootnoteID", "EntityID", "FlowupEntityID", "SourceEntityID", "LineID", "TrackingKey"])
        )

        # Apply Part 5 update to pfic_flowup_df
        pfic_flowup_df = (
            pfic_flowup_df.alias("P")
            .join(
                part5_updates.alias("U"),
                (F.col("P.PFICFootnoteID") == F.col("U.PFICFootnoteID")) &
                (F.col("P.EntityID") == F.col("U.EntityID")) &
                (F.col("P.FlowupEntityID") == F.col("U.FlowupEntityID")) &
                (F.col("P.SourceEntityID") == F.col("U.SourceEntityID")) &
                (F.col("P.LineID") == F.col("U.LineID")) &
                (F.coalesce(F.col("P.TrackingKey"), F.lit("")) == F.coalesce(F.col("U.TrackingKey"), F.lit(""))),
                "left"
            )
            .select(
                F.col("P.RunID"), F.col("P.ClientID"), F.col("P.TaxPeriodID"),
                F.col("P.EntityID"), F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),
                F.col("P.PFICFootnoteID"), F.col("P.LineID"), F.col("P.Amount"),
                F.when(F.col("U.FinalPart5").isNotNull(), F.col("U.FinalPart5"))
                .otherwise(F.col("P.TextValue")).alias("TextValue"),
                F.col("P.TrackingKey"),
            )
        )

    # --- Update Part VII (IsPart_7) ---
    if is_part_7_line_id:
        part7_elections = (
            pfic_flowup_df
            .filter(
                (F.col("RunID") == run_id) &
                (F.col("LineID") == is_part_7_line_id)
            )
            .alias("P")
            .join(
                elections_combined.alias("E"),
                (F.col("P.PFICFootnoteID") == F.col("E.PFICFootnoteID")) &
                (F.coalesce(F.col("P.TrackingKey"), F.lit("")) == F.coalesce(F.col("E.TrackingKey"), F.lit(""))),
                "inner"
            )
            .select(
                F.col("P.PFICFootnoteID"), F.col("P.EntityID"),
                F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),
                F.col("P.LineID"), F.col("P.TrackingKey"),
                F.col("P.TextValue").alias("OrigTextValue"),
                F.col("E.PFICTYPE"),
                # Part VII logic from SQL:
                # PARTVIIINDICATOR=1 and IS1291NoDistribution -> True
                # FC/CFC -> False
                # IS1293EligibleDeemed/IS1291AnyDistribution: TextValue=True -> True, False -> False
                # TextValue=True AND domestic -> False
                # TextValue=True AND foreign -> True
                # TextValue=False -> False
                F.when(
                    (F.lit(part_vii_indicator) == 1) & (F.col("E.PFICTYPE") == "is1291nodistribution"),
                    F.lit("True")
                )
                .when(
                    F.col("E.PFICTYPE").isin("foreign corporation", "controlled foreign corporation"),
                    F.lit("False")
                )
                .when(
                    F.col("E.PFICTYPE").isin("is1293eligibledeemed", "is1291anydistribution") &
                    (F.lower(F.col("P.TextValue")) == "true"),
                    F.lit("True")
                )
                .when(
                    F.col("E.PFICTYPE").isin("is1293eligibledeemed", "is1291anydistribution") &
                    (F.lower(F.col("P.TextValue")) == "false"),
                    F.lit("False")
                )
                .when(
                    (F.lower(F.col("P.TextValue")) == "true") & F.lit(not is_foreign_entity),
                    F.lit("False")
                )
                .when(
                    (F.lower(F.col("P.TextValue")) == "true") & F.lit(is_foreign_entity),
                    F.lit("True")
                )
                .when(F.lower(F.col("P.TextValue")) == "false", F.lit("False"))
                .alias("IsPart7")
            )
            .select(
                F.col("P.PFICFootnoteID"), F.col("P.EntityID"),
                F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),
                F.col("P.LineID"), F.col("P.TrackingKey"),
                F.col("IsPart7"),
                F.lit(True).alias("_p7_matched"),
            )
            .dropDuplicates(["PFICFootnoteID", "EntityID", "FlowupEntityID", "SourceEntityID", "LineID", "TrackingKey"])
        )

        # Apply Part 7 update
        pfic_flowup_df = (
            pfic_flowup_df.alias("P")
            .join(
                part7_elections.alias("U7"),
                (F.col("P.PFICFootnoteID") == F.col("U7.PFICFootnoteID")) &
                (F.col("P.EntityID") == F.col("U7.EntityID")) &
                (F.col("P.FlowupEntityID") == F.col("U7.FlowupEntityID")) &
                (F.col("P.SourceEntityID") == F.col("U7.SourceEntityID")) &
                (F.col("P.LineID") == F.col("U7.LineID")) &
                (F.coalesce(F.col("P.TrackingKey"), F.lit("")) == F.coalesce(F.col("U7.TrackingKey"), F.lit(""))),
                "left"
            )
            .select(
                F.col("P.RunID"), F.col("P.ClientID"), F.col("P.TaxPeriodID"),
                F.col("P.EntityID"), F.col("P.FlowupEntityID"), F.col("P.SourceEntityID"),
                F.col("P.PFICFootnoteID"), F.col("P.LineID"), F.col("P.Amount"),
                F.when(F.col("U7._p7_matched").isNotNull(), F.col("U7.IsPart7"))
                .otherwise(F.col("P.TextValue")).alias("TextValue"),
                F.col("P.TrackingKey"),
            )
        )

    log_timing("apply_part_v_vii_flags", t0)
    return pfic_flowup_df
