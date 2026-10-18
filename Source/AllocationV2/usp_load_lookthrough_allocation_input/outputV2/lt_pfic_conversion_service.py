"""
pfic_conversion_service.py — Sections 13-14: PFIC-to-K1 conversion and income attributes.

Functions:
    build_pfic_conversion       — Section 13: Convert PFIC footnote amounts to K1 line items
    build_pfic_income_attributes — Section 14: Recalculate PFICtoK1IncomeAttributePercentages
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
import time

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger


# ---------------------------------------------------------------------------
# Section 13: build_pfic_conversion
# SQL lines ~2130-2870
# Converts PFIC footnote amounts into K1 line items based on election type.
# Only runs for domestic entities (IsForeignEntity = 0).
# ---------------------------------------------------------------------------

def build_pfic_conversion(
    spark: SparkSession,
    cfg: dict,
    reclass_unblocked_df: DataFrame,
    pfic_mapped_df: DataFrame,
    pfic_elections: dict,
    lower_tier_funds_df: DataFrame,
    pfic_footnote_entity_details_df: DataFrame,
    lt_amounts_df: DataFrame,
    pfic_types_df: DataFrame = None,
) -> tuple:
    """
    Convert PFIC footnote amounts to K1 line items.

    Returns:
        (pfic_alloc_input_df, converted_pfic_amounts_df)
        - pfic_alloc_input_df: rows to add to alloc_input (grouped by standard keys)
        - converted_pfic_amounts_df: PFIC amounts with country codes for income attribute calc
    Returns (None, None) if foreign entity.
    """
    log_section("build_pfic_conversion")
    t0 = time.time()

    if cfg["is_foreign_entity"]:
        log_timing("build_pfic_conversion", t0)
        return None, None

    if reclass_unblocked_df is None:
        logger.warning("reclass_unblocked_df is None (ReclassFootnoteAllocationData missing) — skipping PFIC conversion.")
        log_timing("build_pfic_conversion", t0)
        return None, None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    pfic_footnote_lt = cfg["pfic_footnote_line_type_id"]
    pfic_classification = cfg["pfic_classification"]
    pfic_6a = cfg["pfic_6a_line_id"]
    pfic_7a = cfg["pfic_7a_line_id"]
    pfic_7a_lt = cfg["pfic_7a_longterm_line_id"]
    pfic_8b = cfg["pfic_8b_distributions_line_id"]
    pfic_10c_lt = cfg["pfic_10c_longterm_line_id"]
    pfic_txt11 = cfg["pfic_txt11_held"]
    pfic_investment_line_id = cfg["pfic_investment_line_id"]
    qef_election_line_id = cfg["qef_election_line_id"]
    type_of_pfic_line_id = cfg["type_of_pfic_line_id"]
    type_of_foreign_corp_line_id = cfg["type_of_foreign_corp_line_id"]

    qef_elected_df = pfic_elections["qef_elected"]
    subpartf_elected_df = pfic_elections["subpartf_elected"]

    # -----------------------------------------------------------------------
    # Step 1: Build #RequiredPFICLines
    # -----------------------------------------------------------------------
    required_pfic_lines = (
        tbl(spark, "PFICFootnoteLineItem", cfg)
        .filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (
                F.col("LineID").isin(pfic_6a, pfic_7a, pfic_7a_lt, pfic_10c_lt, pfic_txt11, pfic_8b) |
                F.lower(F.col("ShortName")).isin("investment", "isqefelectionmade", "typeofpfic", "typeofforeigncorp")
            )
        )
        .select("ShortName", "LineID")
    )

    # -----------------------------------------------------------------------
    # Step 2: Build #TempPFICInput from PFICFootnoteInput_Snapshot
    # -----------------------------------------------------------------------
    k1_workflow = tbl(spark, "AllocationInputWorkflow", cfg).filter(
        (F.col("RunID") == run_id) &
        ~(F.coalesce(F.col("K1WorkflowID"), F.lit(0)) == 0)
    ).select(
        F.col("K1WorkflowID").alias("WorkflowID"),
        F.col("EntityID").alias("KW_EntityID"),
    )

    pfic_snapshot = tbl(spark, "PFICFootnoteInput_Snapshot", cfg)

    temp_pfic_input = (
        pfic_snapshot.alias("PFIC")
        .join(required_pfic_lines.alias("R"), F.col("PFIC.LineID") == F.col("R.LineID"))
        .join(k1_workflow.alias("KW"), F.col("PFIC.WorkflowID") == F.col("KW.WorkflowID"))
        .select(
            F.col("KW.KW_EntityID").alias("EntityID"),
            F.col("PFIC.PFICFootnoteID"),
            F.col("PFIC.LineID"),
            F.col("R.ShortName"),
            F.col("PFIC.Amount"),
            F.col("PFIC.TextValue"),
            F.col("KW.KW_EntityID").alias("SourceEntityID"),
            F.col("PFIC.ClientID"),
            F.col("PFIC.TaxPeriodID"),
        )
        .distinct()
    )

    # -----------------------------------------------------------------------
    # Step 3: PIVOT to get #QEFDetails
    # -----------------------------------------------------------------------
    pivot_src = (
        temp_pfic_input
        .filter(F.lower(F.col("ShortName")).isin("isqefelectionmade", "typeofpfic", "typeofforeigncorp", "investment"))
        .select("PFICFootnoteID", "ShortName", "TextValue")
    )

    qef_details = (
        pivot_src
        .groupBy("PFICFootnoteID")
        .pivot("ShortName", ["IsQEFElectionMade", "TypeOfPFIC", "TypeOfForeignCorp", "Investment"])
        .agg(F.max("TextValue"))
        .withColumnRenamed("Investment", "PFICEntityID")
    )

    # -----------------------------------------------------------------------
    # Step 4: Build #PFICInput with QEF election determination
    # -----------------------------------------------------------------------
    pfic_input = (
        temp_pfic_input.alias("PFIC")
        .join(qef_details.alias("QEF"), "PFICFootnoteID")
        .filter(
            F.col("PFIC.LineID").isin(pfic_6a, pfic_7a, pfic_7a_lt, pfic_investment_line_id, pfic_10c_lt, pfic_txt11, pfic_8b)
        )
        .select(
            F.col("PFIC.EntityID"),
            F.col("QEF.PFICEntityID"),
            F.col("PFIC.PFICFootnoteID"),
            F.col("PFIC.LineID"),
            # PBI 381344: zero out 8b distributions unless Is1293EligibleNoDeemed
            F.when(
                (F.col("PFIC.LineID") == pfic_8b) &
                (F.upper(F.coalesce(F.col("QEF.TypeOfPFIC"), F.lit(""))) != "IS1293ELIGIBLENODEEMED"),
                F.lit(0.0),
            ).otherwise(F.col("PFIC.Amount")).alias("Amount"),
            F.col("PFIC.TextValue"),
            # IsQEFElectionMade logic
            F.when(
                (F.lower(F.col("QEF.IsQEFElectionMade")) == "true") |
                (F.upper(F.col("QEF.TypeOfPFIC")).isin("IS1293ELIGIBLENODEEMED", "IS1293ELIGIBLEDEEMED")) |
                (
                    F.coalesce(F.col("QEF.TypeOfPFIC"), F.lit("")).cast("string").eqNullSafe("") &
                    (F.lower(F.coalesce(F.col("QEF.IsQEFElectionMade"), F.lit("false"))) == "false") &
                    ~F.upper(F.col("QEF.TypeOfForeignCorp")).isin("FOREIGN CORPORATION", "CONTROLLED FOREIGN CORPORATION")
                ),
                F.lit("Yes"),
            ).otherwise(F.lit("No")).alias("IsQEFElectionMade"),
            F.col("QEF.TypeOfPFIC").alias("TypeOfPfic"),
            F.col("PFIC.SourceEntityID"),
            F.col("PFIC.ClientID"),
            F.col("PFIC.TaxPeriodID"),
        )
    )

    # -----------------------------------------------------------------------
    # Step 5: Build #tmpPFICFootnoteInputData (QEF-elected lines)
    # -----------------------------------------------------------------------
    # Lines 6a, 7a, 7aLongterm where IsQEFElectionMade = 'Yes'
    qef_lines = (
        pfic_input
        .filter(
            (F.lower(F.col("IsQEFElectionMade")) == "yes") &
            F.col("LineID").isin(pfic_6a, pfic_7a, pfic_7a_lt, pfic_8b)
        )
        .select("EntityID", "PFICFootnoteID", "PFICEntityID", "LineID", "Amount", "TextValue", "SourceEntityID", "ClientID", "TaxPeriodID")
        .distinct()
    )

    # Lines 10cLongterm for ISMToM type — cap negative at pfictxt11Held amount
    pfic_10c_data = pfic_input.filter(
        (F.col("LineID") == pfic_10c_lt) &
        (F.lower(F.col("TypeOfPfic")) == "ismtom")
    )
    pfic_11_data = pfic_input.filter(F.col("LineID") == pfic_txt11).select(
        F.col("EntityID").alias("E11"),
        F.col("SourceEntityID").alias("S11"),
        F.col("PFICFootnoteID").alias("FN11"),
        F.col("Amount").alias("Amount11"),
    )

    p10c_joined = (
        pfic_10c_data.alias("P10c")
        .join(
            pfic_11_data.alias("P11"),
            (F.col("P10c.EntityID") == F.col("P11.E11")) &
            (F.col("P10c.SourceEntityID") == F.col("P11.S11")) &
            (F.col("P10c.PFICFootnoteID") == F.col("P11.FN11")),
        )
        .select(
            F.col("P10c.EntityID"),
            F.col("P10c.PFICFootnoteID"),
            F.col("P10c.PFICEntityID"),
            F.col("P10c.LineID"),
            F.when(
                F.col("P10c.Amount") >= 0, F.col("P10c.Amount")
            ).otherwise(
                F.when(
                    F.abs(F.col("P10c.Amount")) > F.abs(F.col("P11.Amount11")),
                    -F.abs(F.col("P11.Amount11")),
                ).otherwise(F.col("P10c.Amount"))
            ).alias("Amount"),
            F.col("P10c.TextValue"),
            F.col("P10c.SourceEntityID"),
            F.col("P10c.ClientID"),
            F.col("P10c.TaxPeriodID"),
        )
    )

    tmp_pfic_footnote_input = qef_lines.unionByName(p10c_joined, allowMissingColumns=True)

    # -----------------------------------------------------------------------
    # Step 6: Build #ConvertedPFICAmounts (for income attribute calc)
    # -----------------------------------------------------------------------
    pfic_entity_tbl = tbl(spark, "PFICFootnoteEntity", cfg)
    entity_tbl = tbl(spark, "Entity", cfg)
    country_list = tbl(spark, "ENU_CountryListImports", cfg)

    converted_pfic_amounts = (
        tmp_pfic_footnote_input.alias("PFIC")
        .join(
            pfic_mapped_df.alias("PK"),
            F.col("PFIC.LineID") == F.col("PK.PFICLineID"),
        )
        .join(
            pfic_entity_tbl.alias("PE"),
            F.col("PE.EntityId") == F.col("PFIC.PFICEntityID").cast("int"),
        )
        .join(
            entity_tbl.alias("E"),
            F.col("E.EntityID") == F.col("PFIC.SourceEntityID"),
        )
        .join(
            country_list.alias("EC"),
            F.col("EC.CountryCode") == F.coalesce(F.col("PE.CountryCode"), F.col("E.CountryCode")),
        )
        .select(
            F.col("PFIC.EntityID"),
            F.col("PK.K1LineID").alias("LineID"),
            F.col("PFIC.Amount"),
            F.coalesce(F.col("PE.CountryCode"), F.col("E.CountryCode")).alias("CountryCode"),
            F.col("EC.CountryID").alias("AttributeID"),
            F.col("PFIC.EntityID").cast("string").alias("TrackingKey"),
        )
    )

    # -----------------------------------------------------------------------
    # Step 7: Build election tables for PFIC classification
    # -----------------------------------------------------------------------
    pfic_flowup_base = (
        tbl(spark, "PFICFootnoteFlowupWithTrackingKey", cfg)
        .filter(
            (F.col("RunID") == run_id) &
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id) &
            (F.col("FlowupEntityID") != entity_id)
        )
    )

    # QFCandCFCPFICs — FC or CFC type
    qfc_cfc_pfics = (
        pfic_flowup_base
        .filter(
            (F.col("LineID") == type_of_foreign_corp_line_id) &
            F.upper(F.col("TextValue")).isin("FOREIGN CORPORATION", "CONTROLLED FOREIGN CORPORATION")
        )
        .select("FlowupEntityID", "PFICFootnoteID", "TrackingKey")
        .distinct()
    )

    # 1293 Elections
    elections_1293 = (
        pfic_flowup_base
        .filter(
            (F.col("LineID") == type_of_pfic_line_id) &
            F.upper(F.col("TextValue")).isin("IS1293ELIGIBLENODEEMED", "IS1293ELIGIBLEDEEMED")
        )
        .select("FlowupEntityID", "PFICFootnoteID", "TrackingKey")
        .distinct()
    )

    # 1291 Elections (union with QFC/CFC)
    elections_1291 = (
        pfic_flowup_base
        .filter(
            (F.col("LineID") == type_of_pfic_line_id) &
            F.upper(F.col("TextValue")).isin("IS1291NODISTRIBUTION", "IS1291ANYDISTRIBUTION")
        )
        .select("FlowupEntityID", "PFICFootnoteID", "TrackingKey")
        .distinct()
        .unionByName(qfc_cfc_pfics, allowMissingColumns=True)
    )

    # 1296 Elections (ISMToM)
    elections_1296 = (
        pfic_flowup_base
        .filter(
            (F.col("LineID") == type_of_pfic_line_id) &
            (F.lower(F.col("TextValue")) == "ismtom")
        )
        .select("FlowupEntityID", "PFICFootnoteID", "TrackingKey")
        .distinct()
    )

    # -----------------------------------------------------------------------
    # Step 8: Build #Non1291and1296Elections based on classification
    # -----------------------------------------------------------------------
    if pfic_classification == "U":
        # Non1291and1296 = 1293 minus (1291 + 1296)
        non_1291_1296 = (
            pfic_flowup_base.alias("PF")
            .join(
                elections_1293.alias("E1293"),
                (F.col("E1293.PFICFootnoteID") == F.col("PF.PFICFootnoteID")) &
                (F.col("E1293.TrackingKey") == F.col("PF.TrackingKey")) &
                (F.col("PF.FlowupEntityID") == F.col("E1293.FlowupEntityID")),
            )
            .select(F.col("PF.FlowupEntityID"), F.col("PF.PFICFootnoteID"), F.col("PF.TrackingKey"))
            .distinct()
            .unionByName(
                pfic_flowup_base.alias("PF2")
                .join(elections_1291.alias("E1291"),
                      (F.col("E1291.PFICFootnoteID") == F.col("PF2.PFICFootnoteID")) &
                      (F.col("E1291.TrackingKey") == F.col("PF2.TrackingKey")) &
                      (F.col("PF2.FlowupEntityID") == F.col("E1291.FlowupEntityID")),
                      "left_anti")
                .join(elections_1296.alias("E1296"),
                      (F.col("E1296.PFICFootnoteID") == F.col("PF2.PFICFootnoteID")) &
                      (F.col("E1296.TrackingKey") == F.col("PF2.TrackingKey")) &
                      (F.col("PF2.FlowupEntityID") == F.col("E1296.FlowupEntityID")),
                      "left_anti")
                .select(F.col("PF2.FlowupEntityID"), F.col("PF2.PFICFootnoteID"), F.col("PF2.TrackingKey"))
                .distinct()
            )
        )
    else:
        # Classification 'C': QEFElections first, then Non1291and1296 = 1293 minus (QEF + SubpartF)
        qef_elections = (
            pfic_flowup_base
            .filter(
                (F.col("LineID") == qef_election_line_id) &
                (F.lower(F.col("TextValue")) == "true")
            )
            .select("FlowupEntityID", "PFICFootnoteID", "TrackingKey")
            .distinct()
        )
        if qef_elected_df is not None:
            qef_elections = qef_elections.unionByName(
                qef_elected_df.select("FlowUpEntityID", "PFICFootnoteID", "TrackingKey")
                .withColumnRenamed("FlowUpEntityID", "FlowupEntityID")
            , allowMissingColumns=True)

        non_1291_1296 = (
            pfic_flowup_base.alias("PF")
            .join(
                elections_1293.alias("E1293"),
                (F.col("E1293.PFICFootnoteID") == F.col("PF.PFICFootnoteID")) &
                (F.col("E1293.TrackingKey") == F.col("PF.TrackingKey")) &
                (F.col("PF.FlowupEntityID") == F.col("E1293.FlowupEntityID")),
            )
            .join(
                qef_elections.alias("QEF"),
                (F.col("QEF.PFICFootnoteID") == F.col("PF.PFICFootnoteID")) &
                (F.col("QEF.TrackingKey") == F.col("PF.TrackingKey")) &
                (F.col("PF.FlowupEntityID") == F.col("QEF.FlowupEntityID")),
                "left_anti",
            )
            .join(
                subpartf_elected_df.alias("S"),
                (F.col("S.PFICFootnoteID") == F.col("PF.PFICFootnoteID")) &
                (F.col("S.TrackingKey") == F.col("PF.TrackingKey")) &
                (F.col("S.FlowUpEntityID") == F.col("PF.FlowupEntityID")),
                "left_anti",
            )
            .select(F.col("PF.FlowupEntityID"), F.col("PF.PFICFootnoteID"), F.col("PF.TrackingKey"))
            .distinct()
        )

    # -----------------------------------------------------------------------
    # Step 9: Build TempLookThroughAllocationInputUnGrouped
    # Join unblocked PFIC data with packages and elections
    # -----------------------------------------------------------------------
    pfic_package = tbl(spark, "PFICFootnotePackage", cfg)
    k1_package = tbl(spark, "K1Package", cfg)

    def _build_ungrouped(unblocked_df, election_df, pfic_lines, pfic_mapped_lines, is_subpart=False):
        """Build ungrouped allocation input from unblocked PFIC data for a given election."""
        join_mapped = (
            unblocked_df.alias("PFIC")
            .join(pfic_package.alias("P"), F.col("P.PFICFootnoteID") == F.col("PFIC.FootnoteID"))
            .join(k1_package.alias("K"), F.col("K.K1PackageID") == F.col("P.K1PackageID"))
            .join(
                pfic_mapped_lines.alias("PK"),
                (F.col("PFIC.LineID") == F.col("PK.PFICLineID")) &
                (F.when(F.col("PK.PFICID") != -1, F.col("PFIC.FootnoteID")).otherwise(F.lit(-1)) == F.col("PK.PFICID")) &
                (F.lit(True) if not is_subpart else (F.col("PK.IsSubpartLine") == True)),
            )
            .join(
                election_df.alias("ELEC"),
                (F.col("ELEC.PFICFootnoteID") == F.col("PFIC.FootnoteID")) &
                (F.col("ELEC.TrackingKey") == F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))) &
                (F.col("PFIC.LTEntityID") == F.col("ELEC.FlowupEntityID")),
            )
        )
        # PBI 381344: LEFT JOIN #PFICTypes to conditionally zero 8b distributions
        if pfic_types_df is not None:
            join_mapped = (
                join_mapped
                .join(
                    pfic_types_df.alias("PT"),
                    (F.col("PT.PFICFootnoteID") == F.col("PFIC.FootnoteID")) &
                    (F.col("PT.TrackingKey") == F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))) &
                    (F.col("PFIC.LTEntityID") == F.col("PT.FlowUpEntityID")),
                    "left",
                )
            )
        join_mapped = (
            join_mapped
            .filter(
                (F.col("PFIC.ClientID") == client_id) &
                (F.col("PFIC.TaxPeriodID") == tax_period_id) &
                (F.col("PFIC.RunID") == run_id) &
                (F.col("PFIC.LineTypeID") == pfic_footnote_lt) &
                F.col("PFIC.LineID").isin(*pfic_lines)
            )
            .select(
                F.col("K.LowerTierEntityID").alias("EntityID"),
                F.lit(k1_lt).alias("LineTypeID"),
                F.col("PK.K1LineID").alias("LineID"),
                # PBI 381344: zero out 8b unless Is1293EligibleNoDeemed
                F.when(
                    (F.col("PFIC.LineID") == pfic_8b) &
                    (F.upper(F.coalesce(F.col("PT.TypeOfPFIC"), F.lit(""))) != "IS1293ELIGIBLENODEEMED"),
                    F.lit(0.0),
                ).otherwise(F.col("PFIC.FlowupAmount")).alias("Amount") if pfic_types_df is not None else F.col("PFIC.FlowupAmount").alias("Amount"),
                F.when(
                    F.col("PFIC.LTEntityID") == F.col("K.LowerTierEntityID"), F.lit(entity_id)
                ).otherwise(
                    F.coalesce(F.nullif(F.col("PFIC.ParentEntityId"), F.lit(0)), F.col("PFIC.LTEntityID"))
                ).alias("ParentEntityID"),
                F.when(
                    (F.col("PFIC.LTEntityID") == F.col("K.LowerTierEntityID")) |
                    (F.coalesce(F.col("PFIC.ParentEntityId"), F.lit(0)) == 0),
                    F.lit(0),
                ).otherwise(F.col("PFIC.LTEntityID")).alias("SuperParentEntityID"),
                F.col("PFIC.TrackingKey"),
                F.coalesce(F.col("PFIC.Tag"), F.lit("")).alias("TAG"),
                F.coalesce(F.col("PFIC.OriginalParentEntityID"), F.lit(entity_id)).alias("OriginalParentEntityID"),
                F.col("PFIC.LTEntityID"),
                F.col("PFIC.FootnoteID").alias("PFICFootnoteID"),
            )
        )
        return join_mapped

    pfic_std_lines = [pfic_6a, pfic_7a, pfic_7a_lt, pfic_8b]
    ungrouped_parts = []

    # If classification 'C': also include QEF and SubpartF elected
    if pfic_classification == "C":
        if qef_elected_df is not None:
            ungrouped_qef = _build_ungrouped(
                reclass_unblocked_df, qef_elected_df, pfic_std_lines, pfic_mapped_df, is_subpart=False
            )
            ungrouped_parts.append(ungrouped_qef)

        if subpartf_elected_df is not None:
            ungrouped_subpart = _build_ungrouped(
                reclass_unblocked_df, subpartf_elected_df, pfic_std_lines, pfic_mapped_df, is_subpart=True
            )
            ungrouped_parts.append(ungrouped_subpart)

    # Non1291and1296 elections for standard lines
    ungrouped_non = _build_ungrouped(
        reclass_unblocked_df, non_1291_1296, pfic_std_lines, pfic_mapped_df, is_subpart=False
    )
    ungrouped_parts.append(ungrouped_non)

    # -----------------------------------------------------------------------
    # Step 10: 1296 Elections — PFIC10cLongterm with pfictxt11Held cap
    # -----------------------------------------------------------------------
    pfic_txt11_held = (
        reclass_unblocked_df.alias("PFIC")
        .join(
            elections_1296.alias("E1296"),
            (F.col("E1296.PFICFootnoteID") == F.col("PFIC.FootnoteID")) &
            (F.col("E1296.TrackingKey") == F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))) &
            (F.col("PFIC.LTEntityID") == F.col("E1296.FlowupEntityID")),
        )
        .filter(F.col("PFIC.LineID") == pfic_txt11)
        .select(
            F.col("PFIC.FootnoteID"),
            F.col("PFIC.FlowupAmount"),
            F.col("PFIC.LTEntityID"),
            F.col("PFIC.ParentEntityId"),
            F.col("PFIC.TrackingKey"),
            F.col("PFIC.OriginalParentEntityID"),
            F.col("PFIC.Tag"),
        )
    )

    ungrouped_1296 = (
        reclass_unblocked_df.alias("PFIC")
        .join(pfic_package.alias("P"), F.col("P.PFICFootnoteID") == F.col("PFIC.FootnoteID"))
        .join(k1_package.alias("K"), F.col("K.K1PackageID") == F.col("P.K1PackageID"))
        .join(
            pfic_mapped_df.alias("PK"),
            (F.col("PFIC.LineID") == F.col("PK.PFICLineID")) &
            (F.when(F.col("PK.PFICID") != -1, F.col("PFIC.FootnoteID")).otherwise(F.lit(-1)) == F.col("PK.PFICID")),
        )
        .join(
            elections_1296.alias("E1296"),
            (F.col("E1296.PFICFootnoteID") == F.col("PFIC.FootnoteID")) &
            (F.col("E1296.TrackingKey") == F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(str(entity_id)))) &
            (F.col("PFIC.LTEntityID") == F.col("E1296.FlowupEntityID")),
        )
        .join(
            pfic_txt11_held.alias("PTH"),
            (F.col("PTH.FootnoteID") == F.col("PFIC.FootnoteID")) &
            (F.col("PTH.TrackingKey") == F.col("PFIC.TrackingKey")) &
            (F.col("PTH.ParentEntityId") == F.col("PFIC.ParentEntityId")) &
            (F.col("PTH.LTEntityID") == F.col("PFIC.LTEntityID")) &
            (F.coalesce(F.col("PTH.OriginalParentEntityID"), F.lit(0)) == F.coalesce(F.col("PFIC.OriginalParentEntityID"), F.lit(0))) &
            (F.coalesce(F.col("PTH.Tag"), F.lit("")) == F.coalesce(F.col("PFIC.Tag"), F.lit(""))),
            "left",
        )
        .filter(F.col("PFIC.LineID") == pfic_10c_lt)
        .select(
            F.col("K.LowerTierEntityID").alias("EntityID"),
            F.lit(k1_lt).alias("LineTypeID"),
            F.col("PK.K1LineID").alias("LineID"),
            # Amount capped by pfictxt11Held
            F.when(
                F.col("PFIC.FlowupAmount") >= 0, F.col("PFIC.FlowupAmount")
            ).otherwise(
                F.when(
                    F.abs(F.coalesce(F.col("PFIC.FlowupAmount"), F.lit(0))) > F.abs(F.coalesce(F.col("PTH.FlowupAmount"), F.lit(0))),
                    -F.abs(F.coalesce(F.col("PTH.FlowupAmount"), F.lit(0))),
                ).otherwise(F.col("PFIC.FlowupAmount"))
            ).alias("Amount"),
            F.when(
                F.col("PFIC.LTEntityID") == F.col("K.LowerTierEntityID"), F.lit(entity_id)
            ).otherwise(
                F.coalesce(F.nullif(F.col("PFIC.ParentEntityId"), F.lit(0)), F.col("PFIC.LTEntityID"))
            ).alias("ParentEntityID"),
            F.when(
                (F.col("PFIC.LTEntityID") == F.col("K.LowerTierEntityID")) |
                (F.coalesce(F.col("PFIC.ParentEntityId"), F.lit(0)) == 0),
                F.lit(0),
            ).otherwise(F.col("PFIC.LTEntityID")).alias("SuperParentEntityID"),
            F.col("PFIC.TrackingKey"),
            F.coalesce(F.col("PFIC.Tag"), F.lit("")).alias("TAG"),
            F.coalesce(F.col("PFIC.OriginalParentEntityID"), F.lit(entity_id)).alias("OriginalParentEntityID"),
            F.col("PFIC.LTEntityID"),
            F.col("PFIC.FootnoteID").alias("PFICFootnoteID"),
        )
    )
    ungrouped_parts.append(ungrouped_1296)

    # -----------------------------------------------------------------------
    # Step 11: Union all ungrouped, group, and produce alloc input
    # -----------------------------------------------------------------------
    from functools import reduce
    all_ungrouped = reduce(DataFrame.unionByName, ungrouped_parts)

    # Grouped allocation input (sum Amount by keys)
    group_keys = ["EntityID", "LineTypeID", "LineID", "ParentEntityID", "SuperParentEntityID",
                  "TrackingKey", "TAG", "OriginalParentEntityID", "LTEntityID"]
    pfic_alloc_input = (
        all_ungrouped
        .groupBy(*group_keys)
        .agg(F.sum("Amount").alias("Amount"))
    )

    # Also get converted amounts by footnote for income attribute calc
    converted_from_ungrouped = (
        all_ungrouped.alias("T")
        .join(
            pfic_footnote_entity_details_df.alias("PE"),
            F.col("T.PFICFootnoteID") == F.col("PE.PFICFootnoteID"),
        )
        .groupBy(
            F.col("T.EntityID"), F.col("T.LineID"),
            F.col("PE.CountryCode"), F.col("PE.AttributeID"),
            F.col("T.TrackingKey"),
        )
        .agg(F.sum("Amount").alias("Amount"))
    )

    # Union with the direct-conversion amounts
    all_converted = converted_pfic_amounts.unionByName(converted_from_ungrouped, allowMissingColumns=True)

    log_timing("build_pfic_conversion", t0)
    return pfic_alloc_input, all_converted


# ---------------------------------------------------------------------------
# Section 14: build_pfic_income_attributes
# SQL lines ~2870-2920
# Recalculates PFICtoK1IncomeAttributePercentages from converted + flowup amounts.
# ---------------------------------------------------------------------------

def build_pfic_income_attributes(
    spark: SparkSession,
    cfg: dict,
    converted_pfic_amounts_df: DataFrame,
    lt_amounts_df: DataFrame,
    pfic_mapped_df: DataFrame,
    lower_tier_funds_df: DataFrame,
) -> DataFrame:
    """
    Recalculate income attribute percentages from PFIC converted amounts
    and flow-up amounts, then return DataFrame for writing to PFICtoK1IncomeAttributePercentages.

    Returns None if foreign entity or no data.
    """
    log_section("build_pfic_income_attributes")
    t0 = time.time()

    if cfg["is_foreign_entity"]:
        log_timing("build_pfic_income_attributes", t0)
        return None

    if converted_pfic_amounts_df is None:
        log_timing("build_pfic_income_attributes", t0)
        return None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    # Get AttributeTypeID for 'Country'
    attr_type_id = (
        tbl(spark, "ENU_IncomeAttributeType", cfg)
        .filter(F.lower(F.col("AttributeType")) == "country")
        .select("ID")
        .first()
    )
    attribute_type_id = attr_type_id["ID"] if attr_type_id else 0

    # FlowUp PFIC Amounts: from LT amounts joined with PFICMappedK1Lines and existing percentages.
    
    lt_run_ids = [r["RunID"] for r in lower_tier_funds_df.select("RunID").distinct().collect()
                  if r["RunID"] is not None]

    if lt_run_ids:
        existing_pct = (
            tbl(spark, "PFICtoK1IncomeAttributePercentages", cfg)
            .filter(F.col("RunID").isin(lt_run_ids))
        )

        flowup_pfic_amounts = (
            lt_amounts_df.alias("LI")
            .join(pfic_mapped_df.alias("PK"), F.col("LI.LineID") == F.col("PK.K1LineID"))
            .join(
                existing_pct.alias("IA"),
                (F.col("LI.LineID") == F.col("IA.LineID")) &
                (F.col("LI.TrackingKey") == F.col("IA.TrackingKey")),
            )
            .groupBy(
                F.col("LI.EntityID"), F.col("LI.LineID"),
                F.col("IA.CountryCode"), F.col("IA.AttributeID"),
                F.col("LI.TrackingKey"),
            )
            .agg(F.sum(F.col("IA.EffPercentage") * F.col("LI.FlowupAmount")).alias("Amount"))
        )

        # Combine converted + flowup
        recalc_amounts = converted_pfic_amounts_df.unionByName(flowup_pfic_amounts, allowMissingColumns=True)
    else:
        logger.info("No lower-tier funds for this run — skipping PFIC flow-up read.")
        recalc_amounts = converted_pfic_amounts_df

    # Group
    recalc_grouped = (
        recalc_amounts
        .groupBy("EntityID", "LineID", "CountryCode", "AttributeID", "TrackingKey")
        .agg(F.sum("Amount").alias("Amount"))
    )

    # Total amounts per entity/line/trackingkey
    total_amounts = (
        recalc_grouped
        .groupBy("EntityID", "LineID", "TrackingKey")
        .agg(F.sum("Amount").alias("TotalAmount"))
        .filter(F.coalesce(F.col("TotalAmount"), F.lit(0)) != 0)
    )

    # Calculate EffPercentage = Amount / TotalAmount
    result = (
        recalc_grouped.alias("RA")
        .join(
            total_amounts.alias("TA"),
            (F.col("RA.EntityID") == F.col("TA.EntityID")) &
            (F.col("RA.LineID") == F.col("TA.LineID")) &
            (F.col("RA.TrackingKey") == F.col("TA.TrackingKey")),
        )
        .select(
            F.lit(run_id).alias("RunID"),
            F.lit(entity_id).alias("EntityID"),
            F.col("RA.LineID"),
            F.col("RA.Amount"),
            (F.col("RA.Amount") / F.col("TA.TotalAmount")).alias("EffPercentage"),
            F.col("RA.CountryCode"),
            F.col("RA.AttributeID"),
            F.lit(attribute_type_id).alias("AttributeTypeID"),
            F.lit(client_id).alias("ClientID"),
            F.lit(tax_period_id).alias("TaxPeriodID"),
            F.concat(F.col("RA.TrackingKey"), F.lit("~"), F.lit(str(entity_id))).alias("TrackingKey"),
            F.col("RA.EntityID").alias("SourceEntityID"),
        )
    )

    log_timing("build_pfic_income_attributes", t0)
    return result
