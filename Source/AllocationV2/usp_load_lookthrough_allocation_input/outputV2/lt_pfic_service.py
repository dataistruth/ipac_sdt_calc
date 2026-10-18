"""
pfic_service.py — Sections 11-12: PFIC elections and mapped K1 lines setup.

Functions:
    build_pfic_elections    — Section 11: QEF/SubpartF elected PFICs + footnote details
    build_pfic_mapped_lines — Section 12: Distribution lines, PFICMappedK1Lines, FCC blocked/unblocked
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import time

try:
    from .lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger
except ImportError:
    from lt_helpers import tbl, tbl_name, ns, ns0, log_section, log_timing, logger


# ---------------------------------------------------------------------------
# Section 11: build_pfic_elections
# SQL lines ~1830-1933
# Only for domestic entities (IsForeignEntity = 0)
# Populates: QEFElectedPFIC, SubpartFElectedPFIC, PFICFootnoteEntityDetails
# ---------------------------------------------------------------------------

def build_pfic_elections(spark: SparkSession, cfg: dict) -> dict:
    """
    Build PFIC election DataFrames for domestic entities.
    Returns dict with keys: qef_elected, subpartf_elected, pfic_footnote_entity_details,
    distinct_lower_tier_funds.
    Returns empty dict if foreign entity.
    """
    log_section("build_pfic_elections")
    t0 = time.time()

    if cfg["is_foreign_entity"]:
        log_timing("build_pfic_elections", t0)
        return {
            "qef_elected": None,
            "subpartf_elected": None,
            "pfic_footnote_entity_details": None,
            "pfic_types": None,
        }

    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]
    pfic_investment_line_id = cfg["pfic_investment_line_id"]
    type_of_pfic_line_id = cfg["type_of_pfic_line_id"]

    # --- Staging: #TempPFICFootnoteFlowupWithTrackingKey ---
    # PBI 381344: cache the flowup rows for TypeOfPFIC + Investment lines
    temp_pfic_flowup = (
        tbl(spark, "PFICFootnoteFlowupWithTrackingKey", cfg)
        .filter(
            (F.col("RunID") == run_id) &
            F.col("LineID").isin(type_of_pfic_line_id, pfic_investment_line_id)
        )
        .select("RunID", "FlowupEntityID", "PFICFootnoteID", "TrackingKey",
                "LineID", "Amount", "TextValue")
    )

    # --- #PFICTypes ---
    pfic_types = (
        temp_pfic_flowup
        .filter(F.col("LineID") == type_of_pfic_line_id)
        .select(
            F.col("FlowupEntityID").alias("FlowUpEntityID"),
            F.col("PFICFootnoteID"),
            F.col("TrackingKey"),
            F.col("TextValue").alias("TypeOfPFIC"),
        )
        .distinct()
    )

    # Common join condition for PficForeignCorpClassificationInput + staging table
    fcc_input = tbl(spark, "PficForeignCorpClassificationInput", cfg).filter(
        F.col("SourceEntityID") == entity_id
    )
    # Use staging table (filtered to Investment line) for QEF
    pfic_flowup_staged = temp_pfic_flowup.filter(
        F.col("LineID") == pfic_investment_line_id
    )

    # Join condition with NULL-safe tracking key and footnote matching
    join_cond = (
        (fcc_input["EntityID"] == pfic_flowup_staged["TextValue"]) &
        (F.coalesce(fcc_input["TrackingKey"], F.lit("1")) ==
         F.coalesce(fcc_input["TrackingKey"], F.lit("1"))) &
        (F.coalesce(fcc_input["PFICFootnoteID"], F.lit(1)) ==
         F.coalesce(fcc_input["PFICFootnoteID"], F.lit(1)))
    )

    # Build base join (QEF uses staging table)
    base_join = fcc_input.join(pfic_flowup_staged, join_cond)

    # --- QEFElectedPFIC ---
    qef_elected = (
        base_join
        .filter(F.upper(F.col("FootnoteClassification")).isin("QEF", "DEEMEDELECTION"))
        .select(
            pfic_flowup_staged["FlowupEntityID"].alias("FlowUpEntityID"),
            pfic_flowup_staged["PFICFootnoteID"],
            pfic_flowup_staged["TrackingKey"],
        )
        .distinct()
    )

    # --- SubpartFElectedPFIC (uses base table per SQL) ---
    pfic_flowup_base = tbl(spark, "PFICFootnoteFlowupWithTrackingKey", cfg).filter(
        (F.col("RunID") == run_id) &
        (F.col("LineID") == pfic_investment_line_id)
    )
    subpartf_join_cond = (
        (fcc_input["EntityID"] == pfic_flowup_base["TextValue"]) &
        (F.coalesce(fcc_input["TrackingKey"], F.lit("1")) ==
         F.coalesce(fcc_input["TrackingKey"], F.lit("1"))) &
        (F.coalesce(fcc_input["PFICFootnoteID"], F.lit(1)) ==
         F.coalesce(fcc_input["PFICFootnoteID"], F.lit(1)))
    )
    subpartf_elected = (
        fcc_input.join(pfic_flowup_base, subpartf_join_cond)
        .filter(F.lower(F.col("FootnoteClassification")) == "subpartf")
        .select(
            pfic_flowup_base["FlowupEntityID"].alias("FlowUpEntityID"),
            pfic_flowup_base["PFICFootnoteID"],
            pfic_flowup_base["TrackingKey"],
        )
        .distinct()
    )

    # --- PFICFootnoteEntityDetails ---
    pfic_flowup_all = tbl(spark, "PFICFootNoteFlowUp", cfg).filter(
        (F.col("RunID") == run_id) &
        (F.col("LineID") == pfic_investment_line_id)
    )
    pfic_entity = F.broadcast(tbl(spark, "PFICFootnoteEntity", cfg))
    entity_tbl = F.broadcast(tbl(spark, "Entity", cfg))
    country_list = F.broadcast(tbl(spark, "ENU_CountryListImports", cfg))

    pfic_footnote_entity_details = (
        pfic_flowup_all
        .join(pfic_entity, pfic_flowup_all["TextValue"] == pfic_entity["EntityId"].cast("string"))
        .join(entity_tbl, entity_tbl["EntityID"] == pfic_flowup_all["SourceEntityID"])
        .join(
            country_list,
            country_list["CountryCode"] == F.coalesce(pfic_entity["CountryCode"], entity_tbl["CountryCode"]),
        )
        .select(
            pfic_flowup_all["PFICFootnoteID"],
            pfic_flowup_all["TextValue"].alias("EntityID"),
            country_list["CountryCode"],
            country_list["CountryID"].alias("AttributeID"),
        )
        .distinct()
    )

    log_timing("build_pfic_elections", t0)
    return {
        "qef_elected": qef_elected,
        "subpartf_elected": subpartf_elected,
        "pfic_footnote_entity_details": pfic_footnote_entity_details,
        "pfic_types": pfic_types,
    }


# ---------------------------------------------------------------------------
# Section 12: build_pfic_mapped_lines
# SQL lines ~1940-2130
# K1 distribution lines deletion, PFICMappedK1Lines, FCC blocked, unblocked data
# ---------------------------------------------------------------------------

def build_pfic_mapped_lines(spark: SparkSession, cfg: dict,
                            alloc_input_df: DataFrame) -> tuple:
    """
    Build PFIC mapped K1 lines, delete distribution lines from alloc_input,
    and build FCC blocked/unblocked data.
    Returns (updated_alloc_input_df, pfic_mapped_lines_df, fcc_blocked_df, reclass_unblocked_df).
    Returns (alloc_input_df, None, None, None) if foreign entity.
    """
    log_section("build_pfic_mapped_lines")
    t0 = time.time()

    if cfg["is_foreign_entity"]:
        log_timing("build_pfic_mapped_lines", t0)
        return alloc_input_df, None, None, None

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]
    k1_lt = cfg["k1_line_type_id"]
    pfic_footnote_lt = cfg["pfic_footnote_line_type_id"]
    pfic_investment_line_id = cfg["pfic_investment_line_id"]

    # --- K1 Distribution Lines ---
    # Inlined from dbo.udfGetDistributionLines
    k1_line = tbl(spark, "K1LineItem", cfg).filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id)
    )

    # Check if "Configure K1 Line Item" is enabled (GlobalMenu State='C')
    gm_check = (
        tbl(spark, "GlobalMenu", cfg).alias("GM")
        .join(
            tbl(spark, "ENU_GlobalMenuGroup", cfg).alias("EN"),
            (F.col("GM.GlobalMenuGroupID") == F.col("EN.GlobalMenuGroupID")) &
            (F.lower(F.col("EN.GroupName")) == "other configuration"),
            "inner",
        )
        .filter(
            (F.lower(F.col("GM.MenuName")) == "configure k1 line item") &
            (F.col("GM.ClientID") == client_id) &
            (F.col("GM.TaxPeriodID") == tax_period_id) &
            (F.upper(F.col("GM.State")) == "C")
        )
    )

    if gm_check.select(F.lit(1)).first() is not None:
        # Configured path: get 4th level contributing parent distribution lines
        contrib = tbl(spark, "ContributionLineWithAttributes", cfg)

        # Direct distribution lines (PFICClassType = 'PFIC Distribution', no offset)
        dist_lines = (
            contrib.alias("C")
            .join(
                k1_line.alias("K"),
                F.col("C.K1LineID") == F.col("K.LineID"),
                "inner",
            )
            .filter(
                (F.lower(F.col("K.PFICClassType")) == "pfic distribution") &
                F.col("C.Offset").isNull()
            )
            .select(F.col("K.LineID"))
        )

        # Get parent line attributes for those distribution lines
        parent_attrs = (
            contrib.alias("C2")
            .join(dist_lines.alias("D"), F.col("C2.K1LineID") == F.col("D.LineID"), "inner")
            .select(
                F.col("C2.ContributionLineID"),
                F.col("C2.ParentLineID"),
                F.col("C2.Source"),
                F.col("C2.Waterfall"),
                F.col("C2.TransactionDate"),
                F.col("C2.FN"),
                F.col("C2.ContributionLineClassification"),
            )
        )

        # Offset distribution lines matching parent attributes
        offset_lines = (
            parent_attrs.alias("D")
            .join(
                contrib.alias("C3"),
                (F.col("D.ContributionLineID") == F.col("C3.ContributionLineID")) &
                (F.coalesce(F.col("D.ParentLineID").cast("string"), F.lit("")) ==
                 F.coalesce(F.col("C3.ParentLineID").cast("string"), F.lit(""))) &
                (F.coalesce(F.col("D.Waterfall"), F.lit("")) ==
                 F.coalesce(F.col("C3.Waterfall"), F.lit(""))) &
                (F.coalesce(F.col("D.Source"), F.lit("")) ==
                 F.coalesce(F.col("C3.Source"), F.lit(""))) &
                (F.coalesce(F.col("D.TransactionDate"), F.lit("")) ==
                 F.coalesce(F.col("C3.TransactionDate"), F.lit(""))) &
                (F.coalesce(F.col("D.FN"), F.lit("")) ==
                 F.coalesce(F.col("C3.FN"), F.lit(""))) &
                (F.coalesce(F.col("D.ContributionLineClassification"), F.lit("")) ==
                 F.coalesce(F.col("C3.ContributionLineClassification"), F.lit(""))),
                "inner",
            )
            .filter(F.col("C3.Offset").isNotNull())
            .select(F.col("C3.K1LineID").alias("LineID"))
        )

        dist_lines = dist_lines.unionByName(offset_lines)
    else:
        # Simple path: just PFICClassType = 'PFIC Distribution'
        dist_lines = k1_line.filter(
            F.lower(F.col("PFICClassType")) == "pfic distribution"
        ).select("LineID")

    # Also add PFIC Dividend line (from original SP: SELECT LineID WHERE PFICClassType='PFIC Dividend')
    pfic_div_line = k1_line.filter(
        F.lower(F.col("PFICClassType")) == "pfic dividend"
    ).select("LineID")

    all_dist_lines = dist_lines.unionByName(pfic_div_line)

    # Delete distribution lines from alloc_input where LineTypeID = K1LineTypeID
    updated_alloc = alloc_input_df.join(
        all_dist_lines,
        (alloc_input_df["LineID"] == all_dist_lines["LineID"]) &
        (alloc_input_df["LineTypeID"] == k1_lt),
        "left_anti",
    )

    # --- PFICMappedK1Lines ---
    # Inlined from dbo.udfgetPFICMappedK1Lines
    pfic_mapped = _build_pfic_mapped_k1_lines(spark, cfg)

    # --- FCC Blocked ---
    fcc_blocked = tbl(spark, "PficForeignCorpClassificationInput", cfg).filter(
        (F.lower(F.col("FootnoteClassification")) == "blocked") &
        (F.col("SourceEntityID") == entity_id)
    ).select(
        "EntityID", "FlowupEntityID", "SourceEntityID",
        "FootnoteClassification", "PFICSourceEntityID", "TrackingKey", "PFICFootnoteID",
    ).distinct()

    # --- ReclassFootnoteAllocationData (Unblocked) ---
    # This is an output table written by the reclass SP — may not exist yet
    try:
        _test_exists = spark.table(tbl_name("ReclassFootnoteAllocationData", cfg)).limit(0)
        _test_exists.collect()  # Force evaluation to check existence
        reclass_fna = tbl(spark, "ReclassFootnoteAllocationData", cfg).filter(
            (F.col("RunID") == run_id) &
            (F.col("LineID") == pfic_investment_line_id) &
            (F.col("LineTypeID") == pfic_footnote_lt)
        )
        _reclass_exists = True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            logger.warning("ReclassFootnoteAllocationData not found — returning empty reclass_unblocked.")
            _reclass_exists = False
        else:
            raise

    if not _reclass_exists:
        log_timing("build_pfic_mapped_lines", t0)
        return updated_alloc, pfic_mapped, fcc_blocked, None

    # Left anti-join to exclude blocked footnotes
    # SQL: CASE WHEN Pfic_Blocked.PFICFootnoteID IS NULL THEN 1 ELSE Pfic_Blocked.PFICFootnoteID END =
    #      CASE WHEN Pfic_Blocked.PFICFootnoteID IS NULL THEN 1 ELSE PFIC.FootnoteID END
    reclass_unblocked_ids = (
        reclass_fna.alias("PFIC")
        .join(
            fcc_blocked.alias("B"),
            (F.coalesce(F.col("B.PFICSourceEntityID"), F.lit(0)) == F.coalesce(F.col("PFIC.SourceEntityID"), F.lit(0))) &
            (F.col("PFIC.TextValue") == F.col("B.EntityID").cast("string")) &
            (
                F.coalesce(F.col("B.PFICFootnoteID"), F.lit(1)) ==
                F.when(
                    F.col("B.PFICFootnoteID").isNull(), F.lit(1)
                ).otherwise(F.col("PFIC.FootnoteID"))
            ) &
            (
                F.coalesce(F.col("B.TrackingKey"), F.lit("1")) ==
                F.when(
                    F.col("B.TrackingKey").isNull(), F.lit("1")
                ).otherwise(
                    F.concat(F.col("PFIC.TrackingKey"), F.lit("~"), F.lit(entity_id).cast("string"))
                )
            ),
            "left_anti",
        )
        .select(
            F.col("PFIC.LTEntityID"),
            F.col("PFIC.SourceEntityID"),
            F.col("PFIC.FootnoteID"),
            F.col("PFIC.TrackingKey"),
        )
        .distinct()
    )

    # Full unblocked data — SQL reads ALL lines from ReclassFootnoteAllocationData (no LineID filter)
    reclass_all = tbl(spark, "ReclassFootnoteAllocationData", cfg).filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id) &
        (F.col("RunID") == run_id) &
        (F.col("LineTypeID") == pfic_footnote_lt)
    )
    reclass_unblocked = (
        reclass_all.alias("PFIC")
        .join(
            reclass_unblocked_ids.alias("UB"),
            (F.col("PFIC.FootnoteID") == F.col("UB.FootnoteID")) &
            (F.col("PFIC.TrackingKey") == F.col("UB.TrackingKey")) &
            (F.coalesce(F.col("PFIC.SourceEntityID"), F.lit(0)) == F.coalesce(F.col("UB.SourceEntityID"), F.lit(0))) &
            (F.coalesce(F.col("PFIC.LTEntityID"), F.lit(0)) == F.coalesce(F.col("UB.LTEntityID"), F.lit(0))),
        )
        .select(
            F.col("PFIC.RunID"),
            F.col("PFIC.ReclassWorkflowID"),
            F.col("PFIC.LowerTierRunID"),
            F.col("PFIC.ClientID"),
            F.col("PFIC.TaxPeriodID"),
            F.col("PFIC.EntityID"),
            F.col("PFIC.SourceEntityID"),
            F.col("PFIC.FootnoteID"),
            F.col("PFIC.LineTypeID"),
            F.col("PFIC.LineID"),
            F.col("PFIC.Amount"),
            F.col("PFIC.FlowupAmount"),
            F.col("PFIC.TextValue"),
            F.col("PFIC.ParentEntityID"),
            F.col("PFIC.LTEntityID"),
            F.col("PFIC.Tag"),
            F.col("PFIC.TrackingKey"),
            F.col("PFIC.OriginalParentEntityID"),
            F.col("PFIC.TransactionName"),
            F.col("PFIC.TransactionEntityID"),
            F.col("PFIC.Comments"),
            F.col("PFIC.SecIIComments"),
        )
        .distinct()
    )

    log_timing("build_pfic_mapped_lines", t0)
    return updated_alloc, pfic_mapped, fcc_blocked, reclass_unblocked


# ---------------------------------------------------------------------------
# Inlined UDF: dbo.udfgetPFICMappedK1Lines
# ---------------------------------------------------------------------------

def _build_pfic_mapped_k1_lines(spark: SparkSession, cfg: dict) -> "DataFrame":
    """
    Replicates dbo.udfgetPFICMappedK1Lines(@ClientID, @TaxPeriodID, @EntityID, @RunID).

    Two paths:
      1. If PFICK1Mapping table exists and has rows → use that mapping + quarters
      2. Otherwise → build mapping from K1LineItem.MappedToPFIC column
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    # PFIC line IDs (already loaded in config)
    pfic_6a = cfg["pfic_6a_line_id"]
    pfic_7a = cfg["pfic_7a_line_id"]
    pfic_7a_lt = cfg["pfic_7a_longterm_line_id"]
    pfic_10c_lt = cfg["pfic_10c_longterm_line_id"]

    # ReversalPYQEFInclusion line
    rev_row = tbl(spark, "PFICFootNoteLineItem", cfg).filter(
        (F.col("ClientID") == client_id) &
        (F.col("TaxPeriodID") == tax_period_id) &
        (F.upper(F.col("ShortName")) == "REVERSALPYQEFINCLUSION")
    ).select("LineID").first()
    pfic_reversal_qef = rev_row["LineID"] if rev_row else None

    # Check if PFICK1Mapping table exists and has rows
    pfick1_exists = False
    try:
        pfick1_df = tbl(spark, "PFICK1Mapping", cfg)
        if pfick1_df.select(F.lit(1)).first() is not None:
            pfick1_exists = True
    except Exception:
        pfick1_exists = False

    if pfick1_exists:
        # Path 1: Use PFICK1Mapping table
        pfick1_mapping = pfick1_df.select(
            F.col("PFICLineID"),
            F.col("K1LineID"),
            F.col("IsSubpartLine"),
            F.col("Quarter"),
        )

        if run_id != 0:
            # Get quarter line ID
            quarter_line_row = tbl(spark, "PFICFootNoteLineItem", cfg).filter(
                (F.upper(F.col("ShortName")) == "QUARTERALLOCATIONS") &
                (F.col("IsActive") == True)
            ).select("LineID").first()
            quarter_line_id = quarter_line_row["LineID"] if quarter_line_row else None

            # Get distinct PFICFootnoteIDs for this run
            pfic_quarters = (
                tbl(spark, "PFICFootNoteFlowUp", cfg)
                .filter(F.col("RunID") == run_id)
                .select(F.col("PFICFootnoteID").alias("PFICID"))
                .distinct()
                .withColumn("Quarter", F.lit("Q0"))
            )

            # Update quarters from PFICFootNoteFlowUp
            if quarter_line_id is not None:
                pfic_flowup_quarters = (
                    tbl(spark, "PFICFootNoteFlowUp", cfg)
                    .filter(
                        (F.col("RunID") == run_id) &
                        (F.col("LineID") == quarter_line_id) &
                        (F.coalesce(F.col("TextValue"), F.lit("")) != "")
                    )
                    .select(
                        F.col("PFICFootnoteID").alias("PFICID"),
                        F.col("TextValue").alias("QuarterValue"),
                    )
                )

                # Left join to update quarters
                pfic_quarters = (
                    pfic_quarters.alias("PQ")
                    .join(pfic_flowup_quarters.alias("PF"), "PFICID", "left")
                    .select(
                        F.col("PQ.PFICID"),
                        F.coalesce(F.col("PF.QuarterValue"), F.col("PQ.Quarter")).alias("Quarter"),
                    )
                )

            # Final join: PFICK1Mapping × PFICQuarters × K1LineItem
            k1_line_item = F.broadcast(
                tbl(spark, "K1LineItem", cfg).select(
                    F.col("LineID"), F.col("TransactionDate")
                )
            )

            pfic_mapped = (
                pfick1_mapping.alias("PK")
                .join(pfic_quarters.alias("PQ"), F.col("PK.Quarter") == F.col("PQ.Quarter"))
                .join(k1_line_item.alias("K"), F.col("PK.K1LineID") == F.col("K.LineID"))
                .select(
                    F.col("PQ.PFICID"),
                    F.col("PK.PFICLineID"),
                    F.col("PK.K1LineID"),
                    F.col("PK.IsSubpartLine"),
                    F.col("PQ.Quarter"),
                    F.col("K.TransactionDate"),
                )
            )
        else:
            # RunID == 0: return -1 as PFICID
            pfic_mapped = pfick1_mapping.select(
                F.lit(-1).alias("PFICID"),
                F.col("PFICLineID"),
                F.col("K1LineID"),
                F.col("IsSubpartLine"),
                F.col("Quarter"),
                F.lit(None).cast("timestamp").alias("TransactionDate"),
            )
    else:
        # Path 2: No PFICK1Mapping table — build from K1LineItem.MappedToPFIC
        k1li = tbl(spark, "K1LineItem", cfg).filter(
            (F.col("ClientID") == client_id) &
            (F.col("TaxPeriodID") == tax_period_id)
        )

        mappings = []
        if pfic_6a is not None:
            mappings.append(
                k1li.filter(F.upper(F.col("MappedToPFIC")) == "6A")
                .select(
                    F.lit(pfic_6a).alias("PFICLineID"),
                    F.col("LineID").alias("K1LineID"),
                    F.lit(False).alias("IsSubpartLine"),
                    F.col("TransactionDate"),
                )
            )
        if pfic_7a is not None:
            mappings.append(
                k1li.filter(F.upper(F.col("MappedToPFIC")) == "7A")
                .select(
                    F.lit(pfic_7a).alias("PFICLineID"),
                    F.col("LineID").alias("K1LineID"),
                    F.lit(False).alias("IsSubpartLine"),
                    F.col("TransactionDate"),
                )
            )
        if pfic_7a_lt is not None:
            mappings.append(
                k1li.filter(F.upper(F.col("MappedToPFIC")) == "7ALONGTERM")
                .select(
                    F.lit(pfic_7a_lt).alias("PFICLineID"),
                    F.col("LineID").alias("K1LineID"),
                    F.lit(False).alias("IsSubpartLine"),
                    F.col("TransactionDate"),
                )
            )
        # SubpartF → PFICLineID = 0, IsSubpartLine = True
        mappings.append(
            k1li.filter(F.upper(F.col("MappedToPFIC")) == "SUBPARTF")
            .select(
                F.lit(0).alias("PFICLineID"),
                F.col("LineID").alias("K1LineID"),
                F.lit(True).alias("IsSubpartLine"),
                F.col("TransactionDate"),
            )
        )
        if pfic_10c_lt is not None:
            mappings.append(
                k1li.filter(F.upper(F.col("MappedToPFIC")) == "10CLESS10A10B")
                .select(
                    F.lit(pfic_10c_lt).alias("PFICLineID"),
                    F.col("LineID").alias("K1LineID"),
                    F.lit(False).alias("IsSubpartLine"),
                    F.col("TransactionDate"),
                )
            )
        if pfic_reversal_qef is not None:
            mappings.append(
                k1li.filter(F.upper(F.col("MappedToPFIC")) == "REVERSALPYQEF")
                .select(
                    F.lit(pfic_reversal_qef).alias("PFICLineID"),
                    F.col("LineID").alias("K1LineID"),
                    F.lit(False).alias("IsSubpartLine"),
                    F.col("TransactionDate"),
                )
            )

        if mappings:
            from functools import reduce
            combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), mappings)
            pfic_mapped = combined.select(
                F.lit(-1).alias("PFICID"),
                F.col("PFICLineID"),
                F.col("K1LineID"),
                F.col("IsSubpartLine"),
                F.lit("Q0").alias("Quarter"),
                F.col("TransactionDate"),
            )
        else:
            pfic_mapped = spark.createDataFrame(
                [], "PFICID: int, PFICLineID: int, K1LineID: int, IsSubpartLine: boolean, Quarter: string, TransactionDate: timestamp"
            )

    return pfic_mapped
