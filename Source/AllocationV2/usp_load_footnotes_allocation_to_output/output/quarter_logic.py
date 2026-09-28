"""
Quarter resolution logic for uspLoadFootnotesAllocationToOutput.

Handles PartV quarter assignment, PFIC distribution date logic,
and form-specific quarter updates (Form8886, Form199A, Form926, Form8865).
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# update_pfic_partv_quarters
# SQL lines: 605–667
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def update_pfic_partv_quarters(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
    df_temp_final_eff_pct: DataFrame,
) -> tuple:
    """Assign quarters to PFIC lines based on PartV distribution dates.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 605-667.
    Row count: POSSIBLY-EMPTY — only applies when cfg['part_v_allocated'] == True.
    Conditional on: cfg['part_v_allocated'] == True.

    Returns:
        tuple: (df_alloc_input_updated, df_part_v_allocable_lines)
            - df_alloc_input_updated has Quarter updated for PartV lines
            - df_part_v_allocable_lines has (LineID, QuicklinkID) for downstream exclusion

    Joins:
    - #TempAllocationInput → PFICFootNoteFlowUP (RunID, PFICFootNoteID=QuicklinkID, LineID=@PFICDistributionDateLineID)
    - → QuarterDates (TextValue date = StartDate; SQL's BETWEEN StartDate/EndDate
      double-matches on the daily calendar)
    - → PFICFootnoteLineItem (LineID, IsAllocated=1, IsPartVAllocated=1)
    - → #TempFinalEffectivePercentage (Quarter match)

    ISNULL-keyed: TrackingKey (null-safe using ns(''))
    String comparisons: TextValue != 'Various' → F.lower
    """
    log_section("update_pfic_partv_quarters")
    t0 = time.time()

    if not cfg.get("part_v_allocated"):
        # SQL: IF(ISNULL(@PartVAllocated,0) = 1) — skip if not allocated
        logger.info("[SKIP] PartV not allocated — skipping PartV quarter logic")
        # Return empty PartV allocable lines
        from pyspark.sql.types import StructType, StructField, IntegerType
        schema = StructType([
            StructField("LineID", IntegerType()),
            StructField("QuicklinkID", IntegerType()),
        ])
        return df_alloc_input, spark.createDataFrame([], schema)

    run_id = cfg["run_id"]
    pfic_lt = cfg["pfic_footnote_line_type_id"]
    pfic_dist_line_id = cfg["pfic_distribution_date_line_id"]

    # ── Step 1: Build #PartVlinesQuarters ──
    # SELECT DISTINCT D.[Quarter], T.QuicklinkID, ISNULL(T.TrackingKey,'') TrackingKey
    # FROM #TempAllocationInput T
    # INNER JOIN PFICFootNoteFlowUP PF ON PF.RunID=@RunID AND PF.PFICFootNoteID=T.QuicklinkID
    #   AND PF.LineID=@PFICDistributionDateLineID AND ISNULL(PF.TextValue,'')!='Various'
    # INNER JOIN QuarterDates D ON Convert(date,ISNULL(PF.TextValue,'1900-01-01')) BETWEEN D.StartDate AND D.EndDate
    # WHERE T.LineTypeID = @PFICFootNoteLineTypeID

    pfic_flowup = (
        read_table(spark, "PFICFootNoteFlowUP", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineID") == pfic_dist_line_id)
            # BUG-07 FIX: SQL only checks ISNULL(PF.TextValue,'') != 'Various'
            # Empty strings pass through in SQL ('' != 'Various' is TRUE).
            # Removed the extra != "" filter that was excluding empty/null TextValue.
            & (F.lower(F.coalesce(F.col("TextValue"), F.lit(""))) != "various")
        )
        .select(
            F.col("PFICFootnoteID").alias("PFICFootNoteID"),
            F.col("TextValue"),
        )
    )

    quarter_dates = read_table(spark, "QuarterDates", cfg).select(
        F.col("Quarter"), F.col("StartDate")
    )

    # Filter #TempAllocationInput for PFIC lines only
    pfic_input = df_alloc_input.filter(F.col("LineTypeID") == pfic_lt)

    # Join: T → PFICFootNoteFlowUP
    joined = (
        pfic_input
        .join(
            pfic_flowup,
            pfic_input["QuicklinkID"] == pfic_flowup["PFICFootNoteID"],
            "inner",
        )
    )

    # BUG-08 FIX: SQL uses Convert(date, ISNULL(PF.TextValue, '1900-01-01'))
    # Use try_to_date (ANSI-safe: returns NULL on parse failure instead of throwing).
    joined = joined.withColumn(
        "_dist_date",
        F.coalesce(
            F.expr("try_to_date(coalesce(TextValue, '1900-01-01'), 'yyyy-MM-dd')"),
            F.expr("try_to_date(coalesce(TextValue, '1900-01-01'), 'MM/dd/yyyy')"),
            F.expr("try_to_date(split(coalesce(TextValue, '1900-01-01'), ' ')[0], 'M/d/yyyy')"),
        )
    )

    # BUG-01 FIX: Alias Quarter to _pv_quarter to avoid ambiguity with other
    # DataFrames that also have a Quarter column in downstream joins.
    part_v_quarters = (
        joined.join(
            quarter_dates,

            F.col("_dist_date") == F.to_date(F.col("StartDate")),
            "inner",
        )
        .select(
            quarter_dates["Quarter"].alias("_pv_quarter"),
            pfic_input["QuicklinkID"],
            ns(pfic_input["TrackingKey"], F.lit("")).alias("TrackingKey"),
        )
        .distinct()
    )

    # ── Step 2: Build #PartVAllocableLines ──
    # SELECT DISTINCT PF.LineID, AI.QuicklinkID FROM #TempAllocationInput AI
    # JOIN PFICFootnoteLineItem PF ON PF.LineID = AI.LineID
    # JOIN #PartVlinesQuarters V ON V.QuicklinkID = AI.QuicklinkID
    #   AND ISNULL(AI.TrackingKey,'') = ISNULL(V.TrackingKey,'')
    # JOIN #TempFinalEffectivePercentage TF ON TF.Quarter = V.Quarter
    # WHERE PF.IsAllocated = 1 AND IsPartVAllocated = 1

    pfic_line_items = (
        cfg["_df_pfic_footnote_line_item"]
        .filter(
            (F.col("IsAllocated") == True) & (F.col("IsPartVAllocated") == True)  # noqa: E712
        )
        .select(F.col("LineID").alias("pfli_LineID"))
    )

    part_v_allocable = (
        pfic_input
        .join(pfic_line_items, pfic_input["LineID"] == pfic_line_items["pfli_LineID"], "inner")
        .join(
            part_v_quarters,
            (pfic_input["QuicklinkID"] == part_v_quarters["QuicklinkID"])
            & (ns(pfic_input["TrackingKey"], F.lit("")) == part_v_quarters["TrackingKey"]),
            "inner",
        )
        .join(
            df_temp_final_eff_pct,
            df_temp_final_eff_pct["Quarter"] == part_v_quarters["_pv_quarter"],
            "inner",
        )
        .select(
            pfic_line_items["pfli_LineID"].alias("LineID"),
            pfic_input["QuicklinkID"],
        )
        .distinct()
    )

    # ── Step 3: UPDATE Quarter on #TempAllocationInput ──
    # UPDATE T SET [Quarter] = D.[Quarter]
    # FROM #TempAllocationInput T
    # INNER JOIN PFICFootNoteFlowUP PF ON PF.RunID=@RunID AND PF.PFICFootNoteID=T.QuicklinkID
    # INNER JOIN #PartVAllocableLines PL ON T.LINEID=PL.LINEID AND T.QuicklinkID=PL.QuicklinkID
    # INNER JOIN #PartVlinesQuarters D ON D.QuicklinkID=T.QuicklinkID
    #   AND ISNULL(D.TrackingKey,'')=ISNULL(T.TrackingKey,'')
    # JOIN #TempFinalEffectivePercentage TF ON TF.Quarter = D.Quarter
    # WHERE T.LineTypeID = @PFICFootNoteLineTypeID

    # Build the quarter values to assign
    update_source = (
        pfic_input
        .join(
            pfic_flowup,
            pfic_input["QuicklinkID"] == pfic_flowup["PFICFootNoteID"],
            "inner",
        )
        .join(
            part_v_allocable,
            (pfic_input["LineID"] == part_v_allocable["LineID"])
            & (pfic_input["QuicklinkID"] == part_v_allocable["QuicklinkID"]),
            "inner",
        )
        .join(
            part_v_quarters,
            (pfic_input["QuicklinkID"] == part_v_quarters["QuicklinkID"])
            & (ns(pfic_input["TrackingKey"], F.lit("")) == part_v_quarters["TrackingKey"]),
            "inner",
        )
        .join(
            df_temp_final_eff_pct,
            df_temp_final_eff_pct["Quarter"] == part_v_quarters["_pv_quarter"],
            "inner",
        )
        .select(
            pfic_input["QuicklinkID"].alias("_upd_ql"),
            pfic_input["LineID"].alias("_upd_lid"),
            ns(pfic_input["TrackingKey"], F.lit("")).alias("_upd_tk"),
            part_v_quarters["_pv_quarter"].alias("_new_quarter"),
        )
        .distinct()
    )

    # Apply update via left join: match on QuicklinkID + LineID + TrackingKey + LineTypeID
    df_alloc_input = df_alloc_input.alias("T")
    update_source = update_source.alias("U")

    df_result = (
        df_alloc_input.join(
            update_source,
            (F.col("T.QuicklinkID") == F.col("U._upd_ql"))
            & (F.col("T.LineID") == F.col("U._upd_lid"))
            & (ns(F.col("T.TrackingKey"), F.lit("")) == F.col("U._upd_tk"))
            & (F.col("T.LineTypeID") == pfic_lt),
            "left",
        )
        .withColumn(
            "Quarter",
            F.when(
                F.col("U._new_quarter").isNotNull(),
                F.col("U._new_quarter"),
            ).otherwise(F.col("T.Quarter")),
        )
        .select([F.col(f"T.{c}") if c != "Quarter" else F.col("Quarter")
                 for c in df_alloc_input.columns])
    )

    log_timing("update_pfic_partv_quarters", t0)
    return df_result, part_v_allocable


# ---------------------------------------------------------------------------
# update_pfic_quarters_by_config
# SQL lines: 683–742
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def update_pfic_quarters_by_config(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
    df_part_v_allocable: DataFrame,
    df_temp_final_eff_pct: DataFrame,
) -> DataFrame:
    """Assign quarters based on IsPFICAllocationbyQuarter config ('C' vs else).

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 683-742.
    Row count: POSSIBLY-EMPTY — conditional on IsPFICAllocationbyQuarter setting.

    When IsPFICAllocationbyQuarter = 'C':
      1. Build #DistributionQuarter from ENU_DF_DataList (month→quarter mapping)
      2. UPDATE using DefaultAllocationRule (Max Quarter, Q0, Distribution Date)
      3. UPDATE remaining using PFICQuarterLineID

    When IsPFICAllocationbyQuarter != 'C':
      1. Simple UPDATE using PFICQuarterLineID
    
    Both branches exclude PartVAllocableLines (LEFT JOIN + IS NULL pattern).
    """
    log_section("update_pfic_quarters_by_config")
    t0 = time.time()

    run_id = cfg["run_id"]
    pfic_lt = cfg["pfic_footnote_line_type_id"]
    pfic_dist_line_id = cfg["pfic_distribution_date_line_id"]
    pfic_quarter_line_id = cfg["pfic_quarter_line_id"]
    is_pfic_by_quarter = cfg.get("is_pfic_allocation_by_quarter")

    # Filter to PFIC lines only for the update source
    pfic_flowup = (
        read_table(spark, "PFICFootNoteFlowUP", cfg)
        .filter(F.col("RunID") == run_id)
    )

    if is_pfic_by_quarter == "C":
        # ── Branch: IsPFICAllocationbyQuarter = 'C' ──

        # Step 1: Build #DistributionQuarter
        # SELECT DISTINCT D.LOOKUPDATA, T.QuicklinkID, ISNULL(T.TrackingKey,'')
        # FROM #TempAllocationInput T
        # INNER JOIN PFICFootNoteFlowUP PF ON PF.RunID=@RunID AND PF.PFICFootNoteID=T.QuicklinkID
        #   AND PF.LineID=@PFICDistributionDateLineID
        # INNER JOIN ENU_DF_DataList D ON D.LookUpValue = ISNULL(Month(PF.TextValue),0)
        #   AND D.Category='QuarterMonth' AND ISNULL(PF.TextValue,'')!='Various'
        # WHERE T.LineTypeID = @PFICFootNoteLineTypeID

        pfic_dist = pfic_flowup.filter(
            (F.col("LineID") == pfic_dist_line_id)
            & (ns(F.col("TextValue"), "") != "")
            & (F.lower(ns(F.col("TextValue"), "")) != "various")
        )

        enu_df_datalist = (
            read_table(spark, "ENU_DF_DataList", cfg)
            .filter(F.lower(F.col("Category")) == "quartermonth")
            .select(F.col("LookUpData"), F.col("LookUpValue"))
        )

        pfic_input = df_alloc_input.filter(F.col("LineTypeID") == pfic_lt)

        dist_quarter = (
            pfic_input
            .join(
                pfic_dist,
                pfic_input["QuicklinkID"] == pfic_dist["PFICFootnoteID"],
                "inner",
            )
            # BUG-09 FIX: SQL uses ISNULL(Month(PF.TextValue), 0) as integer.
            # Use try_to_date (ANSI-safe: returns NULL on parse failure).
            .withColumn("_month", F.coalesce(
                F.month(F.coalesce(
                    F.expr("try_to_date(TextValue, 'yyyy-MM-dd')"),
                    F.expr("try_to_date(TextValue, 'MM/dd/yyyy')"),
                    F.expr("try_to_date(split(TextValue, ' ')[0], 'M/d/yyyy')"),
                )),
                F.lit(0),
            ))
            .join(
                enu_df_datalist,
                F.col("_month") == enu_df_datalist["LookUpValue"].cast("int"),
                "inner",
            )
            .select(
                enu_df_datalist["LookUpData"].alias("LOOKUPDATA"),
                pfic_input["QuicklinkID"],
                ns(pfic_input["TrackingKey"], F.lit("")).alias("TrackingKey"),
            )
            .distinct()
        )

        # Step 2: UPDATE with DefaultAllocationRule
        # INNER JOIN PFICFootNoteFlowUP PF ON PF.RunID=@RunID AND PF.PFICFootNoteID=T.QuicklinkID
        # INNER JOIN PFICFOOTNOTELINEITEM PL ON T.LINEID=PL.LINEID
        # LEFT JOIN #DistributionQuarter D ON D.QuicklinkID=T.QuicklinkID
        #   AND ISNULL(D.TrackingKey,'')=ISNULL(T.TrackingKey,'')
        # LEFT JOIN #PartVAllocableLines VL ON VL.LineID=PL.LineID AND T.QuicklinkID=VL.QuicklinkID
        # WHERE T.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(PL.DefaultAllocationRule,'')!=''
        #   AND VL.LineID IS NULL

        pfic_li = (
            cfg["_df_pfic_footnote_line_item"]
            .filter(ns(F.col("DefaultAllocationRule"), "") != "")
            .select(
                F.col("LineID").alias("pl_LineID"),
                F.lower(F.col("DefaultAllocationRule")).alias("dar_lower"),
            )
        )

        # BUG-11 FIX: SQL has INNER JOIN PFICFootNoteFlowUP as existence check.
        # Only update PFIC lines that have at least one FlowUP record for this RunID.
        pfic_existence = (
            pfic_flowup
            .select(F.col("PFICFootnoteID").alias("_ex_ql"))
            .distinct()
        )

        # Build update source for DefaultAllocationRule lines
        # CASE WHEN 'max quarter allocation' THEN 'Q4'
        #      WHEN 'q0 allocation' THEN 'Q0'
        #      WHEN 'distribution date allocation' THEN ISNULL(D.LOOKUPDATA, 'Q0')
        dar_update = (
            df_alloc_input.alias("T")
            .filter(F.col("T.LineTypeID") == pfic_lt)
            .join(pfic_existence, F.col("T.QuicklinkID") == pfic_existence["_ex_ql"], "inner")
            .join(pfic_li, F.col("T.LineID") == pfic_li["pl_LineID"], "inner")
            .join(
                dist_quarter.alias("DQ"),
                (F.col("T.QuicklinkID") == F.col("DQ.QuicklinkID"))
                & (ns(F.col("T.TrackingKey"), F.lit("")) == F.col("DQ.TrackingKey")),
                "left",
            )
            .join(
                df_part_v_allocable.alias("VL"),
                (F.col("T.LineID") == F.col("VL.LineID"))
                & (F.col("T.QuicklinkID") == F.col("VL.QuicklinkID")),
                "left",
            )
            .filter(F.col("VL.LineID").isNull())  # NOT IN PartVAllocableLines
            .select(
                F.col("T.QuicklinkID").alias("_ql"),
                F.col("T.LineID").alias("_lid"),
                ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
                F.when(F.col("dar_lower") == "max quarter allocation", F.lit("Q4"))
                .when(F.col("dar_lower") == "q0 allocation", F.lit("Q0"))
                .when(
                    F.col("dar_lower") == "distribution date allocation",
                    F.coalesce(F.col("DQ.LOOKUPDATA"), F.lit("Q0")),
                )
                .alias("_new_quarter"),
            )
        )

        # Apply DAR update
        df_alloc_input = _apply_quarter_update(
            df_alloc_input, dar_update, pfic_lt, "_ql", "_lid", "_tk", "_new_quarter"
        )

        # Step 3: UPDATE remaining using PFIC Quarter line
        # WHERE ISNULL(PL.DefaultAllocationRule,'')='' AND VL.LineID IS NULL
        pfic_quarter_flow = pfic_flowup.filter(
            (F.col("LineID") == pfic_quarter_line_id)
            & (ns(F.col("TextValue"), "") != "")
        ).select(
            F.col("PFICFootnoteID").alias("pf_ql"),
            F.col("TextValue").alias("_pf_quarter"),
        )

        pfic_li_no_dar = (
            cfg["_df_pfic_footnote_line_item"]
            .filter(ns(F.col("DefaultAllocationRule"), "") == "")
            .select(F.col("LineID").alias("pl2_LineID"))
        )

        quarter_update = (
            df_alloc_input.alias("T2")
            .filter(F.col("T2.LineTypeID") == pfic_lt)
            .join(pfic_quarter_flow, F.col("T2.QuicklinkID") == pfic_quarter_flow["pf_ql"], "inner")
            .join(pfic_li_no_dar, F.col("T2.LineID") == pfic_li_no_dar["pl2_LineID"], "inner")
            .join(
                df_part_v_allocable.alias("VL2"),
                (F.col("T2.LineID") == F.col("VL2.LineID"))
                & (F.col("T2.QuicklinkID") == F.col("VL2.QuicklinkID")),
                "left",
            )
            .filter(F.col("VL2.LineID").isNull())
            .select(
                F.col("T2.QuicklinkID").alias("_ql"),
                F.col("T2.LineID").alias("_lid"),
                ns(F.col("T2.TrackingKey"), F.lit("")).alias("_tk"),
                F.col("_pf_quarter").alias("_new_quarter"),
            )
        )

        df_alloc_input = _apply_quarter_update(
            df_alloc_input, quarter_update, pfic_lt, "_ql", "_lid", "_tk", "_new_quarter"
        )

    else:
        # ── Branch: IsPFICAllocationbyQuarter != 'C' ──
        # UPDATE T SET [Quarter] = PF.TextValue
        # FROM #TempAllocationInput T
        # INNER JOIN PFICFootNoteFlowUP PF ON PF.RunID=@RunID AND PF.PFICFootNoteID=T.QuicklinkID
        #   AND PF.LineID=@PFICQuarterLineID
        # LEFT JOIN #PartVAllocableLines VL ON VL.LineID=T.LineID AND T.QuicklinkID=VL.QuicklinkID
        # WHERE T.LineTypeID=@PFICFootNoteLineTypeID AND ISNULL(PF.TextValue,'')!=''
        #   AND VL.LineID IS NULL

        pfic_quarter_flow = pfic_flowup.filter(
            (F.col("LineID") == pfic_quarter_line_id)
            & (ns(F.col("TextValue"), "") != "")
        ).select(
            F.col("PFICFootnoteID").alias("pf_ql"),
            F.col("TextValue").alias("_pf_quarter"),
        )

        quarter_update = (
            df_alloc_input.alias("T3")
            .filter(F.col("T3.LineTypeID") == pfic_lt)
            .join(pfic_quarter_flow, F.col("T3.QuicklinkID") == pfic_quarter_flow["pf_ql"], "inner")
            .join(
                df_part_v_allocable.alias("VL3"),
                (F.col("T3.LineID") == F.col("VL3.LineID"))
                & (F.col("T3.QuicklinkID") == F.col("VL3.QuicklinkID")),
                "left",
            )
            .filter(F.col("VL3.LineID").isNull())
            .select(
                F.col("T3.QuicklinkID").alias("_ql"),
                F.col("T3.LineID").alias("_lid"),
                ns(F.col("T3.TrackingKey"), F.lit("")).alias("_tk"),
                F.col("_pf_quarter").alias("_new_quarter"),
            )
        )

        df_alloc_input = _apply_quarter_update(
            df_alloc_input, quarter_update, pfic_lt, "_ql", "_lid", "_tk", "_new_quarter"
        )

    log_timing("update_pfic_quarters_by_config", t0)
    return df_alloc_input


# ---------------------------------------------------------------------------
# update_form_quarters
# SQL lines: 747–800
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def update_form_quarters(
    spark: SparkSession, cfg: dict,
    df_alloc_input: DataFrame,
) -> DataFrame:
    """Update quarters for Form8886, Form199A, Form926, Form8865 lines.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 747-800.
    Row count: POSSIBLY-EMPTY — depends on which form types have quarter data.

    Form8886: Quarter = PF.TextValue from Form8886FlowUP (QuarterAllocations line)
    Form199A: Quarter = PF.TextValue from Form199AFlowUP (QuarterAllocations line)
    Form926:  IF PE Book AND IsDatedTransfersConfigured='C':
                Quarter from QuarterDates matched on StartDate
              ELSE:
                Quarter = 'Q' + DATEPART(qq, TextValue)
    Form8865: Quarter = 'Q' + DATEPART(qq, TextValue) (also requires SchID match)
    """
    log_section("update_form_quarters")
    t0 = time.time()

    run_id = cfg["run_id"]

    # ── Form8886 ──
    f8886_lt = cfg["form8886_line_type_id"]
    f8886_ql = cfg["form8886_quarter_line_id"]

    f8886_flow = (
        read_table(spark, "Form8886FlowUP", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineID") == f8886_ql)
            & (ns(F.col("TextValue"), "") != "")
        )
        .select(
            F.col("Form8886ID").alias("_fid"),
            F.col("TextValue").alias("_new_quarter"),
        )
    )

    f8886_update = (
        df_alloc_input.alias("T")
        .filter(F.col("T.LineTypeID") == f8886_lt)
        .join(f8886_flow, F.col("T.QuicklinkID") == f8886_flow["_fid"], "inner")
        .select(
            F.col("T.QuicklinkID").alias("_ql"),
            F.col("T.LineID").alias("_lid"),
            ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
            F.col("_new_quarter"),
        )
    )
    df_alloc_input = _apply_quarter_update(
        df_alloc_input, f8886_update, f8886_lt, "_ql", "_lid", "_tk", "_new_quarter"
    )

    # ── Form199A ──
    f199a_lt = cfg["form199a_line_type_id"]
    f199a_ql = cfg["form199a_quarter_line_id"]

    f199a_flow = (
        read_table(spark, "Form199AFlowUP", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineID") == f199a_ql)
            & (ns(F.col("TextValue"), "") != "")
        )
        .select(
            F.col("Form199AID").alias("_fid"),
            F.col("TextValue").alias("_new_quarter"),
        )
    )

    f199a_update = (
        df_alloc_input.alias("T")
        .filter(F.col("T.LineTypeID") == f199a_lt)
        .join(f199a_flow, F.col("T.QuicklinkID") == f199a_flow["_fid"], "inner")
        .select(
            F.col("T.QuicklinkID").alias("_ql"),
            F.col("T.LineID").alias("_lid"),
            ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
            F.col("_new_quarter"),
        )
    )
    df_alloc_input = _apply_quarter_update(
        df_alloc_input, f199a_update, f199a_lt, "_ql", "_lid", "_tk", "_new_quarter"
    )

    # ── Form926 ──
    f926_lt = cfg["form926_line_type_id"]
    f926_ql = cfg["form926_quarter_line_id"]
    alloc_type_name = (cfg.get("allocation_type_name") or "").strip().lower()
    is_dated = cfg.get("is_dated_transfers_configured")

    f926_flow = (
        read_table(spark, "Form926FlowUP", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineID") == f926_ql)
            & (ns(F.col("TextValue"), "") != "")
            & (F.lower(F.col("TextValue")) != "various")
        )
        .select(
            F.col("Form926ID").alias("_fid"),
            F.col("TextValue").alias("_tv"),
        )
    )

    if alloc_type_name == "pe book allocation" and is_dated == "C":
        # Quarter from QuarterDates, matched on StartDate
        quarter_dates = read_table(spark, "QuarterDates", cfg).select(
            F.col("Quarter").alias("_qd_quarter"),
            F.col("StartDate"),
        )

        f926_update = (
            df_alloc_input.alias("T")
            .filter(F.col("T.LineTypeID") == f926_lt)
            .join(f926_flow, F.col("T.QuicklinkID") == f926_flow["_fid"], "inner")
            .withColumn("_dt",
                        # BUG-14 FIX: SQL CONVERT(date, TextValue) handles multiple date formats.
                        # Use try_to_date (ANSI-safe: returns NULL on parse failure).
                        F.coalesce(
                            F.expr("try_to_date(_tv, 'yyyy-MM-dd')"),
                            F.expr("try_to_date(_tv, 'MM/dd/yyyy')"),
                            F.to_date(F.lit("1900-01-01"), "yyyy-MM-dd"),
                        )
                        )
            .join(
                quarter_dates,
                # See build_part_v_quarters: anchor on StartDate rather than the
                # SQL's double-matching BETWEEN, and to_date() the DATETIME side
                # so both sides are DateType.
                F.col("_dt") == F.to_date(quarter_dates["StartDate"]),
                "inner",
            )
            .select(
                F.col("T.QuicklinkID").alias("_ql"),
                F.col("T.LineID").alias("_lid"),
                ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
                F.coalesce(F.col("_qd_quarter"), F.lit("Q0")).alias("_new_quarter"),
            )
        )
    else:
        # Quarter = 'Q' + DATEPART(qq, TextValue)
        f926_update = (
            df_alloc_input.alias("T")
            .filter(F.col("T.LineTypeID") == f926_lt)
            .join(f926_flow, F.col("T.QuicklinkID") == f926_flow["_fid"], "inner")
            .select(
                F.col("T.QuicklinkID").alias("_ql"),
                F.col("T.LineID").alias("_lid"),
                ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
                # BUG-14 FIX: multi-format date parse for quarter extraction
                # Use try_to_date (ANSI-safe: returns NULL on parse failure).
                F.concat(
                    F.lit("Q"),
                    F.coalesce(
                        F.quarter(F.coalesce(
                            F.expr("try_to_date(_tv, 'yyyy-MM-dd')"),
                            F.expr("try_to_date(_tv, 'MM/dd/yyyy')"),
                        )).cast("string"),
                        F.lit("0"),
                    ),
                ).alias("_new_quarter"),
            )
        )

    df_alloc_input = _apply_quarter_update(
        df_alloc_input, f926_update, f926_lt, "_ql", "_lid", "_tk", "_new_quarter"
    )

    # ── Form8865 ──
    f8865_lt = cfg["form8865_line_type_id"]
    f8865_ql = cfg["form8865_quarter_line_id"]

    f8865_flow = (
        read_table(spark, "Form8865FlowUP", cfg)
        .filter(
            (F.col("RunID") == run_id)
            & (F.col("LineID") == f8865_ql)
            & (ns(F.col("TextValue"), "") != "")
            & (F.lower(F.col("TextValue")) != "various")
        )
        .select(
            F.col("Form8865ID").alias("_fid"),
            F.col("SchID").alias("_sch"),
            F.col("TextValue").alias("_tv"),
        )
    )

    # Form8865 also joins on SchID (unlike other forms)
    f8865_update = (
        df_alloc_input.alias("T")
        .filter(F.col("T.LineTypeID") == f8865_lt)
        .join(
            f8865_flow,
            (F.col("T.QuicklinkID") == f8865_flow["_fid"])
            & (F.col("T.SchID") == f8865_flow["_sch"]),
            "inner",
        )
        .select(
            F.col("T.QuicklinkID").alias("_ql"),
            F.col("T.LineID").alias("_lid"),
            F.col("T.SchID").alias("_sch_t"),
            ns(F.col("T.TrackingKey"), F.lit("")).alias("_tk"),
            # BUG-14 FIX: multi-format date parse
            # Use try_to_date (ANSI-safe: returns NULL on parse failure).
            F.concat(
                F.lit("Q"),
                F.coalesce(
                    F.quarter(F.coalesce(
                        F.expr("try_to_date(_tv, 'yyyy-MM-dd')"),
                        F.expr("try_to_date(_tv, 'MM/dd/yyyy')"),
                    )).cast("string"),
                    F.lit("0"),
                ),
            ).alias("_new_quarter"),
        )
    )
    # For Form8865 we need SchID in the match because same QuicklinkID can have different SchIDs
    df_alloc_input = _apply_quarter_update_with_schid(
        df_alloc_input, f8865_update, f8865_lt, f8865_flow
    )

    log_timing("update_form_quarters", t0)
    return df_alloc_input


# ---------------------------------------------------------------------------
# Private helper: _apply_quarter_update
# ---------------------------------------------------------------------------
def _apply_quarter_update(
    df: DataFrame, update_source: DataFrame, line_type_id: int,
    ql_col: str, lid_col: str, tk_col: str, quarter_col: str,
) -> DataFrame:
    """Apply a quarter update to df via left join on QuicklinkID + LineID + TrackingKey.

    Uses the standard pattern: left join update_source, WHEN match & LineTypeID → use new quarter.
    """
    df_a = df.alias("base")
    update_source = update_source.alias("upd")

    result = (
        df_a.join(
            update_source,
            (F.col(f"base.QuicklinkID") == F.col(f"upd.{ql_col}"))
            & (F.col(f"base.LineID") == F.col(f"upd.{lid_col}"))
            & (ns(F.col("base.TrackingKey"), F.lit("")) == F.col(f"upd.{tk_col}"))
            & (F.col("base.LineTypeID") == line_type_id),
            "left",
        )
        .withColumn(
            "Quarter",
            # BUG-12 FIX: SQL UPDATE sets Quarter = CASE result, even if NULL.
            # Detect if row matched by checking upd._ql not null (left join → null if no match).
            # If matched: use new quarter (even NULL). If not matched: keep original.
            F.when(
                F.col(f"upd.{ql_col}").isNotNull(),
                F.col(f"upd.{quarter_col}"),
            ).otherwise(F.col("base.Quarter")),
        )
        .select([F.col(f"base.{c}") if c != "Quarter" else F.col("Quarter")
                 for c in df.columns])
    )
    return result


# ---------------------------------------------------------------------------
# Private helper: _apply_quarter_update_with_schid
# Form8865 requires SchID in match key
# ---------------------------------------------------------------------------
def _apply_quarter_update_with_schid(
    df: DataFrame, update_source: DataFrame, line_type_id: int,
    flow_df: DataFrame,
) -> DataFrame:
    """Apply Form8865 quarter update with SchID match."""
    df_a = df.alias("base")
    update_source = update_source.alias("upd")

    result = (
        df_a.join(
            update_source,
            (F.col("base.QuicklinkID") == F.col("upd._ql"))
            & (F.col("base.LineID") == F.col("upd._lid"))
            # BUG-15 FIX: SQL uses plain = for SchID (NULL != NULL).
            # Removed COALESCE(x, 0) which made NULLs match NULLs.
            & (F.col("base.SchID") == F.col("upd._sch_t"))
            & (ns(F.col("base.TrackingKey"), F.lit("")) == F.col("upd._tk"))
            & (F.col("base.LineTypeID") == line_type_id),
            "left",
        )
        .withColumn(
            "Quarter",
            F.when(
                F.col("upd._new_quarter").isNotNull(),
                F.col("upd._new_quarter"),
            ).otherwise(F.col("base.Quarter")),
        )
        .select([F.col(f"base.{c}") if c != "Quarter" else F.col("Quarter")
                 for c in df.columns])
    )
    return result
