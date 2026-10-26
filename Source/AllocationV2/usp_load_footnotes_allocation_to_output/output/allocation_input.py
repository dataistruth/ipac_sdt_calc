"""
Allocation input builder for uspLoadFootnotesAllocationToOutput.

Handles the initial AllocationInput read, BookEffective_Snapshot load,
and the complex multi-pass INSERT/DELETE logic that builds the final
#AllocationInput working DataFrame.
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import time

from Common_V2.core.helpers import read_table, ns, ns0
from Common_V2.core.observability import log_section, log_timing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# build_temp_book_effective
# SQL lines: 505–523 (SELECT INTO + UPDATE)
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_temp_book_effective(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load BookEffective_Snapshot filtered by workflow, client, tax period.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 505-523.
    Row count: ALWAYS-NON-EMPTY — core pipeline input; invalid RunID = fail.
    Includes UPDATE of AdjustmentAllocationTypeID where it equals @BookAllocationTypeID.

    Columns selected (from _columns.md: dbo.BookEffective_Snapshot):
    - UnderlyingEntityID (INT)
    - LineID (INT)
    - FootNoteID (INT)
    - SourceID (INT)
    - AllocationTypeID (INT) — aliased as AllocationTypeid in SQL
    - AdjustmentAllocationTypeID (INT)
    - TrackingKey (VARCHAR(4000))
    - Tag (VARCHAR(500))
    - IsExcludefromTransfer (BIT → BOOLEAN)
    """
    log_section("build_temp_book_effective")
    t0 = time.time()

    book_alloc_type_id = cfg["book_allocation_type_id"]

    df = (
        read_table(spark, "BookEffective_Snapshot", cfg)
        .filter(
            (F.col("WorkflowID") == cfg["car_workflow_id"])
            & (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
        )
        .select(
            F.col("UnderlyingEntityID"),
            F.col("LineID"),
            F.col("FootNoteID"),
            F.col("SourceID"),
            F.col("AllocationTypeID"),
            F.col("AdjustmentAllocationTypeID"),
            F.col("TrackingKey"),
            F.col("Tag"),
            # ISNULL(IsExcludefromTransfer, 0) — BIT column → cast to INT to match downstream
            F.coalesce(F.col("IsExcludefromTransfer").cast("int"), F.lit(0)).alias("IsExcludefromTransfer"),
        )
    )

    # UPDATE: SET AdjustmentAllocationTypeID = AllocationTypeID
    #         WHERE AdjustmentAllocationTypeID = @BookAllocationTypeID
    df = df.withColumn(
        "AdjustmentAllocationTypeID",
        F.when(
            F.col("AdjustmentAllocationTypeID") == book_alloc_type_id,
            F.col("AllocationTypeID"),
        ).otherwise(F.col("AdjustmentAllocationTypeID")),
    )

    log_timing("build_temp_book_effective", t0)
    return df


# ---------------------------------------------------------------------------
# build_temp_allocation_input
# SQL lines: 526–538
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_temp_allocation_input(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load AllocationInput filtered by RunID, excluding K1/Adjustment line types, Amount<>0.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 526-538.
    Row count: ALWAYS-NON-EMPTY — core pipeline; filtered AllocationInput with Amount<>0.
    Initializes Quarter column to 'Q0' for all rows.

    Columns from _columns.md (dbo.AllocationInput):
    RunID(BIGINT), ClientID(BIGINT), EntityID(INT), LineTypeID(INT), LineID(INT),
    Amount(FLOAT), QuicklinkID(INT), Amount704b(FLOAT), CategoryID(INT),
    PeriodID(INT), LineCode(VARCHAR(100)), ParentEntityID(INT),
    SuperParentEntityID(INT), AdjustmentTypeID(INT), Tag(VARCHAR(5000)),
    TrackingKey(VARCHAR(4000)), SchID(INT), OriginalParentEntityID(INT)
    """
    log_section("build_temp_allocation_input")
    t0 = time.time()

    k1_lt = cfg["k1_line_type_id"]
    adj_lt = cfg["adjustment_line_type_id"]

    df = (
        read_table(spark, "AllocationInput", cfg)
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (~F.col("LineTypeID").isin([k1_lt, adj_lt]))
            & (ns0(F.col("Amount")) != 0)
        )
        .select(
            "RunID", "ClientID", "EntityID", "LineTypeID", "LineID",
            "Amount", "QuicklinkID", "Amount704b", "CategoryID", "PeriodID",
            "LineCode", "ParentEntityID", "SuperParentEntityID",
            "AdjustmentTypeID", "Tag", "TrackingKey", "SchID",
            "OriginalParentEntityID",
        )
        .withColumn("Quarter", F.lit("Q0"))
    )

    log_timing("build_temp_allocation_input", t0)
    return df


# ---------------------------------------------------------------------------
# build_zero_exclude_lines
# SQL lines: 571–583
# Row count: POSSIBLY-EMPTY
# ---------------------------------------------------------------------------
def build_zero_exclude_lines(spark: SparkSession, cfg: dict) -> DataFrame:
    """Build #ZeroExcludeLines: Form926 PreTransferOwnership/PostTransferOwnership + Form8865 PERCENT lines.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 571-583.
    Row count: POSSIBLY-EMPTY — depends on whether Form926/8865 line items exist.
    Used later in writers.py for final deduction logic.

    Columns: LineTypeID (INT), LineID (INT)
    """
    log_section("build_zero_exclude_lines")
    t0 = time.time()

    # Form926LineItem rows with ShortName IN ('PreTransferOwnership', 'PostTransferOwnership')
    form926_lines = (
        read_table(spark, "Form926LineItem", cfg)
        .filter(
            (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
            & (F.lower(F.col("ShortName")).isin(["pretransferownership", "posttransferownership"]))
        )
        .select(
            F.lit(cfg["form926_line_type_id"]).alias("LineTypeID"),
            F.col("LineID"),
        )
    )

    # Form8865LineItem rows with LineDataType = 'PERCENT'
    form8865_lines = (
        read_table(spark, "Form8865LineItem", cfg)
        .filter(
            (F.col("ClientID") == cfg["client_id"])
            & (F.col("TaxPeriodID") == cfg["tax_period_id"])
            & (F.lower(F.col("LineDataType")) == "percent")
        )
        .select(
            F.lit(cfg["form8865_line_type_id"]).alias("LineTypeID"),
            F.col("LineID"),
        )
    )

    df = form926_lines.unionByName(form8865_lines)

    log_timing("build_zero_exclude_lines", t0)
    return df


# ---------------------------------------------------------------------------
# build_temp_final_effective_pct
# SQL lines: 597–601
# Row count: POSSIBLY-EMPTY (conditional on RankForRulePickup <> 2)
# ---------------------------------------------------------------------------
def build_temp_final_effective_pct(spark: SparkSession, cfg: dict) -> DataFrame:
    """Load FNFinalEffectivePercentages filtered by RunID and RankForRule.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 597-601.
    Row count: POSSIBLY-EMPTY — only loaded when rank_for_rule_pickup != 2.
    If rank == 2, returns empty DataFrame with correct schema.

    Columns: InvestmentID, PartnerNumber, EffPercentage, AllocationType, Quarter,
    TypeID, TrackingKey, Tag, LineTypeID, LineID, IsExcludefromTransfer, AssetClassID
    """
    log_section("build_temp_final_effective_pct")
    t0 = time.time()

    rank = cfg["rank_for_rule_pickup"]

    if rank == 2:
        # SQL: if(@LocalRankForRulePickup <> 2) — skip when rank=2
        from pyspark.sql.types import (
            StructType, StructField, IntegerType, StringType,
            DoubleType,
        )
        schema = StructType([
            StructField("InvestmentID", IntegerType()),
            StructField("PartnerNumber", StringType()),
            StructField("EffPercentage", DoubleType()),
            StructField("AllocationType", StringType()),
            StructField("Quarter", StringType()),
            StructField("TypeID", IntegerType()),
            StructField("TrackingKey", StringType()),
            StructField("Tag", StringType()),
            StructField("LineTypeID", IntegerType()),
            StructField("LineID", IntegerType()),
            StructField("IsExcludefromTransfer", IntegerType()),
            StructField("AssetClassID", IntegerType()),
        ])
        df = spark.createDataFrame([], schema)
        log_timing("build_temp_final_effective_pct", t0)
        return df

    df = (
        read_table(spark, "FNFinalEffectivePercentages", cfg)
        .filter(
            (F.col("RunID") == cfg["run_id"])
            & (F.col("RankForRule") == rank)
        )
        .select(
            "InvestmentID", "PartnerNumber", "EffPercentage", "AllocationType",
            "Quarter",
            F.col("TypeId").alias("TypeID"),
            F.col("Trackingkey").alias("TrackingKey"),
            "Tag", "LineTypeID", "LineID",
            F.col("IsExcludefromTransfer").cast("int").alias("IsExcludefromTransfer"),
            "AssetClassID",
        )
        .distinct()
    )

    log_timing("build_temp_final_effective_pct", t0)
    return df


# ---------------------------------------------------------------------------
# build_allocation_input
# SQL lines: 1270–1730
# Row count: ALWAYS-NON-EMPTY
# ---------------------------------------------------------------------------
def build_allocation_input(
    spark: SparkSession, cfg: dict,
    df_temp_alloc_input: DataFrame,
    df_temp_book_eff: DataFrame,
    df_underlyings_fn: DataFrame,
) -> DataFrame:
    """Build final #AllocationInput via 5-pass INSERT/DELETE logic.

    Converted from: uspLoadFootnotesAllocationToOutput, SQL lines 1270-1730.
    Row count: ALWAYS-NON-EMPTY — main accumulator; if empty, SP exits early.

    Pass 1: At Risk lines with BookEffective match (FootNoteID<>-1, LineID<>-1)
    Pass 2: Non-AtRisk lines with BookEffective match (FootNoteID<>-1, LineID<>-1)
    Pass 3: BookEffective with FootNoteID<>-1, LineID=-1 (wildcard line)
    Pass 4: BookEffective with FootNoteID=-1, LineID<>-1 (wildcard footnote)
    Pass 5: BookEffective with FootNoteID=-1, LineID=-1 (full wildcard)
    Catch-all: Remaining unmatched lines from TempAllocationInput.

    Each pass:
    1. INSERT matching rows into #AllocationInput with TypeID resolution
    2. DELETE matched rows from #TempAllocationInput
    3. DELETE consumed rows from #TempBookEffective

    Output columns (#AllocationInput):
        RunID, ClientID, EntityID, LineTypeID, LineID, Amount, QuicklinkID, Amount704b,
        CategoryID, PeriodID, LineCode, ParentEntityID, SuperParentEntityID, TypeID,
        Tag, IsExcludefromTransfer, TrackingKey, Quarter, SchID, OriginalParentEntityID
    """
    log_section("build_allocation_input")
    t0 = time.time()

    at_risk_lt = cfg["at_risk_line_type_id"]
    k1_lt = cfg["k1_line_type_id"]
    pfic_lt = cfg["pfic_footnote_line_type_id"]
    cost_at = cfg["cost_allocation_type_id"]
    lp_offset_at = cfg["lp_offset_allocation_type_id"]
    gp_offset_at = cfg["gp_offset_allocation_type_id"]

    # Load PFICFootnoteLineItem and K1Lineitem for LineDescription lookup (broadcast — small)
    pfic_li = F.broadcast(
        cfg["_df_pfic_footnote_line_item"]
        .select(F.col("LineID").alias("pli_LineID"), F.col("LineDescription").alias("pli_desc"))
    )
    k1_li = F.broadcast(
        read_table(spark, "K1Lineitem", cfg)
        .select(F.col("LineID").alias("k1_LineID"), F.col("LineDescription").alias("k1_desc"))
    )

    # Initialize accumulators
    remaining_input = df_temp_alloc_input
    # Broadcast BookEffective — per-entity it's typically < 200 rows.
    # All 5 pass subsets (be_p1..be_p5) inherit broadcast hint,
    # converting shuffle joins to BroadcastHashJoins.
    remaining_book = F.broadcast(df_temp_book_eff)
    all_inserts = []

    # ── Helper: null-safe join key pattern ──
    # CASE WHEN ISNULL(B.TrackingKey,'')='' THEN '-1' ELSE B.TrackingKey END
    #   = CASE WHEN ISNULL(B.TrackingKey,'')='' THEN '-1' ELSE I.TrackingKey END
    def _ns_match(b_col, i_col):
        """Null-safe match: both empty → match; both non-empty → equal."""
        return (
            F.when(ns(b_col) == "", F.lit("-1")).otherwise(b_col)
            ==
            F.when(ns(b_col) == "", F.lit("-1")).otherwise(i_col)
        )

    # ── Helper: resolve TypeID ──
    def _type_id_expr(b_adj, bk_adj, ai_alloc, line_type_col, desc_col, is_at_risk=False):
        """Build ISNULL cascade for TypeID resolution with LP/GP offset detection."""
        lt_ref = at_risk_lt if is_at_risk else pfic_lt
        _null_int = F.lit(None).cast("int")
        return F.coalesce(
            b_adj if b_adj is not None else _null_int,
            bk_adj if bk_adj is not None else _null_int,
            ai_alloc if ai_alloc is not None else _null_int,
            F.when(
                (line_type_col == lt_ref)
                & (F.lower(ns(desc_col)).endswith("- lp - offset")),
                F.lit(lp_offset_at),
            ).when(
                (line_type_col == lt_ref)
                & (F.lower(ns(desc_col)).endswith("- gp - offset")),
                F.lit(gp_offset_at),
            ).otherwise(F.lit(cost_at)),
        )

    # ── PASS 1: At Risk lines with BookEffective (FootNoteID<>-1, LineID<>-1) ──
    be_p1 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1)
        & (F.col("SourceID") == at_risk_lt)
    ).alias("B1")

    be_k1 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1)
        & (F.col("SourceID") == k1_lt)
    ).alias("BK1")

    input_atrisk = remaining_input.filter(F.col("LineTypeID") == at_risk_lt).alias("I1")

    # Multi-way join: I → K1Lineitem, LEFT B1, LEFT BK1, LEFT AI (underlyings)
    p1_joined = (
        input_atrisk
        .join(k1_li, F.col("I1.LineID") == k1_li["k1_LineID"], "inner")
        .join(
            be_p1,
            (F.col("I1.EntityID") == F.col("B1.UnderlyingEntityID"))
            & (F.col("B1.FootNoteID") == F.col("I1.QuicklinkID"))
            & (ns0(F.col("I1.LineID")) == ns0(F.col("B1.LineID")))
            & (F.col("B1.SourceID") == at_risk_lt)
            & (_ns_match(F.col("B1.TrackingKey"), F.col("I1.TrackingKey")))
            & (_ns_match(F.col("B1.Tag"), F.col("I1.Tag"))),
            "left",
        )
        .join(
            be_k1,
            (F.col("I1.EntityID") == F.col("BK1.UnderlyingEntityID"))
            & (ns0(F.col("I1.LineID")) == ns0(F.col("BK1.LineID")))
            & (F.col("BK1.SourceID") == k1_lt)
            & (_ns_match(F.col("BK1.TrackingKey"), F.col("I1.TrackingKey")))
            & (_ns_match(F.col("BK1.Tag"), F.col("I1.Tag"))),
            "left",
        )
        .join(
            df_underlyings_fn.alias("AI1"),
            (F.col("I1.EntityID") == F.col("AI1.UnderlyingEntityId"))
            & (F.col("I1.LineID") == F.col("AI1.LineID"))
            & (F.col("I1.LineTypeID") == F.col("AI1.LineTypeId"))
            & (F.col("I1.TrackingKey") == F.col("AI1.TrackingKey")),
            "left",
        )
        .filter(
            (ns0(F.col("B1.FootNoteID")) != -1)
            & ((ns0(F.col("B1.LineID")) != -1) | (F.col("AI1.LineID").isNotNull())
               | (ns0(F.col("BK1.LineID")) != -1))
        )
    )

    p1_insert = (
        p1_joined
        .select(
            F.col("I1.RunID"), F.col("I1.ClientID"), F.col("I1.EntityID"),
            F.col("I1.LineTypeID"), F.col("I1.LineID"), F.col("I1.Amount"),
            F.col("I1.QuicklinkID"), F.col("I1.Amount704b"), F.col("I1.CategoryID"),
            F.col("I1.PeriodID"), F.col("I1.LineCode"),
            F.col("I1.ParentEntityID"), F.col("I1.SuperParentEntityID"),
            _type_id_expr(
                F.col("B1.AdjustmentAllocationTypeID"),
                F.col("BK1.AdjustmentAllocationTypeID"),
                F.col("AI1.AllocationTypeId"),
                F.col("I1.LineTypeID"), F.col("k1_desc"), is_at_risk=True,
            ).alias("TypeID"),
            ns(F.col("I1.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(
                F.col("B1.IsExcludefromTransfer").cast("int"),
                F.col("AI1.ExcludeFromTransfers"),
                F.lit(0),
            ).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B1.TrackingKey"), F.col("I1.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I1.Quarter"), F.col("I1.SchID"), F.col("I1.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(p1_insert)

    # Delete matched rows from remaining_input (At Risk pass 1)
    remaining_input = remaining_input.join(
        p1_insert.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey")
        .distinct().alias("del1"),
        (remaining_input["EntityID"] == F.col("del1.EntityID"))
        & (remaining_input["LineID"] == F.col("del1.LineID"))
        & (remaining_input["LineTypeID"] == F.col("del1.LineTypeID"))
        & (remaining_input["QuicklinkID"] == F.col("del1.QuicklinkID"))
        & (remaining_input["TrackingKey"] == F.col("del1.TrackingKey")),
        "left_anti",
    )
    # Delete consumed BookEffective (AtRisk, FootNoteID<>-1, LineID<>-1)
    remaining_book = remaining_book.filter(
        ~((F.col("SourceID") == at_risk_lt)
          & (ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1))
    )

    # ── PASS 2: Non-AtRisk lines (FootNoteID<>-1, LineID<>-1) ──
    be_p2 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1)
    ).alias("B2")

    p2_joined = (
        remaining_input.alias("I2")
        .join(pfic_li, F.col("I2.LineID") == pfic_li["pli_LineID"], "left")
        .join(
            be_p2,
            (F.col("I2.EntityID") == F.col("B2.UnderlyingEntityID"))
            & (F.col("B2.FootNoteID") == F.col("I2.QuicklinkID"))
            & (ns0(F.col("I2.LineID")) == ns0(F.col("B2.LineID")))
            & (F.col("B2.SourceID") == F.col("I2.LineTypeID"))
            & (_ns_match(F.col("B2.TrackingKey"), F.col("I2.TrackingKey")))
            & (_ns_match(F.col("B2.Tag"), F.col("I2.Tag"))),
            "left",
        )
        .join(
            df_underlyings_fn.alias("AI2"),
            (F.col("I2.EntityID") == F.col("AI2.UnderlyingEntityId"))
            & (F.col("I2.LineID") == F.col("AI2.LineID"))
            & (F.col("I2.LineTypeID") == F.col("AI2.LineTypeId"))
            & (F.col("I2.TrackingKey") == F.col("AI2.TrackingKey")),
            "left",
        )
        .filter(
            (ns0(F.col("B2.FootNoteID")) != -1) & (ns0(F.col("B2.LineID")) != -1)
            | (F.col("AI2.LineID").isNotNull())
        )
    )

    p2_insert = (
        p2_joined
        .select(
            F.col("I2.RunID"), F.col("I2.ClientID"), F.col("I2.EntityID"),
            F.col("I2.LineTypeID"), F.col("I2.LineID"), F.col("I2.Amount"),
            F.col("I2.QuicklinkID"), F.col("I2.Amount704b"), F.col("I2.CategoryID"),
            F.col("I2.PeriodID"), F.col("I2.LineCode"),
            F.col("I2.ParentEntityID"), F.col("I2.SuperParentEntityID"),
            _type_id_expr(
                F.col("B2.AdjustmentAllocationTypeID"), None,
                F.col("AI2.AllocationTypeId"),
                F.col("I2.LineTypeID"), F.col("pli_desc"), is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I2.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(
                F.col("B2.IsExcludefromTransfer").cast("int"),
                F.col("AI2.ExcludeFromTransfers"),
                F.lit(0),
            ).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B2.TrackingKey"), F.col("I2.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I2.Quarter"), F.col("I2.SchID"), F.col("I2.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(p2_insert)

    remaining_input = remaining_input.join(
        p2_insert.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey")
        .distinct().alias("del2"),
        (remaining_input["EntityID"] == F.col("del2.EntityID"))
        & (remaining_input["LineID"] == F.col("del2.LineID"))
        & (remaining_input["LineTypeID"] == F.col("del2.LineTypeID"))
        & (remaining_input["QuicklinkID"] == F.col("del2.QuicklinkID"))
        & (remaining_input["TrackingKey"] == F.col("del2.TrackingKey")),
        "left_anti",
    )
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1))
    )

    # ── PASS 3: FootNoteID<>-1, LineID=-1 (wildcard line) ──
    be_p3 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) == -1)
    ).alias("B3")

    p3_joined = (
        remaining_input.alias("I3")
        .join(pfic_li, F.col("I3.LineID") == pfic_li["pli_LineID"], "left")
        .join(
            be_p3,
            (F.col("I3.EntityID") == F.col("B3.UnderlyingEntityID"))
            & (F.col("B3.FootNoteID") == F.col("I3.QuicklinkID"))
            & (F.col("B3.SourceID") == F.col("I3.LineTypeID"))
            & (_ns_match(F.col("B3.TrackingKey"), F.col("I3.TrackingKey")))
            & (_ns_match(F.col("B3.Tag"), F.col("I3.Tag"))),
            "inner",
        )
    )

    p3_insert = (
        p3_joined
        .select(
            F.col("I3.RunID"), F.col("I3.ClientID"), F.col("I3.EntityID"),
            F.col("I3.LineTypeID"), F.col("I3.LineID"), F.col("I3.Amount"),
            F.col("I3.QuicklinkID"), F.col("I3.Amount704b"), F.col("I3.CategoryID"),
            F.col("I3.PeriodID"), F.col("I3.LineCode"),
            F.col("I3.ParentEntityID"), F.col("I3.SuperParentEntityID"),
            _type_id_expr(
                F.col("B3.AdjustmentAllocationTypeID"), None, None,
                F.col("I3.LineTypeID"), F.col("pli_desc"), is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I3.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(F.col("B3.IsExcludefromTransfer").cast("int"), F.lit(0)).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B3.TrackingKey"), F.col("I3.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I3.Quarter"), F.col("I3.SchID"), F.col("I3.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(p3_insert)

    remaining_input = remaining_input.join(
        p3_insert.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey")
        .distinct().alias("del3"),
        (remaining_input["EntityID"] == F.col("del3.EntityID"))
        & (remaining_input["LineID"] == F.col("del3.LineID"))
        & (remaining_input["LineTypeID"] == F.col("del3.LineTypeID"))
        & (remaining_input["QuicklinkID"] == F.col("del3.QuicklinkID"))
        & (remaining_input["TrackingKey"] == F.col("del3.TrackingKey")),
        "left_anti",
    )
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) == -1))
    )

    # ── PASS 4: FootNoteID=-1, LineID<>-1 (wildcard footnote) ──
    be_p4 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) == -1) & (ns0(F.col("LineID")) != -1)
    ).alias("B4")

    p4_joined = (
        remaining_input.alias("I4")
        .join(pfic_li, F.col("I4.LineID") == pfic_li["pli_LineID"], "left")
        .join(
            be_p4,
            (F.col("I4.EntityID") == F.col("B4.UnderlyingEntityID"))
            & (ns0(F.col("B4.LineID")) == ns0(F.col("I4.LineID")))
            & (F.col("B4.SourceID") == F.col("I4.LineTypeID"))
            & (_ns_match(F.col("B4.TrackingKey"), F.col("I4.TrackingKey")))
            & (_ns_match(F.col("B4.Tag"), F.col("I4.Tag"))),
            "inner",
        )
    )

    p4_insert = (
        p4_joined
        .select(
            F.col("I4.RunID"), F.col("I4.ClientID"), F.col("I4.EntityID"),
            F.col("I4.LineTypeID"), F.col("I4.LineID"), F.col("I4.Amount"),
            F.col("I4.QuicklinkID"), F.col("I4.Amount704b"), F.col("I4.CategoryID"),
            F.col("I4.PeriodID"), F.col("I4.LineCode"),
            F.col("I4.ParentEntityID"), F.col("I4.SuperParentEntityID"),
            _type_id_expr(
                F.col("B4.AdjustmentAllocationTypeID"), None, None,
                F.col("I4.LineTypeID"), F.col("pli_desc"), is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I4.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(F.col("B4.IsExcludefromTransfer").cast("int"), F.lit(0)).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B4.TrackingKey"), F.col("I4.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I4.Quarter"), F.col("I4.SchID"), F.col("I4.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(p4_insert)

    remaining_input = remaining_input.join(
        p4_insert.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey")
        .distinct().alias("del4"),
        (remaining_input["EntityID"] == F.col("del4.EntityID"))
        & (remaining_input["LineID"] == F.col("del4.LineID"))
        & (remaining_input["LineTypeID"] == F.col("del4.LineTypeID"))
        & (remaining_input["QuicklinkID"] == F.col("del4.QuicklinkID"))
        & (remaining_input["TrackingKey"] == F.col("del4.TrackingKey")),
        "left_anti",
    )
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) == -1) & (ns0(F.col("LineID")) != -1))
    )

    # ── PASS 5: FootNoteID=-1, LineID=-1 (full wildcard) ──
    be_p5 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) == -1) & (ns0(F.col("LineID")) == -1)
    ).alias("B5")

    p5_joined = (
        remaining_input.alias("I5")
        .join(pfic_li, F.col("I5.LineID") == pfic_li["pli_LineID"], "left")
        .join(
            be_p5,
            (F.col("I5.EntityID") == F.col("B5.UnderlyingEntityID"))
            & (F.col("B5.SourceID") == F.col("I5.LineTypeID"))
            & (_ns_match(F.col("B5.TrackingKey"), F.col("I5.TrackingKey")))
            & (_ns_match(F.col("B5.Tag"), F.col("I5.Tag"))),
            "inner",
        )
    )

    p5_insert = (
        p5_joined
        .select(
            F.col("I5.RunID"), F.col("I5.ClientID"), F.col("I5.EntityID"),
            F.col("I5.LineTypeID"), F.col("I5.LineID"), F.col("I5.Amount"),
            F.col("I5.QuicklinkID"), F.col("I5.Amount704b"), F.col("I5.CategoryID"),
            F.col("I5.PeriodID"), F.col("I5.LineCode"),
            F.col("I5.ParentEntityID"), F.col("I5.SuperParentEntityID"),
            _type_id_expr(
                F.col("B5.AdjustmentAllocationTypeID"), None, None,
                F.col("I5.LineTypeID"), F.col("pli_desc"), is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I5.Tag"), F.lit("")).alias("Tag"),
            F.coalesce(F.col("B5.IsExcludefromTransfer").cast("int"), F.lit(0)).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B5.TrackingKey"), F.col("I5.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I5.Quarter"), F.col("I5.SchID"), F.col("I5.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(p5_insert)

    remaining_input = remaining_input.join(
        p5_insert.select("EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey")
        .distinct().alias("del5"),
        (remaining_input["EntityID"] == F.col("del5.EntityID"))
        & (remaining_input["LineID"] == F.col("del5.LineID"))
        & (remaining_input["LineTypeID"] == F.col("del5.LineTypeID"))
        & (remaining_input["QuicklinkID"] == F.col("del5.QuicklinkID"))
        & (remaining_input["TrackingKey"] == F.col("del5.TrackingKey")),
        "left_anti",
    )

    # ── CATCH-ALL: Remaining unmatched lines ──
    # LEFT JOIN BookEffective (match all criteria) WHERE B.UnderlyingEntityID IS NULL
    catchall = (
        remaining_input.alias("I6")
        .join(pfic_li, F.col("I6.LineID") == pfic_li["pli_LineID"], "left")
        .join(
            remaining_book.alias("B6"),
            (F.col("I6.EntityID") == F.col("B6.UnderlyingEntityID"))
            & (F.col("B6.SourceID") == F.col("I6.LineTypeID"))
            & (ns0(F.col("I6.LineID")) == ns0(F.col("B6.LineID")))
            & (ns0(F.col("I6.QuicklinkID")) == ns0(F.col("B6.FootNoteID")))
            & (_ns_match(F.col("B6.TrackingKey"), F.col("I6.TrackingKey")))
            & (_ns_match(F.col("B6.Tag"), F.col("I6.Tag"))),
            "left",
        )
        .filter(F.col("B6.UnderlyingEntityID").isNull())
        .select(
            F.col("I6.RunID"), F.col("I6.ClientID"), F.col("I6.EntityID"),
            F.col("I6.LineTypeID"), F.col("I6.LineID"), F.col("I6.Amount"),
            F.col("I6.QuicklinkID"), F.col("I6.Amount704b"), F.col("I6.CategoryID"),
            F.col("I6.PeriodID"), F.col("I6.LineCode"),
            F.col("I6.ParentEntityID"), F.col("I6.SuperParentEntityID"),
            _type_id_expr(
                F.col("B6.AdjustmentAllocationTypeID"), None, None,
                F.col("I6.LineTypeID"), F.col("pli_desc"), is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I6.Tag"), F.lit("")).alias("Tag"),
            F.lit(0).alias("IsExcludefromTransfer"),
            F.coalesce(F.col("B6.TrackingKey"), F.col("I6.TrackingKey"), F.lit("")).alias("TrackingKey"),
            F.col("I6.Quarter"), F.col("I6.SchID"), F.col("I6.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(catchall)

    # ── Final assembly ──
    df_alloc_input = all_inserts[0]
    for part in all_inserts[1:]:
        df_alloc_input = df_alloc_input.unionByName(part)

    log_timing("build_allocation_input", t0)
    return df_alloc_input
