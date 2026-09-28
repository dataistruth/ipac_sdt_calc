"""Input preparation, validation, and allocation processing.

Consolidates: prepare_lookthrough_input, allocation validation,
offset handling, and the three allocation processors (by-amount,
by-percentage, 704c).
"""

import logging
import time

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from Common_V2.core.helpers import read_table, table_prefix, ns, ns0
from Common_V2.core.observability import log_section, log_timing
from Common_V2.core.assertions import warn_possibly_empty

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input preparation
# ---------------------------------------------------------------------------

def prepare_lookthrough_input(spark: SparkSession, cfg: dict,
                              input_data: DataFrame,
                              line_items: DataFrame,
                              book_effective: DataFrame,
                              entity_rules: DataFrame,
                              all_underlyings: DataFrame,
                              default_rules: DataFrame = None) -> DataFrame:
    """Prepare final lookthrough input with all business logic (2-pass).
    Pass 1: Input rows that match EITHER specific book_effective OR all_underlyings.
    Pass 2: Remaining rows LEFT JOIN wildcard book_effective (LineID=-1).
    ALWAYS-NON-EMPTY for valid run.
    """
    log_section("prepare_lookthrough_input")
    t0 = time.time()
    k1_lti = cfg["k1_line_type_id"]
    adj_lti = cfg["adjustment_line_type_id"]
    book_alloc_id = cfg["book_allocation_type_id"]
    cost_alloc_id = cfg["cost_allocation_type_id"]
    offset_alloc_id = cfg["offset_allocation_type_id"]

    book_effective_no_wildcard = book_effective.filter(F.col("LineID") != -1)
    book_effective_wildcard = book_effective.filter(F.col("LineID") == -1)

    # --- Pass 1: Input rows matching book_effective OR all_underlyings ---
    # SQL: INNER JOIN #LineItem K ON K.LineID = L.LineID
    #      AND K.LineTypeID = CASE WHEN L.LineTypeID = @AdjustmentLineTypeID
    #          THEN @K1LineTypeID ELSE L.LineTypeID END
    # LEFT JOIN #TempBookEffective B ON EntityID + LineID + SourceID + TrackingKey + Tag
    # LEFT JOIN #TempEnitityAllocationRule ER ON LineID
    # LEFT JOIN #TempAllUnderlyings AI ON EntityID + TrackingKey + LineID + LineTypeID
    # WHERE B.LineID != -1 OR AI.LineID != -1
    pass1 = input_data.alias("L").join(
        line_items.alias("K"),
        (F.col("K.LineID") == F.col("L.LineID")) &
        (F.col("K.LineTypeID") ==
         F.when(F.col("L.LineTypeID") == adj_lti, F.lit(k1_lti))
         .otherwise(F.col("L.LineTypeID")))
    ).join(
        book_effective_no_wildcard.alias("B"),
        (F.col("B.UnderlyingEntityID") == F.col("L.EntityID")) &
        (F.col("B.LineID") == F.col("L.LineID")) &
        (F.col("B.SourceID") == k1_lti) &
        (F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.TrackingKey")) ==
         F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.TrackingKey"))) &
        (F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.Tag")) ==
         F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.Tag"))),
        "left"
    ).join(
        entity_rules.alias("ER"),
        F.col("ER.LineID") == F.col("L.LineID"),
        "left"
    ).join(
        all_underlyings.alias("AI"),
        (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId")) &
        (F.col("L.TrackingKey") == F.col("AI.TrackingKey")) &
        (F.col("L.LineID") == F.col("AI.LineID")) &
        (F.col("AI.LineTypeID") ==
         F.when(F.col("L.LineTypeID") == adj_lti, F.lit(k1_lti))
         .otherwise(F.col("L.LineTypeID"))),
        "left"
    ).filter(
        # WHERE B.LineID != -1 OR AI.LineID != -1
        (F.col("B.LineID").isNotNull()) | (F.col("AI.LineID").isNotNull())
    ).select(
        _input_select_columns_pass1(cfg, "L", "K", "B", "ER", "AI",
                                    book_alloc_id, cost_alloc_id, offset_alloc_id,
                                    adj_lti, k1_lti)
    )

    # --- Identify remaining rows (not matched in pass 1) ---
    # SQL: DELETE from #TempLookThroughAllocationInputDataLoad using same criteria
    input_remaining = input_data.alias("L").join(
        book_effective_no_wildcard.alias("B"),
        (F.col("B.UnderlyingEntityID") == F.col("L.EntityID")) &
        (F.col("B.LineID") == F.col("L.LineID")) &
        (F.col("B.SourceID") == k1_lti) &
        (F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.TrackingKey")) ==
         F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.TrackingKey"))) &
        (F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.Tag")) ==
         F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.Tag"))),
        "left"
    ).join(
        all_underlyings.alias("AI"),
        (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId")) &
        (F.col("L.TrackingKey") == F.col("AI.TrackingKey")) &
        (F.col("L.LineID") == F.col("AI.LineID")) &
        (F.col("AI.LineTypeID") ==
         F.when(F.col("L.LineTypeID") == adj_lti, F.lit(k1_lti))
         .otherwise(F.col("L.LineTypeID"))),
        "left"
    ).filter(
        # Rows that did NOT match in pass 1
        (F.col("B.LineID").isNull()) & (F.col("AI.LineID").isNull())
    ).select([F.col(f"L.{c}") for c in input_data.columns])

    # --- Pass 2: Remaining rows with wildcard book effective (LineID = -1) ---
    # SQL: LEFT JOIN #TempBookEffective B (after DELETE WHERE LineID <> -1)
    #      No #TempAllUnderlyings join
    pass2 = input_remaining.alias("L").join(
        line_items.alias("K"),
        (F.col("K.LineID") == F.col("L.LineID")) &
        (F.col("K.LineTypeID") ==
         F.when(F.col("L.LineTypeID") == adj_lti, F.lit(k1_lti))
         .otherwise(F.col("L.LineTypeID")))
    ).join(
        book_effective_wildcard.alias("B"),
        (F.col("B.UnderlyingEntityID") == F.col("L.EntityID")) &
        (F.col("B.SourceID") == k1_lti) &
        (F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.TrackingKey")) ==
         F.when(ns(F.col("B.TrackingKey")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.TrackingKey"))) &
        (F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("B.Tag")) ==
         F.when(ns(F.col("B.Tag")) == F.lit(""), F.lit("-1"))
         .otherwise(F.col("L.Tag"))),
        "left"
    ).join(
        entity_rules.alias("ER"),
        F.col("ER.LineID") == F.col("L.LineID"),
        "left"
    ).select(
        _input_select_columns_pass2(cfg, "L", "K", "B", "ER",
                                    book_alloc_id, cost_alloc_id, offset_alloc_id,
                                    adj_lti, k1_lti)
    )

    combined = pass1.unionByName(pass2)

    log_timing("prepare_lookthrough_input", t0)
    return combined


def _input_select_columns_pass1(cfg, l_alias, k_alias, b_alias, er_alias,
                                ai_alias, book_alloc_id, cost_alloc_id,
                                offset_alloc_id, adj_lti, k1_lti):
    """Select columns for Pass 1 (with all_underlyings join).
    TypeID priority: B → Book→Cost → ER → AI → K
    IsExcludefromTransfer: ISNULL(B, AI.ExcludeFromTransfers)
    """
    return [
        F.col(f"{l_alias}.RunID"),
        F.col(f"{l_alias}.ClientID"),
        F.col(f"{l_alias}.EntityID"),
        F.col(f"{l_alias}.LineTypeID"),
        F.col(f"{l_alias}.LineID"),
        F.col(f"{l_alias}.Amount"),
        F.col(f"{l_alias}.QuicklinkID"),
        F.col(f"{l_alias}.Amount704b"),
        F.col(f"{l_alias}.CategoryID"),
        F.col(f"{l_alias}.ParentEntityID"),
        F.col(f"{l_alias}.PeriodID"),
        F.col(f"{l_alias}.LineCode"),
        F.col(f"{l_alias}.SuperParentEntityID"),
        F.col(f"{l_alias}.AdjustmentTypeID"),
        F.col(f"{l_alias}.TrackingKey"),
        F.coalesce(F.col(f"{l_alias}.Tag"), F.lit("")).alias("Tag"),
        F.col(f"{l_alias}.OriginalParentEntityID"),
        # TransactionDate: CASE WHEN IsTransactionDate=1 AND IsTransfersAdjusted=1
        F.when(
            (F.col(f"{k_alias}.IsTransactionDate") == True) &
            (F.col(f"{k_alias}.IsTransfersAdjusted") == True),
            F.col(f"{k_alias}.TransactionDate")
        ).otherwise(F.lit(None).cast("timestamp")).alias("TransactionDate"),
        # TypeID: ISNULL(B.AdjustmentAllocationTypeID,
        #   CASE WHEN K = Book THEN Cost
        #   ELSE ISNULL(ER, ISNULL(AI.AllocationTypeId, K)) END)
        F.when(F.col(f"{b_alias}.AdjustmentAllocationTypeID").isNotNull(),
               F.col(f"{b_alias}.AdjustmentAllocationTypeID"))
        .when(F.col(f"{k_alias}.AllocationTypeRuleId") == book_alloc_id,
              F.lit(cost_alloc_id).cast("long"))
        .otherwise(F.coalesce(
            F.col(f"{er_alias}.UpdatedAllocationRuleID"),
            F.col(f"{ai_alias}.AllocationTypeId"),
            F.col(f"{k_alias}.AllocationTypeRuleId")
        )).alias("TypeID"),
        # CustomTrackingKey: ISNULL(B.TrackingKey, ISNULL(L.TrackingKey, ''))
        F.coalesce(
            F.col(f"{b_alias}.TrackingKey"),
            ns(F.col(f"{l_alias}.TrackingKey"))
        ).alias("CustomTrackingKey"),
        # CustomTag: ISNULL(B.Tag, ISNULL(L.Tag, ''))
        F.coalesce(
            F.col(f"{b_alias}.Tag"),
            ns(F.col(f"{l_alias}.Tag"))
        ).alias("CustomTag"),
        # IsExcludefromTransfer: ISNULL(B.IsExcludefromTransfer, AI.ExcludeFromTransfers)
        F.coalesce(
            F.col(f"{b_alias}.IsExcludefromTransfer"),
            F.col(f"{ai_alias}.ExcludeFromTransfers").cast("boolean"),
        ).alias("IsExcludefromTransfer"),
        F.col(f"{k_alias}.Classification"),
        F.col(f"{k_alias}.CapitalGainLoss"),
    ]


def _input_select_columns_pass2(cfg, l_alias, k_alias, b_alias, er_alias,
                                book_alloc_id, cost_alloc_id,
                                offset_alloc_id, adj_lti, k1_lti):
    """Select columns for Pass 2 (no all_underlyings).
    TypeID priority: B → Book→Cost → ER → K
    IsExcludefromTransfer: ISNULL(B, CASE WHEN K=Offset THEN 1 ELSE 0 END)
    """
    return [
        F.col(f"{l_alias}.RunID"),
        F.col(f"{l_alias}.ClientID"),
        F.col(f"{l_alias}.EntityID"),
        F.col(f"{l_alias}.LineTypeID"),
        F.col(f"{l_alias}.LineID"),
        F.col(f"{l_alias}.Amount"),
        F.col(f"{l_alias}.QuicklinkID"),
        F.col(f"{l_alias}.Amount704b"),
        F.col(f"{l_alias}.CategoryID"),
        F.col(f"{l_alias}.ParentEntityID"),
        F.col(f"{l_alias}.PeriodID"),
        F.col(f"{l_alias}.LineCode"),
        F.col(f"{l_alias}.SuperParentEntityID"),
        F.col(f"{l_alias}.AdjustmentTypeID"),
        F.col(f"{l_alias}.TrackingKey"),
        F.coalesce(F.col(f"{l_alias}.Tag"), F.lit("")).alias("Tag"),
        F.col(f"{l_alias}.OriginalParentEntityID"),
        # TransactionDate
        F.when(
            (F.col(f"{k_alias}.IsTransactionDate") == True) &
            (F.col(f"{k_alias}.IsTransfersAdjusted") == True),
            F.col(f"{k_alias}.TransactionDate")
        ).otherwise(F.lit(None).cast("timestamp")).alias("TransactionDate"),
        # TypeID: ISNULL(B.AdjustmentAllocationTypeID,
        #   CASE WHEN K = Book THEN Cost ELSE ISNULL(ER, K) END)
        F.when(F.col(f"{b_alias}.AdjustmentAllocationTypeID").isNotNull(),
               F.col(f"{b_alias}.AdjustmentAllocationTypeID"))
        .when(F.col(f"{k_alias}.AllocationTypeRuleId") == book_alloc_id,
              F.lit(cost_alloc_id).cast("long"))
        .otherwise(F.coalesce(
            F.col(f"{er_alias}.UpdatedAllocationRuleID"),
            F.col(f"{k_alias}.AllocationTypeRuleId")
        )).alias("TypeID"),
        # CustomTrackingKey: ISNULL(B.TrackingKey, ISNULL(L.TrackingKey, ''))
        F.coalesce(
            F.col(f"{b_alias}.TrackingKey"),
            ns(F.col(f"{l_alias}.TrackingKey"))
        ).alias("CustomTrackingKey"),
        # CustomTag: ISNULL(B.Tag, ISNULL(L.Tag, ''))
        F.coalesce(
            F.col(f"{b_alias}.Tag"),
            ns(F.col(f"{l_alias}.Tag"))
        ).alias("CustomTag"),
        # IsExcludefromTransfer: ISNULL(B, CASE WHEN K=Offset THEN 1 ELSE 0 END)
        F.coalesce(
            F.col(f"{b_alias}.IsExcludefromTransfer"),
            F.when(F.col(f"{k_alias}.AllocationTypeRuleId") == offset_alloc_id,
                   F.lit(True))
            .otherwise(F.lit(False))
        ).alias("IsExcludefromTransfer"),
        F.col(f"{k_alias}.Classification"),
        F.col(f"{k_alias}.CapitalGainLoss"),
    ]


# ---------------------------------------------------------------------------
# Validation + offset handling
# ---------------------------------------------------------------------------

def validate_by_amount_allocations(spark: SparkSession, cfg: dict,
                                   input_data: DataFrame,
                                   final_eff_pct: DataFrame,
                                   partners: DataFrame,
                                   default_rules: DataFrame,
                                   map_rules: DataFrame) -> None:
    """Validate allocated amounts don't exceed input amounts.
    Writes to AllocationRunErrors if violation found (warning, not fatal).
    """
    log_section("validate_by_amount_allocations")
    t0 = time.time()
    prefix = table_prefix(cfg)
    run_id = cfg["run_id"]

    # Filter FEP for by-amount entries (EffPercentage=0, EffAmount != 0)
    fep_filtered = final_eff_pct.filter(
        (ns0(F.col("EffPercentage")) == 0) &
        (F.col("RunID") == run_id)
    )

    total_amounts = input_data.alias("L").join(
        fep_filtered.alias("T"),
        (ns0(F.col("L.EntityID")) == ns0(F.col("T.InvestmentID"))) &
        (F.col("L.TypeID") == F.col("T.TypeID")) &
        (F.col("L.CustomTrackingKey") == F.col("T.TrackingKey")) &
        (F.col("L.CustomTag") == F.col("T.Tag")) &
        (F.col("L.LineTypeID") == F.col("T.LineTypeID"))
    ).join(
        F.broadcast(partners).alias("P"),
        F.col("T.PartnerNumber") == F.col("P.PartnerNumber")
    ).join(
        F.broadcast(default_rules).alias("M"),
        F.col("L.TypeID") == F.col("M.RuleID")
    ).join(
        F.broadcast(map_rules).alias("D"),
        (F.col("D.TransactionID") == F.col("M.TransactionID")) &
        (F.when(F.col("M.EntityID") == -1, F.lit(1))
         .otherwise(F.col("M.EntityID")) ==
         F.when(F.col("M.EntityID") == -1, F.lit(1))
         .otherwise(F.col("D.EntityID"))) &
        (F.col("D.RuleID") == F.col("M.RuleID")) &
        (F.when(F.col("D.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("D.SelectedMappingID")) ==
         F.when(F.col("D.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("L.LineID"))) &
        (F.col("L.LineTypeID") == F.col("D.SourceID")) &
        # WARN-3 fix: exclude transfers per SQL
        # AND ISNULL(D.ExcludeFromTransfers, 0) = ISNULL(L.IsExcludefromTransfer, 0)
        (F.coalesce(F.col("D.ExcludeFromTransfers"), F.lit(0)) ==
         F.coalesce(F.col("L.IsExcludefromTransfer").cast("int"), F.lit(0)))
    ).join(
        F.broadcast(read_table(spark, "ENU_AllocationBy", cfg)).alias("EA"),
        (F.col("M.AllocationByID") == F.col("EA.AllocationByID")) &
        (F.lower(F.col("EA.AllocationBy")) == "amount")
    ).groupBy(
        "L.EntityID", "P.ShareClass", "L.LineTypeID", "L.LineID",
        "T.AllocationType", "L.QuicklinkID", "L.ParentEntityID",
        "L.SuperParentEntityID", "L.AdjustmentTypeID",
        "L.TrackingKey", "L.TypeID", "L.Tag", "L.Amount"
    ).agg(
        F.sum("T.EffAmount").alias("TotalAmount")
    ).filter(
        ns0(F.col("TotalAmount")) > F.col("L.Amount")
    )

    if not total_amounts.isEmpty():
        # Get rule names for error message
        rule_names = total_amounts.join(
            F.broadcast(read_table(spark, "ENU_CustomAllocations", cfg)).alias("EC"),
            F.col("TypeID") == F.col("EC.AllocationTypeID")
        ).select(F.col("EC.AllocationType")).distinct().collect()

        rules_str = ", ".join(r["AllocationType"] for r in rule_names)
        error_msg = (f"Allocated amounts are greater than input amounts "
                     f"for following rules: {rules_str}")

        # Write error to AllocationRunErrors
        fqn = f"{prefix}.AllocationRunErrors"
        run_id_val = cfg["run_id"]
        entity_id_val = cfg["entity_id"]
        spark.sql(f"""
            INSERT INTO {fqn}
                (RunID, EntityID, ErrorMessage, ErrorType)
            VALUES ({run_id_val}, {entity_id_val},
                    '{error_msg}', 'Warning')
        """)
        logger.warning(f"[VALIDATION] {error_msg}")

    log_timing("validate_by_amount_allocations", t0)


def handle_offset_types_704c(spark: SparkSession, cfg: dict,
                             k1_lineitems_704c: DataFrame,
                             allocation_percentages: DataFrame) -> DataFrame:
    """Handle LP/GP offset type inheritance for 704c.
    POSSIBLY-EMPTY: no offsets may exist.
    """
    log_section("handle_offset_types_704c")
    t0 = time.time()
    prefix = table_prefix(cfg)
    lp_offset_id = cfg["lp_offset_type_id"]
    gp_offset_id = cfg["gp_offset_type_id"]
    c704_alloc_id = cfg["c704_allocation_type_id"]

    # Check if any offset types exist
    has_offsets = not k1_lineitems_704c.filter(
        F.col("AllocationTypeRuleId").isin([lp_offset_id, gp_offset_id])
    ).isEmpty()

    if not has_offsets:
        log_timing("handle_offset_types_704c", t0)
        return k1_lineitems_704c

    # Get offset line mappings via MAP_DerivedLines
    offset_mappings = k1_lineitems_704c.alias("K").filter(
        F.col("AllocationTypeRuleId").isin([lp_offset_id, gp_offset_id])
    ).join(
        F.broadcast(read_table(spark, "MAP_DerivedLines", cfg)).alias("MD"),
        (F.col("K.LineID") == F.col("MD.DerivedLineID")) &
        (F.col("MD.BaseLineID").isNotNull())
    ).join(
        k1_lineitems_704c.alias("K1"),
        F.col("MD.BaseLineID") == F.col("K1.LineID")
    ).select(
        F.col("K.LineID").alias("OffsetLineID"),
        F.col("MD.BaseLineID").alias("ParentLineID"),
        F.col("K1.AllocationTypeRuleId").alias("ParentAllocType"),
        F.col("K1.EntityId"),
        F.col("K1.TrackingKey"),
    )

    # Update offset lines: inherit parent's allocation type if parent has percentages
    updated = k1_lineitems_704c.alias("K").join(
        offset_mappings.alias("OM"),
        (F.col("K.LineID") == F.col("OM.OffsetLineID")) &
        (F.col("OM.EntityId") == F.col("K.EntityId")) &
        (F.col("OM.TrackingKey") == F.col("K.TrackingKey")),
        "left"
    ).join(
        allocation_percentages.alias("T"),
        (F.col("OM.ParentAllocType") == F.col("T.TypeID")) &
        (F.col("OM.EntityId") == F.col("T.InvestmentID")),
        "left"
    ).select(
        F.col("K.EntityId"),
        F.col("K.TrackingKey"),
        F.col("K.LineID"),
        F.col("K.Classification"),
        F.col("K.CapitalGainLoss"),
        F.when(
            F.col("OM.OffsetLineID").isNotNull(),
            F.when(F.col("T.TypeID").isNull(), F.lit(c704_alloc_id).cast("long"))
            .otherwise(F.col("OM.ParentAllocType"))
        ).otherwise(F.col("K.AllocationTypeRuleId")).alias("AllocationTypeRuleId"),
        F.col("K.LineTypeID"),
    )

    log_timing("handle_offset_types_704c", t0)
    return updated


# ---------------------------------------------------------------------------
# Core allocation processors
# ---------------------------------------------------------------------------

def process_by_amount_allocation(spark: SparkSession, cfg: dict,
                                 input_data: DataFrame,
                                 final_eff_pct: DataFrame,
                                 partners: DataFrame,
                                 map_rules: DataFrame) -> DataFrame:
    """Process allocation by amount — FEP rows with EffPercentage=0, EffAmount!=0.
    POSSIBLY-EMPTY: may have no by-amount rules.
    """
    log_section("process_by_amount_allocation")
    t0 = time.time()
    run_id = cfg["run_id"]

    fep_by_amount = final_eff_pct.filter(
        (ns0(F.col("EffPercentage")) == 0) &
        (ns0(F.col("EffAmount")) != 0)
    )

    by_amount = input_data.alias("L").join(
        fep_by_amount.alias("T"),
        (ns0(F.col("L.EntityID")) == ns0(F.col("T.InvestmentID"))) &
        (F.col("L.TypeID") == F.col("T.TypeID")) &
        (F.col("L.CustomTrackingKey") == F.col("T.TrackingKey")) &
        (F.col("L.CustomTag") == F.col("T.Tag")) &
        (F.col("L.LineTypeID") == F.col("T.LineTypeID"))
    ).join(
        F.broadcast(partners).alias("P"),
        F.col("T.PartnerNumber") == F.col("P.PartnerNumber")
    ).join(
        map_rules.alias("D"),
        (F.when(F.col("D.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("D.SelectedMappingID")) ==
         F.when(F.col("D.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("L.LineID"))) &
        (F.col("L.LineTypeID") == F.col("D.SourceID")) &
        (F.col("L.TypeID") == F.col("D.RuleID"))
    ).select(
        F.col("L.EntityID"),
        F.col("P.ShareClass"),
        F.col("T.PartnerNumber"),
        F.col("L.LineTypeID"),
        F.col("L.LineID"),
        F.col("T.EffAmount").alias("Amount"),
        F.col("T.AllocationType"),
        F.col("L.QuicklinkID"),
        F.col("T.EffAmount").alias("Amount704b"),
        F.col("L.ParentEntityID"),
        F.col("L.SuperParentEntityID"),
        F.col("L.AdjustmentTypeID"),
        F.col("L.TrackingKey"),
        F.col("L.TypeID"),
        F.col("L.Tag"),
        F.col("L.OriginalParentEntityID"),
    )

    warn_possibly_empty(by_amount, "process_by_amount_allocation",
                        "No by-amount rules configured — may be expected")
    log_timing("process_by_amount_allocation", t0)
    return by_amount


def process_by_percentage_allocation(spark: SparkSession, cfg: dict,
                                     input_data: DataFrame,
                                     final_eff_pct: DataFrame,
                                     partners: DataFrame) -> DataFrame:
    """Process allocation by percentage — Amount * EffPercentage.
    Handles dated transfer variant (QuarterDates) vs standard (QuarterMonth lookup).
    ALWAYS-NON-EMPTY for non-704c line types.
    """
    log_section("process_by_percentage_allocation")
    t0 = time.time()
    prefix = table_prefix(cfg)
    alloc_type_name = cfg.get("allocation_type_name", "")
    is_dated = cfg.get("is_dated_transfers", False)

    cost_alloc_types = [
        "Cost", "CostAdjustedDatedTransfer", "ProRata",
        "DEFAULT", "DefaultAdjustedDatedTransfer",
        "Cost without Transfer Adj %"
    ]

    fep_pct = final_eff_pct.filter(
        F.lower(F.col("AllocationType")).isin([t.lower() for t in cost_alloc_types])
    )

    if alloc_type_name == "PE Book Allocation" and is_dated:
        # Dated transfers: join via QuarterDates start date
        by_pct = input_data.alias("L").join(
            read_table(spark, "QuarterDates", cfg).alias("D"),
            F.coalesce(F.col("L.TransactionDate"),
                       F.lit("1900-01-01").cast("timestamp"))
            == F.col("D.StartDate"),
            "left"
        ).join(
            fep_pct.alias("T"),
            (ns0(F.col("L.EntityID")) == ns0(F.col("T.InvestmentID"))) &
            (F.col("T.Quarter") == F.col("D.Quarter")) &
            (F.col("T.TypeID") == F.col("L.TypeID")) &
            (F.col("L.CustomTrackingKey") == F.col("T.TrackingKey")) &
            (F.col("L.CustomTag") == F.col("T.Tag")) &
            (F.col("L.IsExcludefromTransfer") == F.col("T.IsExcludefromTransfer")) &
            (F.col("L.LineTypeID") == F.col("T.LineTypeID"))
        ).join(
            F.broadcast(partners).alias("P"),
            F.col("T.PartnerNumber") == F.col("P.PartnerNumber")
        ).select(
            F.col("L.EntityID"),
            F.col("P.ShareClass"),
            F.col("T.PartnerNumber"),
            F.col("L.LineTypeID"),
            F.col("L.LineID"),
            (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount"),
            F.col("T.AllocationType"),
            F.col("L.QuicklinkID"),
            (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount704b"),
            F.col("L.ParentEntityID"),
            F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"),
            F.col("L.TypeID").alias("TypeID"),
            F.col("L.Tag"),
            F.col("L.OriginalParentEntityID"),
        )
    else:
        # Standard: join via ENU_DF_DataList QuarterMonth mapping
        by_pct = input_data.alias("L").join(
            read_table(spark, "ENU_DF_DataList", cfg).alias("D").filter(
                F.lower(F.col("Category")) == "quartermonth"
            ),
            ns0(F.month(F.col("L.TransactionDate"))) == F.col("D.LookUpValue")
        ).join(
            fep_pct.alias("T"),
            (ns0(F.col("L.EntityID")) == ns0(F.col("T.InvestmentID"))) &
            (F.col("T.Quarter") == F.col("D.LookUpData")) &
            (F.col("T.TypeID") == F.col("L.TypeID")) &
            (F.col("L.CustomTrackingKey") == F.col("T.TrackingKey")) &
            (F.col("L.CustomTag") == F.col("T.Tag")) &
            (F.col("L.IsExcludefromTransfer") == F.col("T.IsExcludefromTransfer")) &
            (F.col("L.LineTypeID") == F.col("T.LineTypeID"))
        ).join(
            F.broadcast(partners).alias("P"),
            F.col("T.PartnerNumber") == F.col("P.PartnerNumber")
        ).select(
            F.col("L.EntityID"),
            F.col("P.ShareClass"),
            F.col("T.PartnerNumber"),
            F.col("L.LineTypeID"),
            F.col("L.LineID"),
            (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount"),
            F.col("T.AllocationType"),
            F.col("L.QuicklinkID"),
            (F.col("L.Amount") * F.col("T.EffPercentage")).alias("Amount704b"),
            F.col("L.ParentEntityID"),
            F.col("L.SuperParentEntityID"),
            F.col("L.AdjustmentTypeID"),
            F.col("L.TrackingKey"),
            F.col("L.TypeID").alias("TypeID"),
            F.col("L.Tag"),
            F.col("L.OriginalParentEntityID"),
        )

    log_timing("process_by_percentage_allocation", t0)
    return by_pct


def process_704c_allocation(spark: SparkSession, cfg: dict,
                            input_data: DataFrame,
                            final_eff_pct: DataFrame,
                            allocation_percentages: DataFrame,
                            k1_lineitems_704c: DataFrame) -> DataFrame:
    """Process 704c allocation with Classification-based percentage selection.
    Ordinary → OrdinaryPercentage, Capital Gain → CapitalGain, etc.
    ALWAYS-NON-EMPTY for 704c line type.
    """
    log_section("process_704c_allocation")
    t0 = time.time()
    adj_lti = cfg["adjustment_line_type_id"]
    cost_alloc_id = cfg["cost_allocation_type_id"]
    is_sep_gl = cfg.get("is_separate_gains_loss", False)

    # FAIL-4 fix: extract the amount expression so it can be reused for
    # Amount704b (was hard-coded NULL). FAIL-4 also requires the same
    # percentage-driven amount be written to the Amount704b column for 704c.
    _pct_expr = (
        F.when(F.lower(F.col("K1.Classification")) == "ordinary",
               ns0(F.col("AP.OrdinaryPercentage")))
        .when((F.lower(ns(F.col("K1.Classification"))) != "ordinary") &
              (F.lit(is_sep_gl) == False),
              ns0(F.col("AP.CapitalPercentage")))
        .when((F.lower(F.col("K1.Classification")) == "capital") &
              (F.lower(F.col("K1.CapitalGainLoss")) == "capital gain"),
              ns0(F.col("AP.CapitalGainPercentage")))
        .when((F.lower(F.col("K1.Classification")) == "capital") &
              (F.lower(F.col("K1.CapitalGainLoss")) == "capital loss"),
              ns0(F.col("AP.CapitalLossPercentage")))
        .otherwise(
            F.when(F.col("AI.Amount") > 0, F.col("AP.CapitalGainPercentage"))
            .otherwise(F.col("AP.CapitalLossPercentage"))
        )
    )
    _amount_expr = F.col("AI.Amount") * _pct_expr

    c704_alloc = input_data.alias("AI").join(
        allocation_percentages.alias("AP"),
        (ns0(F.col("AI.EntityID")) == ns0(F.col("AP.InvestmentID"))) &
        (F.col("AI.RunID") == F.col("AP.RunID")) &
        # WARN-4 fix: ClientID equality is part of the SQL ON clause
        (F.col("AI.ClientID") == F.col("AP.ClientID")) &
        (ns(F.col("AI.TrackingKey")) == ns(F.col("AP.TrackingKey"))) &
        (F.when(F.col("AI.LineTypeID") == adj_lti, F.col("AP.LineTypeID"))
         .otherwise(F.col("AI.LineTypeID")) == F.col("AP.LineTypeID"))
    ).join(
        read_table(spark, "ENU_CustomAllocations", cfg).alias("EC"),
        F.col("AP.TypeID") == F.col("EC.AllocationTypeID")
    ).join(
        k1_lineitems_704c.alias("K1"),
        (F.col("AI.LineID") == F.col("K1.LineID")) &
        (F.coalesce(F.col("K1.AllocationTypeRuleId"),
                    F.lit(cost_alloc_id).cast("long")) == F.col("AP.TypeID")) &
        (F.col("K1.LineTypeID") == F.col("AI.LineTypeID")) &
        (ns0(F.col("K1.EntityId")) == ns0(F.col("AI.EntityID"))) &
        (ns(F.col("AI.TrackingKey")) == ns(F.col("K1.TrackingKey")))
    ).select(
        F.col("AI.EntityID"),
        F.lit("").alias("ShareClass"),
        F.col("AP.PartnerNumber"),
        F.col("AI.LineTypeID"),
        F.col("AI.LineID"),
        # Amount = input * classification-based percentage
        _amount_expr.alias("Amount"),
        # AllocationType naming
        (F.when(F.lower(F.col("K1.Classification")) == "ordinary",
                F.concat(ns(F.col("EC.AllocationType")),
                         F.lit(" Ordinary Percentage")))
         .when((F.lower(ns(F.col("K1.Classification"))) != "ordinary") &
               (F.lit(is_sep_gl) == False),
               F.concat(ns(F.col("EC.AllocationType")),
                        F.lit(" Capital Percentage")))
         .when((F.lower(F.col("K1.Classification")) == "capital") &
               (F.lower(F.col("K1.CapitalGainLoss")) == "capital gain"),
               F.concat(ns(F.col("EC.AllocationType")),
                        F.lit(" Capital Gain Percentage")))
         .when((F.lower(F.col("K1.Classification")) == "capital") &
               (F.lower(F.col("K1.CapitalGainLoss")) == "capital loss"),
               F.concat(ns(F.col("EC.AllocationType")),
                        F.lit(" Capital Loss Percentage")))
         .otherwise(
             F.when(F.col("AI.Amount") > 0,
                    F.concat(ns(F.col("EC.AllocationType")),
                             F.lit(" Capital Gain Percentage")))
             .otherwise(F.concat(ns(F.col("EC.AllocationType")),
                                 F.lit(" Capital Loss Percentage")))
         )).alias("AllocationType"),
        F.col("AI.QuicklinkID"),
        # FAIL-4 fix: Amount704b = same percentage-driven amount (was NULL).
        # SQL line 704 writes the computed Amount into both Amount and
        # Amount704b for 704c allocations.
        _amount_expr.alias("Amount704b"),
        F.col("AI.ParentEntityID"),
        F.col("AI.SuperParentEntityID"),
        F.col("AI.AdjustmentTypeID"),
        F.col("AI.TrackingKey"),
        F.col("AP.TypeID"),
        F.col("AI.Tag"),
        F.col("AI.OriginalParentEntityID"),
    )

    log_timing("process_704c_allocation", t0)
    return c704_alloc


# ---------------------------------------------------------------------------
# FAIL-5 fix: 704c By-Amount mapped-line insert (SQL lines 933-957).
# ---------------------------------------------------------------------------

def insert_704c_by_amount_mapped_lines(spark: SparkSession, cfg: dict,
                                       input_data: DataFrame,
                                       distinct_mappings: DataFrame,
                                       all_underlyings: DataFrame,
                                       default_rules: DataFrame,
                                       book_effective: DataFrame) -> DataFrame:
    """FAIL-5 fix: SQL port of the 704c CAR By-Amount mapped-line insert
    (SQL lines 933-957).

    SQL:
        IF (@IsCustomAllocationRuleEnabled = 'C'
            AND EXISTS(#CostPercentage704cValues)
            AND EXISTS(#DefaultAllocationRuleSetup WHERE AllocationByID = AMOUNT))
        BEGIN
            INSERT INTO #tmpLookThroughAllocationInput (...)
            SELECT ..., AI.AllocationTypeID AS TypeID, ...
            FROM #tmpLookThroughAllocationInput I
            INNER JOIN #DistinctMappings M
              ON M.RegisterLineID = I.LineID AND I.LineTypeID = M.FieldSourceID
            INNER JOIN #TempAllUnderlyings AI
              ON I.EntityID=AI.UnderlyingEntityID AND I.TrackingKey=AI.TrackingKey
                 AND I.LineID=AI.LineID AND AI.LineTypeID=I.LineTypeID
            INNER JOIN #DefaultAllocationRuleSetup R
              ON R.RuleID = AI.AllocationTypeID
                 AND R.UnderlyingTypeID = AI.UnderlyingType
            INNER JOIN #TempBookEffectiveValues B
              ON B.UnderlyingEntityID=I.EntityID AND B.LineID=I.LineID
                 AND ISNULL(B.TrackingKey,'') ... = ISNULL(B.TrackingKey,'') ... I.TrackingKey
                 AND ISNULL(B.Tag,'') ... = ISNULL(B.Tag,'') ... I.Tag
            WHERE R.AllocationByID = @AllocationByAmountTypeID
              AND B.SourceID = @K1LineTypeID
              AND ISNULL(B.AdjustmentAllocationTypeID, 0) <> 0
    """
    log_section("insert_704c_by_amount_mapped_lines")
    t0 = time.time()

    # Resolve scalar IDs once
    by_amount_id = cfg.get("allocation_by_amount_type_id")
    if by_amount_id is None:
        # Fall back to looking it up
        prefix = table_prefix(cfg)
        r = spark.sql(
            f"SELECT AllocationByID FROM {prefix}.ENU_AllocationBy "
            f"WHERE AllocationBy = 'AMOUNT'"
        ).first()
        by_amount_id = r["AllocationByID"] if r else 0
    k1_lti = cfg["k1_line_type_id"]

    # SQL EXISTS(... DefaultAllocationRuleSetup WHERE AllocationByID = AMOUNT)
    # Use take(1) — short-circuits after first row, no aggregation shuffle.
    has_by_amount = bool(
        default_rules.filter(F.col("AllocationByID") == by_amount_id).take(1)
    )
    if distinct_mappings is None or not has_by_amount:
        log_timing("insert_704c_by_amount_mapped_lines", t0)
        return input_data

    # Build the additional rows
    additional = input_data.alias("I").join(
        F.broadcast(distinct_mappings.alias("M")),
        (F.col("M.RegisterLineID") == F.col("I.LineID")) &
        (F.col("I.LineTypeID") == F.col("M.FieldSourceID")),
    ).join(
        all_underlyings.alias("AI"),
        (F.col("I.EntityID") == F.col("AI.UnderlyingEntityID")) &
        (F.col("I.TrackingKey") == F.col("AI.TrackingKey")) &
        (F.col("I.LineID") == F.col("AI.LineID")) &
        (F.col("AI.LineTypeID") == F.col("I.LineTypeID")),
    ).join(
        F.broadcast(default_rules.alias("R")),
        (F.col("R.RuleID") == F.col("AI.AllocationTypeID")) &
        (F.col("R.UnderlyingTypeID") == F.col("AI.UnderlyingType")) &
        (F.col("R.AllocationByID") == F.lit(by_amount_id)),
    ).join(
        book_effective.alias("B"),
        (F.col("B.UnderlyingEntityID") == F.col("I.EntityID")) &
        (F.col("B.LineID") == F.col("I.LineID")) &
        (F.col("B.SourceID") == F.lit(k1_lti)) &
        (F.coalesce(F.col("B.AdjustmentAllocationTypeID"), F.lit(0)) != 0) &
        # SQL CASE-WHEN-NULL alignment for TrackingKey and Tag
        (F.when(F.coalesce(F.col("B.TrackingKey"), F.lit("")) == "",
                F.lit("-1"))
         .otherwise(F.col("B.TrackingKey")) ==
         F.when(F.coalesce(F.col("B.TrackingKey"), F.lit("")) == "",
                F.lit("-1"))
         .otherwise(F.col("I.TrackingKey"))) &
        (F.when(F.coalesce(F.col("B.Tag"), F.lit("")) == "", F.lit("-1"))
         .otherwise(F.col("B.Tag")) ==
         F.when(F.coalesce(F.col("B.Tag"), F.lit("")) == "", F.lit("-1"))
         .otherwise(F.col("I.Tag"))),
    ).select(
        # SQL: same columns as #tmpLookThroughAllocationInput; TypeID is
        # overridden to AI.AllocationTypeID (the new rule's allocation type).
        F.col("I.RunID"), F.col("I.ClientID"), F.col("I.EntityID"),
        F.col("I.LineTypeID"), F.col("I.LineID"), F.col("I.Amount"),
        F.col("I.QuicklinkID"), F.col("I.Amount704b"),
        F.col("I.CategoryID"), F.col("I.ParentEntityID"),
        F.col("I.PeriodID"), F.col("I.LineCode"),
        F.col("I.SuperParentEntityID"), F.col("I.AdjustmentTypeID"),
        F.col("I.TrackingKey"), F.col("I.Tag"), F.col("I.TransactionDate"),
        F.col("AI.AllocationTypeID").alias("TypeID"),
        F.col("I.CustomTrackingKey"), F.col("I.CustomTag"),
        F.col("I.IsExcludefromTransfer"),
        F.col("I.OriginalParentEntityID"),
        F.col("I.Classification"), F.col("I.CapitalGainLoss"),
    )

    result = input_data.unionByName(additional, allowMissingColumns=True)
    log_timing("insert_704c_by_amount_mapped_lines", t0)
    return result
