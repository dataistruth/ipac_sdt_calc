"""Updated-only reimplementations that insert lineage breaks into two
plan-heavy production builders.

On small inputs the dominant cost of ``build_entity_hierarchy`` and
``build_allocation_input`` is Catalyst analysis + whole-stage codegen of very
large logical plans (an 8-level unrolled union tree, and a 5-pass anti-join
accumulation), not data movement. These copies preserve the production logic
byte-for-byte and only add ``checkpoint`` calls at the plan-explosion seams so
each half of the plan is analyzed/codegen'd on a materialized, small table.

Business semantics are identical to the production functions in
``..underlyings`` and ``..allocation_input`` — verified by the benchmark's
row-count, amount-sum, and xxhash64 fingerprint parity checks.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.helpers import ns, ns0, read_table

from ..underlyings import _build_asset_class_relationship
from .checkpoint import checkpoint
from .plan_profiler import track_plan

logger = logging.getLogger(__name__)

# Number of entity-hierarchy levels to unroll. Must match the production
# ``MAX_DEPTH`` in ``..underlyings.build_entity_hierarchy``.
_MAX_DEPTH = 8


@track_plan
def build_entity_hierarchy(
    spark: SparkSession,
    cfg: dict,
    df_cost_pct_snapshot: DataFrame,
    df_temp_cost_underlying_types: DataFrame,
) -> tuple:
    """Production ``build_entity_hierarchy`` with one lineage break.

    The only change from production is a ``checkpoint`` of ``all_levels`` right
    after the depth loop, so the join-back + distinct + four unions downstream
    are planned against a materialized table instead of the fully unrolled
    8-level union tree.
    """
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]

    entity_rel = F.broadcast(
        read_table(spark, "EntityRelationship", cfg)
        .filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select("UpperTierEntityID", "LowerTierEntityID")
    )

    df_asset_class_rel = _build_asset_class_relationship(spark, cfg)

    tc = df_temp_cost_underlying_types.alias("TC")

    anchor_entity = tc.withColumn(
        "_join_entity",
        F.when(
            F.lower(F.col("EntityUnderlyingtype")) == "asset class",
            F.col("EntityId"),
        ).otherwise(F.col("InvestmentID")),
    )

    anchor = anchor_entity.join(
        entity_rel,
        anchor_entity["_join_entity"] == entity_rel["UpperTierEntityID"],
        "inner",
    ).select(
        entity_rel["LowerTierEntityID"],
        entity_rel["UpperTierEntityID"].alias("ParentEntityID"),
        entity_rel["UpperTierEntityID"].alias("CurrentEntityId"),
        F.lit(2).alias("HLevel"),
        anchor_entity["AllocationTypeId"],
        F.concat(
            F.lit("~"),
            F.when(
                F.lower(anchor_entity["EntityUnderlyingtype"]) == "asset class",
                F.concat(
                    entity_rel["LowerTierEntityID"].cast("string"), F.lit("~")
                ),
            ).otherwise(
                F.when(
                    ns(anchor_entity["TrackingKey"], F.lit("")) == "",
                    F.concat(
                        anchor_entity["InvestmentID"].cast("string"),
                        F.lit("~"),
                    ),
                ).otherwise(
                    F.concat(anchor_entity["TrackingKey"], F.lit("~"))
                )
            ),
        ).alias("TrackingKey"),
        anchor_entity["InvestmentID"].alias("AssetClassId"),
        entity_rel["LowerTierEntityID"].alias("ImmediateLowerTierEntityID"),
    )

    all_levels = anchor
    current_level = anchor
    for depth in range(_MAX_DEPTH):
        er = entity_rel.alias(f"er_cte_{depth}")
        next_level = current_level.join(
            er,
            current_level["LowerTierEntityID"]
            == F.col(f"er_cte_{depth}.UpperTierEntityID"),
            "inner",
        ).select(
            F.col(f"er_cte_{depth}.LowerTierEntityID").alias("LowerTierEntityID"),
            F.col(f"er_cte_{depth}.UpperTierEntityID").alias("ParentEntityID"),
            current_level["CurrentEntityId"],
            (current_level["HLevel"] + 1).alias("HLevel"),
            current_level["AllocationTypeId"],
            current_level["TrackingKey"],
            current_level["AssetClassId"],
            current_level["ImmediateLowerTierEntityID"],
        )
        all_levels = all_levels.unionByName(next_level)
        current_level = next_level

    # LINEAGE BREAK: materialize the unrolled level tree so the downstream
    # join-back + distinct + four unions are planned against a small table.
    all_levels = checkpoint(spark, all_levels, "entity_levels", cfg)

    cte_result = (
        all_levels.alias("EH")
        .join(
            anchor_entity.alias("TC2"),
            (F.col("EH.CurrentEntityId") == F.col("TC2._join_entity"))
            & (F.col("TC2.AllocationTypeId") == F.col("EH.AllocationTypeId"))
            & (F.col("TC2.InvestmentID") == F.col("EH.AssetClassId")),
            "inner",
        )
        .select(
            F.col("EH.LowerTierEntityID").alias("UnderlyingEntityId"),
            F.col("EH.CurrentEntityId").alias("EntityId"),
            F.col("EH.HLevel"),
            F.col("TC2.Underlyingtype"),
            F.col("TC2.AllocationTypeId"),
            F.col("EH.TrackingKey"),
            F.col("EH.AssetClassId"),
            F.col("EH.ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    k1_only = (
        df_cost_pct_snapshot.filter(
            F.lower(F.col("EntityUnderlyingtype")) == "k-1 only"
        )
        .select(
            F.col("InvestmentID").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.when(
                ns(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(
                    F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")
                ),
            )
            .otherwise(F.col("TrackingKey"))
            .alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    k1_self = (
        df_cost_pct_snapshot.filter(
            (F.lower(F.col("EntityUnderlyingtype")) == "k-1 only")
            & (F.col("InvestmentID") == -1)
        )
        .select(
            F.lit(entity_id).alias("UnderlyingEntityId"),
            F.lit(entity_id).alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.concat(
                F.lit("~"), F.lit(entity_id).cast("string"), F.lit("~")
            ).alias("TrackingKey"),
            F.lit(entity_id).alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )

    asset_class_union = (
        df_temp_cost_underlying_types.filter(
            F.lower(F.col("EntityUnderlyingtype")) == "asset class"
        ).select(
            F.col("EntityId").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.concat(
                F.lit("~"), F.col("EntityId").cast("string"), F.lit("~")
            ).alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.col("EntityId").alias("ImmediateLowerTierEntityID"),
        )
    )

    entity_total_union = (
        df_temp_cost_underlying_types.filter(
            F.lower(F.col("EntityUnderlyingtype")) == "entity total"
        ).select(
            F.col("InvestmentID").alias("UnderlyingEntityId"),
            F.col("InvestmentID").alias("EntityId"),
            F.lit(1).alias("HLevel"),
            F.col("Underlyingtype"),
            F.col("AllocationTypeId"),
            F.when(
                ns(F.col("TrackingKey"), F.lit("")) == "",
                F.concat(
                    F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")
                ),
            )
            .otherwise(F.col("TrackingKey"))
            .alias("TrackingKey"),
            F.col("InvestmentID").alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
    )

    df_all = (
        cte_result.unionByName(k1_only)
        .unionByName(k1_self)
        .unionByName(asset_class_union)
        .unionByName(entity_total_union)
    )

    return df_all, df_asset_class_rel


@track_plan
def build_allocation_input(
    spark: SparkSession,
    cfg: dict,
    df_temp_alloc_input: DataFrame,
    df_temp_book_eff: DataFrame,
    df_underlyings_fn: DataFrame,
) -> DataFrame:
    """Production ``build_allocation_input`` with per-pass lineage breaks.

    The only change from production is a ``checkpoint`` of ``remaining_input``
    after each pass's left-anti delete, so the accumulating anti-join chain is
    truncated and every later pass / the final union is planned against a
    materialized table.
    """
    at_risk_lt = cfg["at_risk_line_type_id"]
    k1_lt = cfg["k1_line_type_id"]
    pfic_lt = cfg["pfic_footnote_line_type_id"]
    cost_at = cfg["cost_allocation_type_id"]
    lp_offset_at = cfg["lp_offset_allocation_type_id"]
    gp_offset_at = cfg["gp_offset_allocation_type_id"]

    pfic_li = F.broadcast(
        cfg["_df_pfic_footnote_line_item"].select(
            F.col("LineID").alias("pli_LineID"),
            F.col("LineDescription").alias("pli_desc"),
        )
    )
    k1_li = F.broadcast(
        read_table(spark, "K1Lineitem", cfg).select(
            F.col("LineID").alias("k1_LineID"),
            F.col("LineDescription").alias("k1_desc"),
        )
    )

    remaining_input = df_temp_alloc_input
    remaining_book = F.broadcast(df_temp_book_eff)
    all_inserts = []

    def _ns_match(b_col, i_col):
        return (
            F.when(ns(b_col) == "", F.lit("-1")).otherwise(b_col)
            == F.when(ns(b_col) == "", F.lit("-1")).otherwise(i_col)
        )

    def _type_id_expr(
        b_adj, bk_adj, ai_alloc, line_type_col, desc_col, is_at_risk=False
    ):
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
            )
            .when(
                (line_type_col == lt_ref)
                & (F.lower(ns(desc_col)).endswith("- gp - offset")),
                F.lit(gp_offset_at),
            )
            .otherwise(F.lit(cost_at)),
        )

    def _delete_matched(current, insert_df, alias_name):
        return current.join(
            insert_df.select(
                "EntityID", "LineID", "LineTypeID", "QuicklinkID", "TrackingKey"
            )
            .distinct()
            .alias(alias_name),
            (current["EntityID"] == F.col(f"{alias_name}.EntityID"))
            & (current["LineID"] == F.col(f"{alias_name}.LineID"))
            & (current["LineTypeID"] == F.col(f"{alias_name}.LineTypeID"))
            & (current["QuicklinkID"] == F.col(f"{alias_name}.QuicklinkID"))
            & (current["TrackingKey"] == F.col(f"{alias_name}.TrackingKey")),
            "left_anti",
        )

    # ── PASS 1: At Risk lines with BookEffective (FootNoteID<>-1, LineID<>-1) ──
    be_p1 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1)
        & (ns0(F.col("LineID")) != -1)
        & (F.col("SourceID") == at_risk_lt)
    ).alias("B1")

    be_k1 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1)
        & (ns0(F.col("LineID")) != -1)
        & (F.col("SourceID") == k1_lt)
    ).alias("BK1")

    input_atrisk = remaining_input.filter(
        F.col("LineTypeID") == at_risk_lt
    ).alias("I1")

    p1_joined = (
        input_atrisk.join(
            k1_li, F.col("I1.LineID") == k1_li["k1_LineID"], "inner"
        )
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
            & (
                (ns0(F.col("B1.LineID")) != -1)
                | (F.col("AI1.LineID").isNotNull())
                | (ns0(F.col("BK1.LineID")) != -1)
            )
        )
    )

    p1_insert = p1_joined.select(
        F.col("I1.RunID"),
        F.col("I1.ClientID"),
        F.col("I1.EntityID"),
        F.col("I1.LineTypeID"),
        F.col("I1.LineID"),
        F.col("I1.Amount"),
        F.col("I1.QuicklinkID"),
        F.col("I1.Amount704b"),
        F.col("I1.CategoryID"),
        F.col("I1.PeriodID"),
        F.col("I1.LineCode"),
        F.col("I1.ParentEntityID"),
        F.col("I1.SuperParentEntityID"),
        _type_id_expr(
            F.col("B1.AdjustmentAllocationTypeID"),
            F.col("BK1.AdjustmentAllocationTypeID"),
            F.col("AI1.AllocationTypeId"),
            F.col("I1.LineTypeID"),
            F.col("k1_desc"),
            is_at_risk=True,
        ).alias("TypeID"),
        ns(F.col("I1.Tag"), F.lit("")).alias("Tag"),
        F.coalesce(
            F.col("B1.IsExcludefromTransfer").cast("int"),
            F.col("AI1.ExcludeFromTransfers"),
            F.lit(0),
        ).alias("IsExcludefromTransfer"),
        F.coalesce(
            F.col("B1.TrackingKey"), F.col("I1.TrackingKey"), F.lit("")
        ).alias("TrackingKey"),
        F.col("I1.Quarter"),
        F.col("I1.SchID"),
        F.col("I1.OriginalParentEntityID"),
    ).distinct()
    all_inserts.append(p1_insert)

    remaining_input = _delete_matched(remaining_input, p1_insert, "del1")
    remaining_book = remaining_book.filter(
        ~(
            (F.col("SourceID") == at_risk_lt)
            & (ns0(F.col("FootNoteID")) != -1)
            & (ns0(F.col("LineID")) != -1)
        )
    )
    # LINEAGE BREAK: truncate the anti-join chain after pass 1.
    remaining_input = checkpoint(spark, remaining_input, "alloc_pass1", cfg)

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

    p2_insert = p2_joined.select(
        F.col("I2.RunID"),
        F.col("I2.ClientID"),
        F.col("I2.EntityID"),
        F.col("I2.LineTypeID"),
        F.col("I2.LineID"),
        F.col("I2.Amount"),
        F.col("I2.QuicklinkID"),
        F.col("I2.Amount704b"),
        F.col("I2.CategoryID"),
        F.col("I2.PeriodID"),
        F.col("I2.LineCode"),
        F.col("I2.ParentEntityID"),
        F.col("I2.SuperParentEntityID"),
        _type_id_expr(
            F.col("B2.AdjustmentAllocationTypeID"),
            None,
            F.col("AI2.AllocationTypeId"),
            F.col("I2.LineTypeID"),
            F.col("pli_desc"),
            is_at_risk=False,
        ).alias("TypeID"),
        ns(F.col("I2.Tag"), F.lit("")).alias("Tag"),
        F.coalesce(
            F.col("B2.IsExcludefromTransfer").cast("int"),
            F.col("AI2.ExcludeFromTransfers"),
            F.lit(0),
        ).alias("IsExcludefromTransfer"),
        F.coalesce(
            F.col("B2.TrackingKey"), F.col("I2.TrackingKey"), F.lit("")
        ).alias("TrackingKey"),
        F.col("I2.Quarter"),
        F.col("I2.SchID"),
        F.col("I2.OriginalParentEntityID"),
    ).distinct()
    all_inserts.append(p2_insert)

    remaining_input = _delete_matched(remaining_input, p2_insert, "del2")
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) != -1))
    )
    remaining_input = checkpoint(spark, remaining_input, "alloc_pass2", cfg)

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

    p3_insert = p3_joined.select(
        F.col("I3.RunID"),
        F.col("I3.ClientID"),
        F.col("I3.EntityID"),
        F.col("I3.LineTypeID"),
        F.col("I3.LineID"),
        F.col("I3.Amount"),
        F.col("I3.QuicklinkID"),
        F.col("I3.Amount704b"),
        F.col("I3.CategoryID"),
        F.col("I3.PeriodID"),
        F.col("I3.LineCode"),
        F.col("I3.ParentEntityID"),
        F.col("I3.SuperParentEntityID"),
        _type_id_expr(
            F.col("B3.AdjustmentAllocationTypeID"),
            None,
            None,
            F.col("I3.LineTypeID"),
            F.col("pli_desc"),
            is_at_risk=False,
        ).alias("TypeID"),
        ns(F.col("I3.Tag"), F.lit("")).alias("Tag"),
        F.coalesce(
            F.col("B3.IsExcludefromTransfer").cast("int"), F.lit(0)
        ).alias("IsExcludefromTransfer"),
        F.coalesce(
            F.col("B3.TrackingKey"), F.col("I3.TrackingKey"), F.lit("")
        ).alias("TrackingKey"),
        F.col("I3.Quarter"),
        F.col("I3.SchID"),
        F.col("I3.OriginalParentEntityID"),
    ).distinct()
    all_inserts.append(p3_insert)

    remaining_input = _delete_matched(remaining_input, p3_insert, "del3")
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) != -1) & (ns0(F.col("LineID")) == -1))
    )
    remaining_input = checkpoint(spark, remaining_input, "alloc_pass3", cfg)

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

    p4_insert = p4_joined.select(
        F.col("I4.RunID"),
        F.col("I4.ClientID"),
        F.col("I4.EntityID"),
        F.col("I4.LineTypeID"),
        F.col("I4.LineID"),
        F.col("I4.Amount"),
        F.col("I4.QuicklinkID"),
        F.col("I4.Amount704b"),
        F.col("I4.CategoryID"),
        F.col("I4.PeriodID"),
        F.col("I4.LineCode"),
        F.col("I4.ParentEntityID"),
        F.col("I4.SuperParentEntityID"),
        _type_id_expr(
            F.col("B4.AdjustmentAllocationTypeID"),
            None,
            None,
            F.col("I4.LineTypeID"),
            F.col("pli_desc"),
            is_at_risk=False,
        ).alias("TypeID"),
        ns(F.col("I4.Tag"), F.lit("")).alias("Tag"),
        F.coalesce(
            F.col("B4.IsExcludefromTransfer").cast("int"), F.lit(0)
        ).alias("IsExcludefromTransfer"),
        F.coalesce(
            F.col("B4.TrackingKey"), F.col("I4.TrackingKey"), F.lit("")
        ).alias("TrackingKey"),
        F.col("I4.Quarter"),
        F.col("I4.SchID"),
        F.col("I4.OriginalParentEntityID"),
    ).distinct()
    all_inserts.append(p4_insert)

    remaining_input = _delete_matched(remaining_input, p4_insert, "del4")
    remaining_book = remaining_book.filter(
        ~((ns0(F.col("FootNoteID")) == -1) & (ns0(F.col("LineID")) != -1))
    )
    remaining_input = checkpoint(spark, remaining_input, "alloc_pass4", cfg)

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

    p5_insert = p5_joined.select(
        F.col("I5.RunID"),
        F.col("I5.ClientID"),
        F.col("I5.EntityID"),
        F.col("I5.LineTypeID"),
        F.col("I5.LineID"),
        F.col("I5.Amount"),
        F.col("I5.QuicklinkID"),
        F.col("I5.Amount704b"),
        F.col("I5.CategoryID"),
        F.col("I5.PeriodID"),
        F.col("I5.LineCode"),
        F.col("I5.ParentEntityID"),
        F.col("I5.SuperParentEntityID"),
        _type_id_expr(
            F.col("B5.AdjustmentAllocationTypeID"),
            None,
            None,
            F.col("I5.LineTypeID"),
            F.col("pli_desc"),
            is_at_risk=False,
        ).alias("TypeID"),
        ns(F.col("I5.Tag"), F.lit("")).alias("Tag"),
        F.coalesce(
            F.col("B5.IsExcludefromTransfer").cast("int"), F.lit(0)
        ).alias("IsExcludefromTransfer"),
        F.coalesce(
            F.col("B5.TrackingKey"), F.col("I5.TrackingKey"), F.lit("")
        ).alias("TrackingKey"),
        F.col("I5.Quarter"),
        F.col("I5.SchID"),
        F.col("I5.OriginalParentEntityID"),
    ).distinct()
    all_inserts.append(p5_insert)

    remaining_input = _delete_matched(remaining_input, p5_insert, "del5")

    # ── CATCH-ALL: Remaining unmatched lines ──
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
            F.col("I6.RunID"),
            F.col("I6.ClientID"),
            F.col("I6.EntityID"),
            F.col("I6.LineTypeID"),
            F.col("I6.LineID"),
            F.col("I6.Amount"),
            F.col("I6.QuicklinkID"),
            F.col("I6.Amount704b"),
            F.col("I6.CategoryID"),
            F.col("I6.PeriodID"),
            F.col("I6.LineCode"),
            F.col("I6.ParentEntityID"),
            F.col("I6.SuperParentEntityID"),
            _type_id_expr(
                F.col("B6.AdjustmentAllocationTypeID"),
                None,
                None,
                F.col("I6.LineTypeID"),
                F.col("pli_desc"),
                is_at_risk=False,
            ).alias("TypeID"),
            ns(F.col("I6.Tag"), F.lit("")).alias("Tag"),
            F.lit(0).alias("IsExcludefromTransfer"),
            F.coalesce(
                F.col("B6.TrackingKey"), F.col("I6.TrackingKey"), F.lit("")
            ).alias("TrackingKey"),
            F.col("I6.Quarter"),
            F.col("I6.SchID"),
            F.col("I6.OriginalParentEntityID"),
        )
        .distinct()
    )
    all_inserts.append(catchall)

    df_alloc_input = all_inserts[0]
    for part in all_inserts[1:]:
        df_alloc_input = df_alloc_input.unionByName(part)

    return df_alloc_input
