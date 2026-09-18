"""Mature hierarchy and allocation-input plan breaks for outputV2."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from Common_V2.core.checkpoint_V2 import checkpoint_V2
from Common_V2.core.helpers import ns, ns0, read_table

from ..output.underlyings import _build_asset_class_relationship
from .plan_profiler import track_plan

_MAX_DEPTH = 8


def _checkpoint(spark, df, name, cfg):
    """Use shared V2 and reset qualifiers after an actual local backend."""
    activity_start = len(cfg.get("_checkpoint_v2_activity", ()))
    result = checkpoint_V2(spark, df, name, cfg)
    activity = cfg.get("_checkpoint_v2_activity", ())
    if (
        len(activity) > activity_start
        and activity[-1].get("backend") == "local"
    ):
        result = result.toDF(*result.columns)
    return result


@track_plan
def build_entity_hierarchy(
    spark: SparkSession,
    cfg: dict,
    df_cost_pct_snapshot: DataFrame,
    df_temp_cost_underlying_types: DataFrame,
) -> tuple:
    """Production hierarchy with the mature ``entity_levels`` seam."""
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
                    entity_rel["LowerTierEntityID"].cast("string"),
                    F.lit("~"),
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
        entity_rel["LowerTierEntityID"].alias(
            "ImmediateLowerTierEntityID"
        ),
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
            F.col(f"er_cte_{depth}.LowerTierEntityID").alias(
                "LowerTierEntityID"
            ),
            F.col(f"er_cte_{depth}.UpperTierEntityID").alias(
                "ParentEntityID"
            ),
            current_level["CurrentEntityId"],
            (current_level["HLevel"] + 1).alias("HLevel"),
            current_level["AllocationTypeId"],
            current_level["TrackingKey"],
            current_level["AssetClassId"],
            current_level["ImmediateLowerTierEntityID"],
        )
        all_levels = all_levels.unionByName(next_level)
        current_level = next_level

    all_levels = _checkpoint(spark, all_levels, "entity_levels", cfg)
    cte_result = (
        all_levels.alias("EH")
        .join(
            anchor_entity.alias("TC2"),
            (F.col("EH.CurrentEntityId") == F.col("TC2._join_entity"))
            & (
                F.col("TC2.AllocationTypeId")
                == F.col("EH.AllocationTypeId")
            )
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
                    F.lit("~"),
                    F.col("InvestmentID").cast("string"),
                    F.lit("~"),
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
                F.lit("~"),
                F.lit(entity_id).cast("string"),
                F.lit("~"),
            ).alias("TrackingKey"),
            F.lit(entity_id).alias("AssetClassId"),
            F.lit(0).alias("ImmediateLowerTierEntityID"),
        )
        .distinct()
    )
    asset_class_union = df_temp_cost_underlying_types.filter(
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
    entity_total_union = df_temp_cost_underlying_types.filter(
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
                F.lit("~"),
                F.col("InvestmentID").cast("string"),
                F.lit("~"),
            ),
        )
        .otherwise(F.col("TrackingKey"))
        .alias("TrackingKey"),
        F.col("InvestmentID").alias("AssetClassId"),
        F.lit(0).alias("ImmediateLowerTierEntityID"),
    )
    return (
        cte_result.unionByName(k1_only)
        .unionByName(k1_self)
        .unionByName(asset_class_union)
        .unionByName(entity_total_union),
        df_asset_class_rel,
    )


@track_plan
def build_allocation_input(
    spark: SparkSession,
    cfg: dict,
    df_temp_alloc_input: DataFrame,
    df_temp_book_eff: DataFrame,
    df_underlyings_fn: DataFrame,
) -> DataFrame:
    """Production five-pass allocation builder with four mature seams."""
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
        null_int = F.lit(None).cast("int")
        return F.coalesce(
            b_adj if b_adj is not None else null_int,
            bk_adj if bk_adj is not None else null_int,
            ai_alloc if ai_alloc is not None else null_int,
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
        matched = insert_df.select(
            "EntityID",
            "LineID",
            "LineTypeID",
            "QuicklinkID",
            "TrackingKey",
        ).distinct().alias(alias_name)
        return current.join(
            matched,
            (current["EntityID"] == F.col(f"{alias_name}.EntityID"))
            & (current["LineID"] == F.col(f"{alias_name}.LineID"))
            & (
                current["LineTypeID"]
                == F.col(f"{alias_name}.LineTypeID")
            )
            & (
                current["QuicklinkID"]
                == F.col(f"{alias_name}.QuicklinkID")
            )
            & (
                current["TrackingKey"]
                == F.col(f"{alias_name}.TrackingKey")
            ),
            "left_anti",
        )

    def _select_insert(
        source_alias,
        b_adj,
        bk_adj,
        ai_alloc,
        desc,
        exclude,
        tracking,
        is_at_risk=False,
    ):
        prefix = source_alias
        return [
            F.col(f"{prefix}.RunID"),
            F.col(f"{prefix}.ClientID"),
            F.col(f"{prefix}.EntityID"),
            F.col(f"{prefix}.LineTypeID"),
            F.col(f"{prefix}.LineID"),
            F.col(f"{prefix}.Amount"),
            F.col(f"{prefix}.QuicklinkID"),
            F.col(f"{prefix}.Amount704b"),
            F.col(f"{prefix}.CategoryID"),
            F.col(f"{prefix}.PeriodID"),
            F.col(f"{prefix}.LineCode"),
            F.col(f"{prefix}.ParentEntityID"),
            F.col(f"{prefix}.SuperParentEntityID"),
            _type_id_expr(
                b_adj,
                bk_adj,
                ai_alloc,
                F.col(f"{prefix}.LineTypeID"),
                desc,
                is_at_risk,
            ).alias("TypeID"),
            ns(F.col(f"{prefix}.Tag"), F.lit("")).alias("Tag"),
            exclude.alias("IsExcludefromTransfer"),
            tracking.alias("TrackingKey"),
            F.col(f"{prefix}.Quarter"),
            F.col(f"{prefix}.SchID"),
            F.col(f"{prefix}.OriginalParentEntityID"),
        ]

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
    p1_joined = (
        remaining_input.filter(
            F.col("LineTypeID") == at_risk_lt
        ).alias("I1")
        .join(
            k1_li,
            F.col("I1.LineID") == k1_li["k1_LineID"],
            "inner",
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
                | F.col("AI1.LineID").isNotNull()
                | (ns0(F.col("BK1.LineID")) != -1)
            )
        )
    )
    p1_insert = p1_joined.select(
        *_select_insert(
            "I1",
            F.col("B1.AdjustmentAllocationTypeID"),
            F.col("BK1.AdjustmentAllocationTypeID"),
            F.col("AI1.AllocationTypeId"),
            F.col("k1_desc"),
            F.coalesce(
                F.col("B1.IsExcludefromTransfer").cast("int"),
                F.col("AI1.ExcludeFromTransfers"),
                F.lit(0),
            ),
            F.coalesce(
                F.col("B1.TrackingKey"),
                F.col("I1.TrackingKey"),
                F.lit(""),
            ),
            True,
        )
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
    remaining_input = _checkpoint(
        spark, remaining_input, "alloc_pass1", cfg
    )

    be_p2 = remaining_book.filter(
        (ns0(F.col("FootNoteID")) != -1)
        & (ns0(F.col("LineID")) != -1)
    ).alias("B2")
    p2_joined = (
        remaining_input.alias("I2")
        .join(
            pfic_li,
            F.col("I2.LineID") == pfic_li["pli_LineID"],
            "left",
        )
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
            (
                (ns0(F.col("B2.FootNoteID")) != -1)
                & (ns0(F.col("B2.LineID")) != -1)
            )
            | F.col("AI2.LineID").isNotNull()
        )
    )
    p2_insert = p2_joined.select(
        *_select_insert(
            "I2",
            F.col("B2.AdjustmentAllocationTypeID"),
            None,
            F.col("AI2.AllocationTypeId"),
            F.col("pli_desc"),
            F.coalesce(
                F.col("B2.IsExcludefromTransfer").cast("int"),
                F.col("AI2.ExcludeFromTransfers"),
                F.lit(0),
            ),
            F.coalesce(
                F.col("B2.TrackingKey"),
                F.col("I2.TrackingKey"),
                F.lit(""),
            ),
        )
    ).distinct()
    all_inserts.append(p2_insert)
    remaining_input = _delete_matched(remaining_input, p2_insert, "del2")
    remaining_book = remaining_book.filter(
        ~(
            (ns0(F.col("FootNoteID")) != -1)
            & (ns0(F.col("LineID")) != -1)
        )
    )
    remaining_input = _checkpoint(
        spark, remaining_input, "alloc_pass2", cfg
    )

    def _wildcard_insert(pass_no, footnote_nonnull, line_nonnull):
        alias = f"I{pass_no}"
        book_alias = f"B{pass_no}"
        input_df = remaining_input.alias(alias)
        book = remaining_book.filter(
            (
                ns0(F.col("FootNoteID")) != -1
                if footnote_nonnull
                else ns0(F.col("FootNoteID")) == -1
            )
            & (
                ns0(F.col("LineID")) != -1
                if line_nonnull
                else ns0(F.col("LineID")) == -1
            )
        ).alias(book_alias)
        joined = input_df.join(
            pfic_li,
            F.col(f"{alias}.LineID") == pfic_li["pli_LineID"],
            "left",
        )
        conditions = (
            F.col(f"{alias}.EntityID")
            == F.col(f"{book_alias}.UnderlyingEntityID")
        ) & (
            F.col(f"{book_alias}.SourceID")
            == F.col(f"{alias}.LineTypeID")
        )
        if footnote_nonnull:
            conditions = conditions & (
                F.col(f"{book_alias}.FootNoteID")
                == F.col(f"{alias}.QuicklinkID")
            )
        if line_nonnull:
            conditions = conditions & (
                ns0(F.col(f"{book_alias}.LineID"))
                == ns0(F.col(f"{alias}.LineID"))
            )
        conditions = (
            conditions
            & _ns_match(
                F.col(f"{book_alias}.TrackingKey"),
                F.col(f"{alias}.TrackingKey"),
            )
            & _ns_match(
                F.col(f"{book_alias}.Tag"),
                F.col(f"{alias}.Tag"),
            )
        )
        joined = joined.join(book, conditions, "inner")
        return joined.select(
            *_select_insert(
                alias,
                F.col(f"{book_alias}.AdjustmentAllocationTypeID"),
                None,
                None,
                F.col("pli_desc"),
                F.coalesce(
                    F.col(
                        f"{book_alias}.IsExcludefromTransfer"
                    ).cast("int"),
                    F.lit(0),
                ),
                F.coalesce(
                    F.col(f"{book_alias}.TrackingKey"),
                    F.col(f"{alias}.TrackingKey"),
                    F.lit(""),
                ),
            )
        ).distinct()

    p3_insert = _wildcard_insert(3, True, False)
    all_inserts.append(p3_insert)
    remaining_input = _delete_matched(remaining_input, p3_insert, "del3")
    remaining_book = remaining_book.filter(
        ~(
            (ns0(F.col("FootNoteID")) != -1)
            & (ns0(F.col("LineID")) == -1)
        )
    )
    remaining_input = _checkpoint(
        spark, remaining_input, "alloc_pass3", cfg
    )

    p4_insert = _wildcard_insert(4, False, True)
    all_inserts.append(p4_insert)
    remaining_input = _delete_matched(remaining_input, p4_insert, "del4")
    remaining_book = remaining_book.filter(
        ~(
            (ns0(F.col("FootNoteID")) == -1)
            & (ns0(F.col("LineID")) != -1)
        )
    )
    remaining_input = _checkpoint(
        spark, remaining_input, "alloc_pass4", cfg
    )

    p5_insert = _wildcard_insert(5, False, False)
    all_inserts.append(p5_insert)
    remaining_input = _delete_matched(remaining_input, p5_insert, "del5")

    catchall = (
        remaining_input.alias("I6")
        .join(
            pfic_li,
            F.col("I6.LineID") == pfic_li["pli_LineID"],
            "left",
        )
        .join(
            remaining_book.alias("B6"),
            (F.col("I6.EntityID") == F.col("B6.UnderlyingEntityID"))
            & (F.col("B6.SourceID") == F.col("I6.LineTypeID"))
            & (ns0(F.col("I6.LineID")) == ns0(F.col("B6.LineID")))
            & (
                ns0(F.col("I6.QuicklinkID"))
                == ns0(F.col("B6.FootNoteID"))
            )
            & (_ns_match(F.col("B6.TrackingKey"), F.col("I6.TrackingKey")))
            & (_ns_match(F.col("B6.Tag"), F.col("I6.Tag"))),
            "left",
        )
        .filter(F.col("B6.UnderlyingEntityID").isNull())
        .select(
            *_select_insert(
                "I6",
                F.col("B6.AdjustmentAllocationTypeID"),
                None,
                None,
                F.col("pli_desc"),
                F.lit(0),
                F.coalesce(
                    F.col("B6.TrackingKey"),
                    F.col("I6.TrackingKey"),
                    F.lit(""),
                ),
            )
        )
        .distinct()
    )
    all_inserts.append(catchall)
    result = all_inserts[0]
    for part in all_inserts[1:]:
        result = result.unionByName(part)
    return result


__all__ = ["build_allocation_input", "build_entity_hierarchy"]
