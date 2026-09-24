"""Candidate-claim CPBT wave vendored for the isolated outputV3 package."""

from __future__ import annotations

from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import Window

from .parent import isolated_output_module

_original = isolated_output_module("cost_pct_loader")
_CLAIM_KEY = ("DealId", "TypeId", "Tag", "TrackingKey", "_mode")
_PRIORITY = "_candidate_priority"
_STAGE = "_candidate_stage"
_RANK = "_candidate_claim_rank"


def _metadata(rows, priority: int, stage: str):
    return (
        rows.withColumn(_PRIORITY, F.lit(priority).cast("int"))
        .withColumn(_STAGE, F.lit(stage))
    )


def _union(parts):
    return reduce(
        lambda left, right: left.unionByName(
            right, allowMissingColumns=True
        ),
        parts,
    )


def _winning_payload(existing_rows, candidates):
    existing_claims = (
        existing_rows.select(*_CLAIM_KEY)
        .distinct()
        .withColumn(_PRIORITY, F.lit(0).cast("int"))
        .withColumn(_STAGE, F.lit("__existing__"))
    )
    candidate_claims = candidates.select(
        *_CLAIM_KEY, _PRIORITY, _STAGE
    ).distinct()
    winners = (
        existing_claims.unionByName(candidate_claims)
        .withColumn(
            _RANK,
            F.row_number().over(
                Window.partitionBy(*_CLAIM_KEY).orderBy(
                    F.col(_PRIORITY), F.col(_STAGE)
                )
            ),
        )
        .filter((F.col(_RANK) == 1) & (F.col(_STAGE) != "__existing__"))
        .select(*_CLAIM_KEY, _PRIORITY, _STAGE)
    )
    condition = [
        candidates[column] == winners[column]
        for column in (*_CLAIM_KEY, _PRIORITY, _STAGE)
    ]
    return (
        candidates.join(
            winners,
            reduce(lambda left, right: left & right, condition),
            "left_semi",
        )
        .drop(_PRIORITY, _STAGE)
    )


def build_cost_percentage_by_type(
    spark,
    cfg,
    cost_pct_snapshot,
    temp_cost_pct,
    all_underlyings,
    entity_underlyings,
    non_dated,
    dated,
    transfers_adj,
    checkpoint_fn=None,
):
    """Resolve six independent direct tiers in one ranked candidate wave."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    cost_type = cfg["cost_allocation_type_id"]
    entity_ut = cfg.get("entity_underlying_type_id")
    uo_ut = cfg.get("underlying_only_type_id")
    et_ut = cfg.get("entity_total_underlying_type_id")
    ac_ut = cfg.get("asset_class_underlying_type_id")

    cps = F.broadcast(
        cost_pct_snapshot.filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
    )
    formatted_investment = _original._iif_inv(
        F.col("C.InvestmentID"), F.col("C.EntityID")
    )
    formatted_match = _original._tracking_match_expr(
        F.col("C.TrackingKey"),
        F.col("C.InvestmentID"),
        F.col("C.EntityID"),
    )
    effective_type = F.coalesce(
        F.col("C.UnderlyingType"), F.lit(entity_ut).cast("int")
    )

    def select_rows(
        rows,
        deal_column,
        tracking_column,
        underlying_type_column=None,
    ):
        columns = [
            deal_column.alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias(
                "CommitmentPercent"
            ),
            F.coalesce(
                F.col("C.AllocationTypeID"),
                F.lit(cost_type).cast("int"),
            ).alias("TypeId"),
            tracking_column.alias("TrackingKey"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
            (
                F.col("C.UnderlyingType")
                if underlying_type_column is None
                else underlying_type_column.alias("UnderlyingType")
            ),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("E._mode"),
        ]
        return rows.select(*columns).distinct()

    uo_key = select_rows(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (
                F.coalesce(F.col("C.TrackingKey"), F.lit(""))
                == F.col("E.TrackingKey")
            ),
        )
        .filter(effective_type == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )
    uo_match = select_rows(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (formatted_match == F.col("E.TrackingMatch")),
        )
        .filter(effective_type == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )
    et_key = select_rows(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (F.col("C.TrackingKey") == F.col("E.TrackingKey")),
        )
        .filter(effective_type == et_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )
    et_match = (
        cps.alias("C")
        .join(
            all_underlyings.select(
                "EntityId",
                "AllocationTypeId",
                "TrackingMatch",
                "UnderlyingEntityId",
                "TrackingKey",
                "_mode",
            )
            .distinct()
            .alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (F.col("E.AllocationTypeId") == F.col("C.AllocationTypeID"))
            & (formatted_match == F.col("E.TrackingMatch")),
        )
        .filter(effective_type == et_ut)
        .select(
            F.col("E.UnderlyingEntityId").alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(F.col("C.CommitmentPercent"), F.lit(0)).alias(
                "CommitmentPercent"
            ),
            F.coalesce(
                F.col("C.AllocationTypeID"),
                F.lit(cost_type).cast("int"),
            ).alias("TypeId"),
            F.coalesce(F.col("C.TrackingKey"), F.lit("")).alias(
                "TrackingKey"
            ),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
            F.when(
                (formatted_investment != F.col("C.EntityID"))
                & (
                    F.coalesce(F.col("C.TrackingKey"), F.lit(""))
                    == ""
                ),
                F.col("E.TrackingMatch"),
            )
            .otherwise(F.lit(None).cast("string"))
            .alias("TrackingKeyMatch"),
            F.col("C.UnderlyingType"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("E._mode"),
        )
        .distinct()
    )
    ac_rows = select_rows(
        cps.alias("C")
        .join(
            entity_underlyings.alias("E"),
            F.col("E.AssetClassId") == F.col("C.InvestmentID"),
        )
        .filter(
            (effective_type == ac_ut)
            & (F.col("C.InvestmentID") != -1)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
        F.col("C.UnderlyingType"),
    )
    entity_ids = entity_underlyings.select(
        "UnderlyingEntityId", "_mode"
    ).distinct()
    ac_default = select_rows(
        cps.alias("C")
        .crossJoin(entity_ids.alias("E"))
        .filter(
            (F.col("C.InvestmentID") == -1)
            & (effective_type == entity_ut)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )

    candidates = _union(
        [
            _metadata(uo_key, 10, "uo_tracking_key"),
            _metadata(uo_match, 20, "uo_tracking_match"),
            _metadata(et_key, 30, "et_tracking_key"),
            _metadata(et_match, 40, "et_tracking_match"),
            _metadata(ac_rows, 50, "ac_by_asset_class"),
            _metadata(ac_default, 60, "ac_entity_default"),
        ]
    )
    temp_cost_pct = temp_cost_pct.unionByName(
        _winning_payload(temp_cost_pct, candidates),
        allowMissingColumns=True,
    )
    checkpoint_name = f"tcp_post_et_m{cfg.get('_current_mode', 1)}"
    if checkpoint_fn is not None:
        temp_cost_pct = checkpoint_fn(
            spark, temp_cost_pct, checkpoint_name, cfg
        )

    skipped = False

    def tail_checkpoint(spark_arg, frame, name, cfg_arg):
        nonlocal skipped
        if name == checkpoint_name and not skipped:
            skipped = True
            return frame
        return checkpoint_fn(spark_arg, frame, name, cfg_arg)

    return _original.build_cost_percentage_by_type(
        spark,
        cfg,
        cost_pct_snapshot.limit(0),
        temp_cost_pct,
        all_underlyings,
        entity_underlyings,
        non_dated,
        dated,
        transfers_adj,
        checkpoint_fn=(
            tail_checkpoint if checkpoint_fn is not None else None
        ),
    )


__all__ = ["build_cost_percentage_by_type"]
