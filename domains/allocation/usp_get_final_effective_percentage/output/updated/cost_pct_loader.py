"""Candidate-based direct cost-percentage matching for the isolated variant.

Only the six direct UO/ET/AC tiers are replaced here.  The parent hierarchy,
transfer expansion, TrackingKeyMatch cleanup, tag expansion, and empty-key
fallback continue through the unchanged production implementation with an
empty snapshot, which makes its direct tiers no-ops.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

from .parent import output_module


_original = output_module("cost_pct_loader")

OPTIMIZATION_PROFILE_MARKER = "candidate_claims_direct_v1"

_CLAIM_KEY = ("DealId", "TypeId", "Tag", "TrackingKey", "_mode")
_PRIORITY_COLUMN = "_candidate_priority"
_STAGE_COLUMN = "_candidate_stage"
_RANK_COLUMN = "_candidate_claim_rank"


def _with_candidate_metadata(
    rows: DataFrame,
    priority: int,
    stage: str,
) -> DataFrame:
    """Attach stable claim metadata without changing payload identity."""
    return (
        rows.withColumn(_PRIORITY_COLUMN, F.lit(priority).cast("int"))
        .withColumn(_STAGE_COLUMN, F.lit(stage))
    )


def _winning_candidate_payload(
    existing_rows: DataFrame,
    candidates: DataFrame,
) -> DataFrame:
    """Return every payload row belonging to the first claim for each key.

    Ranking payload rows directly would discard valid partner/quarter rows.
    Instead, this ranks DISTINCT key-level claims and semi-joins the winning
    candidate claim back to the complete candidate payload.
    """
    existing_claims = (
        existing_rows.select(*_CLAIM_KEY)
        .distinct()
        .withColumn(_PRIORITY_COLUMN, F.lit(0).cast("int"))
        .withColumn(_STAGE_COLUMN, F.lit("__existing__"))
    )
    candidate_claims = candidates.select(
        *_CLAIM_KEY, _PRIORITY_COLUMN, _STAGE_COLUMN,
    ).distinct()

    ranked_claims = (
        existing_claims.unionByName(candidate_claims)
        .withColumn(
            _RANK_COLUMN,
            F.row_number().over(
                Window.partitionBy(*_CLAIM_KEY).orderBy(
                    F.col(_PRIORITY_COLUMN),
                    F.col(_STAGE_COLUMN),
                )
            ),
        )
    )
    winning_candidate_claims = ranked_claims.filter(
        (F.col(_RANK_COLUMN) == 1)
        & (F.col(_STAGE_COLUMN) != "__existing__")
    ).select(
        *_CLAIM_KEY, _PRIORITY_COLUMN, _STAGE_COLUMN,
    )

    claim_match = [
        candidates[column] == winning_candidate_claims[column]
        for column in (
            *_CLAIM_KEY,
            _PRIORITY_COLUMN,
            _STAGE_COLUMN,
        )
    ]
    return (
        candidates.join(
            winning_candidate_claims,
            reduce(lambda left, right: left & right, claim_match),
            "left_semi",
        )
        .drop(_PRIORITY_COLUMN, _STAGE_COLUMN)
    )


def _candidate_union(parts: list[DataFrame]) -> DataFrame:
    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True,
        ),
        parts,
    )


def build_cost_percentage_by_type(
    spark: SparkSession,
    cfg: dict,
    cost_pct_snapshot: DataFrame,
    temp_cost_pct: DataFrame,
    all_underlyings: DataFrame,
    entity_underlyings: DataFrame,
    non_dated: DataFrame,
    dated: DataFrame,
    transfers_adj: DataFrame,
    checkpoint_fn=None,
) -> tuple[DataFrame, DataFrame]:
    """Resolve direct tiers with key-level claims, then run the original tail."""
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    cost_alloc_type_id = cfg["cost_allocation_type_id"]
    entity_ut = cfg.get("entity_underlying_type_id")
    uo_ut = cfg.get("underlying_only_type_id")
    et_ut = cfg.get("entity_total_underlying_type_id")
    ac_ut = cfg.get("asset_class_underlying_type_id")

    _original.logger.info(
        "[OPTIMIZATION] build_cost_percentage_by_type profile=%s",
        OPTIMIZATION_PROFILE_MARKER,
    )

    cps = F.broadcast(
        cost_pct_snapshot.filter(
            (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
    )

    def select_cost_pct(
        rows: DataFrame,
        underlying_entity_col,
        tracking_key_col,
        underlying_type_col=None,
    ) -> DataFrame:
        columns = [
            underlying_entity_col.alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(
                F.col("C.CommitmentPercent"),
                F.lit(0),
            ).alias("CommitmentPercent"),
            F.coalesce(
                F.col("C.AllocationTypeID"),
                F.lit(cost_alloc_type_id).cast("int"),
            ).alias("TypeId"),
            tracking_key_col.alias("TrackingKey"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
        ]
        if underlying_type_col is None:
            columns.append(F.col("C.UnderlyingType"))
        else:
            columns.append(underlying_type_col.alias("UnderlyingType"))
        columns.extend(
            [
                F.col("C.`704cAllocationTypeID`"),
                F.col("C.`704cPercentageType`"),
                F.col("C.GPPartnerReceivingCarry"),
                F.col("E._mode"),
            ]
        )
        return rows.select(*columns).distinct()

    formatted_investment = _original._iif_inv(
        F.col("C.InvestmentID"),
        F.col("C.EntityID"),
    )
    formatted_tracking_key = _original._tracking_match_expr(
        F.col("C.TrackingKey"),
        F.col("C.InvestmentID"),
        F.col("C.EntityID"),
    )
    effective_underlying_type = F.coalesce(
        F.col("C.UnderlyingType"),
        F.lit(entity_ut).cast("int"),
    )

    uo_tracking_key = select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (
                F.col("E.AllocationTypeId")
                == F.col("C.AllocationTypeID")
            )
            & (
                F.coalesce(F.col("C.TrackingKey"), F.lit(""))
                == F.col("E.TrackingKey")
            ),
        )
        .filter(effective_underlying_type == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )

    uo_tracking_match = select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (
                F.col("E.AllocationTypeId")
                == F.col("C.AllocationTypeID")
            )
            & (formatted_tracking_key == F.col("E.TrackingMatch")),
        )
        .filter(effective_underlying_type == uo_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
    )

    et_tracking_key = select_cost_pct(
        cps.alias("C")
        .join(
            all_underlyings.alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (
                F.col("E.AllocationTypeId")
                == F.col("C.AllocationTypeID")
            )
            & (
                F.col("C.TrackingKey")
                == F.col("E.TrackingKey")
            ),
        )
        .filter(effective_underlying_type == et_ut),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )

    et_tracking_match = (
        cps.alias("C")
        .join(
            all_underlyings.select(
                "EntityId",
                "AllocationTypeId",
                "TrackingMatch",
                "UnderlyingEntityId",
                "TrackingKey",
                "_mode",
            ).distinct().alias("E"),
            (F.col("E.EntityId") == formatted_investment)
            & (
                F.col("E.AllocationTypeId")
                == F.col("C.AllocationTypeID")
            )
            & (formatted_tracking_key == F.col("E.TrackingMatch")),
        )
        .filter(effective_underlying_type == et_ut)
        .select(
            F.col("E.UnderlyingEntityId").alias("DealId"),
            F.col("C.PartnerNumber"),
            F.col("C.Quarter"),
            F.coalesce(
                F.col("C.CommitmentPercent"),
                F.lit(0),
            ).alias("CommitmentPercent"),
            F.coalesce(
                F.col("C.AllocationTypeID"),
                F.lit(cost_alloc_type_id).cast("int"),
            ).alias("TypeId"),
            F.coalesce(
                F.col("C.TrackingKey"),
                F.lit(""),
            ).alias("TrackingKey"),
            F.coalesce(F.col("C.Tag"), F.lit("")).alias("Tag"),
            F.when(
                (formatted_investment != F.col("C.EntityID"))
                & (
                    F.coalesce(F.col("C.TrackingKey"), F.lit(""))
                    == ""
                ),
                F.col("E.TrackingMatch"),
            ).otherwise(
                F.lit(None).cast("string")
            ).alias("TrackingKeyMatch"),
            F.col("C.UnderlyingType"),
            F.col("C.`704cAllocationTypeID`"),
            F.col("C.`704cPercentageType`"),
            F.col("C.GPPartnerReceivingCarry"),
            F.col("E._mode"),
        )
        .distinct()
    )

    ac_by_asset_class = select_cost_pct(
        cps.alias("C")
        .join(
            entity_underlyings.alias("E"),
            F.col("E.AssetClassId") == F.col("C.InvestmentID"),
        )
        .filter(
            (effective_underlying_type == ac_ut)
            & (F.col("C.InvestmentID") != -1)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("E.TrackingKey"), F.lit("")),
        F.col("C.UnderlyingType"),
    )

    entity_underlying_ids = entity_underlyings.select(
        "UnderlyingEntityId",
        "_mode",
    ).distinct()
    ac_entity_default = select_cost_pct(
        cps.alias("C")
        .crossJoin(entity_underlying_ids.alias("E"))
        .filter(
            (F.col("C.InvestmentID") == -1)
            & (effective_underlying_type == entity_ut)
        ),
        F.col("E.UnderlyingEntityId"),
        F.coalesce(F.col("C.TrackingKey"), F.lit("")),
    )

    candidates = _candidate_union(
        [
            _with_candidate_metadata(
                uo_tracking_key,
                10,
                "uo_tracking_key",
            ),
            _with_candidate_metadata(
                uo_tracking_match,
                20,
                "uo_tracking_match",
            ),
            _with_candidate_metadata(
                et_tracking_key,
                30,
                "et_tracking_key",
            ),
            _with_candidate_metadata(
                et_tracking_match,
                40,
                "et_tracking_match",
            ),
            _with_candidate_metadata(
                ac_by_asset_class,
                50,
                "ac_by_asset_class",
            ),
            _with_candidate_metadata(
                ac_entity_default,
                60,
                "ac_entity_default",
            ),
        ]
    )
    winning_payload = _winning_candidate_payload(
        temp_cost_pct,
        candidates,
    )
    temp_cost_pct = temp_cost_pct.unionByName(
        winning_payload,
        allowMissingColumns=True,
    )

    mode = cfg.get("_current_mode", 1)
    direct_checkpoint_name = f"tcp_post_et_m{mode}"
    if checkpoint_fn is not None:
        temp_cost_pct = checkpoint_fn(
            spark,
            temp_cost_pct,
            direct_checkpoint_name,
            cfg,
        )
        _original.logger.info(
            "[CHECKPOINT] optimized direct candidate wave"
        )

    skipped_direct_checkpoint = False

    def tail_checkpoint_fn(spark_arg, df, name, cfg_arg):
        nonlocal skipped_direct_checkpoint
        if (
            name == direct_checkpoint_name
            and not skipped_direct_checkpoint
        ):
            skipped_direct_checkpoint = True
            return df
        return checkpoint_fn(spark_arg, df, name, cfg_arg)

    # Retain the original downstream implementation exactly.  An empty
    # snapshot makes all six original direct branches no-ops.  The adapter
    # suppresses only their now-redundant tcp_post_et checkpoint; notably,
    # parent_ord_m{mode} and every later checkpoint remain untouched.
    empty_snapshot = cost_pct_snapshot.limit(0)
    return _original.build_cost_percentage_by_type(
        spark,
        cfg,
        empty_snapshot,
        temp_cost_pct,
        all_underlyings,
        entity_underlyings,
        non_dated,
        dated,
        transfers_adj,
        checkpoint_fn=(
            tail_checkpoint_fn
            if checkpoint_fn is not None
            else None
        ),
    )
