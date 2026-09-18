"""Effective-percentage builder with the production seam routed through V2."""

from __future__ import annotations

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import checkpoint_V2

from .parent import service_module

_production = service_module("effective_pct_service")
apply_exclude_from_residual = _production.apply_exclude_from_residual
apply_pe_book_unmapped_lines = _production.apply_pe_book_unmapped_lines


def compute_effective_percentages(
    spark, cfg, total_amounts, sm_lt_input
):
    """Preserve the production effective-amount expression and checkpoint."""
    entity_id = cfg["entity_id"]
    sm_lt_keyed = sm_lt_input.withColumn(
        "_join_tk",
        F.concat(F.col("TrackingKey"), F.lit("~"), F.lit(str(entity_id))),
    )
    effective_amounts = (
        total_amounts.alias("A")
        .join(
            F.broadcast(sm_lt_keyed).alias("S"),
            (F.col("A.StateID") == F.col("S.StateID"))
            & (F.col("A.StateFieldID") == F.col("S.StateLineID"))
            & (F.col("A.LineTypeID") == F.col("S.LineTypeID"))
            & (F.col("A.EntityID") == F.col("S.EntityID"))
            & (F.col("A.TrackingKey") == F.col("S._join_tk")),
        )
        .filter(
            (F.col("A.AllocAmount") != 0)
            & (F.col("A.InputAmount") != 0)
        )
        .select(
            F.col("S.EntityID"),
            F.col("A.StateID"),
            F.col("S.LineTypeID"),
            F.col("A.StateFieldID"),
            F.col("A.PartnerNumber"),
            F.when(
                F.col("A.InputAmount") != 0,
                (
                    F.col("A.AllocAmount") / F.col("A.InputAmount")
                ) * F.col("S.StateAmount"),
            ).alias("EffectiveAmount"),
            F.col("S.ParentEntityID"),
            F.col("S.SuperParentEntityID"),
            F.col("S.TrackingKey"),
            F.col("A.AllocType"),
            F.col("S.OriginalParentEntityID"),
        )
    )
    effective_amounts = checkpoint_V2(
        spark, effective_amounts, "effective_amounts", cfg
    )
    temp_effective = effective_amounts.select(
        "EntityID", "StateID", "LineTypeID", "StateFieldID",
        "PartnerNumber", "EffectiveAmount", "ParentEntityID",
        "SuperParentEntityID", "TrackingKey", "AllocType",
    )
    return effective_amounts, temp_effective


__all__ = [
    "apply_exclude_from_residual",
    "apply_pe_book_unmapped_lines",
    "compute_effective_percentages",
]
