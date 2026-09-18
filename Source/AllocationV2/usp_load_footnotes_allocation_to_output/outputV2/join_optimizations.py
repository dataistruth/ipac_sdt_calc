"""Bounded join hints proven by the existing candidate."""

from contextlib import contextmanager

import pyspark.sql.functions as F

from Common_V2.core.helpers import ns

from ..output import quarter_logic as _quarter_logic
from ..output.allocation_704c import (
    build_custom_footnote_line_types as _build_custom_types,
)


def _quarter_update(df, source, line_type, ql, lid, tk, quarter):
    base = df.alias("base")
    update = F.broadcast(source).alias("upd")
    return (
        base.join(
            update,
            (F.col("base.QuicklinkID") == F.col(f"upd.{ql}"))
            & (F.col("base.LineID") == F.col(f"upd.{lid}"))
            & (ns(F.col("base.TrackingKey"), F.lit("")) == F.col(f"upd.{tk}"))
            & (F.col("base.LineTypeID") == line_type),
            "left",
        )
        .withColumn(
            "Quarter",
            F.when(
                F.col(f"upd.{ql}").isNotNull(), F.col(f"upd.{quarter}")
            ).otherwise(F.col("base.Quarter")),
        )
        .select(
            *[
                F.col("Quarter")
                if column == "Quarter"
                else F.col(f"base.{column}")
                for column in df.columns
            ]
        )
    )


def _quarter_update_schid(df, source, line_type, flow_df):
    del flow_df
    base = df.alias("base")
    update = F.broadcast(source).alias("upd")
    return (
        base.join(
            update,
            (F.col("base.QuicklinkID") == F.col("upd._ql"))
            & (F.col("base.LineID") == F.col("upd._lid"))
            & (F.col("base.SchID") == F.col("upd._sch_t"))
            & (ns(F.col("base.TrackingKey"), F.lit("")) == F.col("upd._tk"))
            & (F.col("base.LineTypeID") == line_type),
            "left",
        )
        .withColumn(
            "Quarter",
            F.when(
                F.col("upd._new_quarter").isNotNull(),
                F.col("upd._new_quarter"),
            ).otherwise(F.col("base.Quarter")),
        )
        .select(
            *[
                F.col("Quarter")
                if column == "Quarter"
                else F.col(f"base.{column}")
                for column in df.columns
            ]
        )
    )


@contextmanager
def quarter_join_hints():
    """Temporarily broadcast only narrow quarter-update key sets."""
    original = _quarter_logic._apply_quarter_update
    original_schid = _quarter_logic._apply_quarter_update_with_schid
    _quarter_logic._apply_quarter_update = _quarter_update
    _quarter_logic._apply_quarter_update_with_schid = _quarter_update_schid
    try:
        yield
    finally:
        _quarter_logic._apply_quarter_update = original
        _quarter_logic._apply_quarter_update_with_schid = original_schid


def build_custom_footnote_line_types(spark, cfg):
    return F.broadcast(_build_custom_types(spark, cfg))


def broadcast_part_v_lines(df):
    return F.broadcast(df)


def broadcast_zero_exclude_lines(df):
    return F.broadcast(df)


def derive_cost_underlying_types(df_cost_snapshot):
    """Mirror production's TempCostUnderlyingTypes derivation."""
    return (
        df_cost_snapshot.filter(
            (F.lower(F.col("EntityUnderlyingtype")) != "k-1 only")
            | (
                (F.lower(F.col("EntityUnderlyingtype")) == "k-1 only")
                & (F.col("InvestmentID") == -1)
            )
        )
        .select(
            "EntityId",
            "InvestmentID",
            "Quarter",
            "AllocationTypeId",
            "TrackingKey",
            "Underlyingtype",
            "EntityUnderlyingtype",
        )
        .distinct()
    )
