"""Conservative join hints used only by the updated footnote pipeline.

The helpers in this module deliberately broadcast only bounded lookup/update
sets. Fact DataFrames such as AllocationInput and effective percentages are
never broadcast.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from Common_V2.core.helpers import ns

from .. import quarter_logic as _quarter_logic
from ..allocation_704c import (
    build_custom_footnote_line_types as _build_custom_footnote_line_types,
)


def _apply_quarter_update_broadcast(
    df: DataFrame,
    update_source: DataFrame,
    line_type_id: int,
    ql_col: str,
    lid_col: str,
    tk_col: str,
    quarter_col: str,
) -> DataFrame:
    """Apply a quarter update with its narrow update set as the build side."""
    base = df.alias("base")
    update = F.broadcast(update_source).alias("upd")
    return (
        base.join(
            update,
            (F.col("base.QuicklinkID") == F.col(f"upd.{ql_col}"))
            & (F.col("base.LineID") == F.col(f"upd.{lid_col}"))
            & (
                ns(F.col("base.TrackingKey"), F.lit(""))
                == F.col(f"upd.{tk_col}")
            )
            & (F.col("base.LineTypeID") == line_type_id),
            "left",
        )
        .withColumn(
            "Quarter",
            F.when(
                F.col(f"upd.{ql_col}").isNotNull(),
                F.col(f"upd.{quarter_col}"),
            ).otherwise(F.col("base.Quarter")),
        )
        .select(
            [
                F.col(f"base.{column}")
                if column != "Quarter"
                else F.col("Quarter")
                for column in df.columns
            ]
        )
    )


def _apply_quarter_update_with_schid_broadcast(
    df: DataFrame,
    update_source: DataFrame,
    line_type_id: int,
    flow_df: DataFrame,
) -> DataFrame:
    """Apply the Form8865 update with its narrow update set broadcast."""
    del flow_df  # Kept for compatibility with the production helper signature.
    base = df.alias("base")
    update = F.broadcast(update_source).alias("upd")
    return (
        base.join(
            update,
            (F.col("base.QuicklinkID") == F.col("upd._ql"))
            & (F.col("base.LineID") == F.col("upd._lid"))
            & (F.col("base.SchID") == F.col("upd._sch_t"))
            & (
                ns(F.col("base.TrackingKey"), F.lit(""))
                == F.col("upd._tk")
            )
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
        .select(
            [
                F.col(f"base.{column}")
                if column != "Quarter"
                else F.col("Quarter")
                for column in df.columns
            ]
        )
    )


@contextmanager
def quarter_join_hints() -> Iterator[None]:
    """Install updated-only quarter helpers and always restore production."""
    original_update = _quarter_logic._apply_quarter_update
    original_schid_update = _quarter_logic._apply_quarter_update_with_schid
    _quarter_logic._apply_quarter_update = _apply_quarter_update_broadcast
    _quarter_logic._apply_quarter_update_with_schid = (
        _apply_quarter_update_with_schid_broadcast
    )
    try:
        yield
    finally:
        _quarter_logic._apply_quarter_update = original_update
        _quarter_logic._apply_quarter_update_with_schid = original_schid_update


def build_custom_footnote_line_types(spark, cfg: dict) -> DataFrame:
    """Return the naturally small custom line-type lookup with a broadcast hint."""
    return F.broadcast(_build_custom_footnote_line_types(spark, cfg))


def broadcast_part_v_lines(df: DataFrame) -> DataFrame:
    """Hint the distinct Part-V exclusion key set."""
    return F.broadcast(df)


def broadcast_zero_exclude_lines(df: DataFrame) -> DataFrame:
    """Hint the two-form zero-exclusion lookup."""
    return F.broadcast(df)
