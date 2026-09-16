"""Notebook helper: render plan-profile records as a Spark DataFrame.

Kept separate from ``profiler`` so importing the core in non-notebook contexts
does not require Spark at import time — ``pyspark`` is imported lazily inside
the function. Every SP benchmark notebook can render its plan profile
identically::

    from AllocationV2.plan_profiler import build_plan_profile_display
    display(build_plan_profile_display(spark, row["plan_profile"],
                                       threshold=plan_checkpoint_threshold))
"""

from __future__ import annotations

_DEFAULT_THRESHOLD = 30


def _format_ops(ops: dict | None) -> str:
    if not ops:
        return ""
    items = sorted(ops.items(), key=lambda kv: kv[1], reverse=True)
    return " · ".join(f"{name} {count}" for name, count in items)


def build_plan_profile_display(
    spark,
    records,
    threshold: int = _DEFAULT_THRESHOLD,
):
    """Build a Spark DataFrame from plan-profile records, ranked by delta desc.

    Columns: ``func``, ``nodes``, ``depth``, ``delta``, ``checkpoint_candidate``,
    ``ops``. ``depth`` is included as a first-class runtime column. Returns an
    empty-schema DataFrame when there are no records.
    """
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("func", StringType(), True),
            StructField("nodes", LongType(), True),
            StructField("depth", LongType(), True),
            StructField("delta", LongType(), True),
            StructField("checkpoint_candidate", StringType(), True),
            StructField("ops", StringType(), True),
        ]
    )

    try:
        limit = int(threshold)
    except (TypeError, ValueError):
        limit = _DEFAULT_THRESHOLD

    ranked = sorted(
        list(records or []),
        key=lambda r: int(r.get("delta", 0) or 0),
        reverse=True,
    )
    rows = [
        (
            str(r.get("func")),
            int(r.get("nodes", 0) or 0),
            int(r.get("depth", 0) or 0),
            int(r.get("delta", 0) or 0),
            "yes" if int(r.get("delta", 0) or 0) >= limit else "",
            _format_ops(r.get("ops")),
        )
        for r in ranked
    ]
    return spark.createDataFrame(rows, schema=schema)
