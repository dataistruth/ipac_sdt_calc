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

from .profiler import classify_plan_recommendation

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
    kind: str = "builder",
):
    """Build a Spark DataFrame from plan-profile records, ranked by delta desc.

    Columns: ``func``, ``nodes``, ``depth``, ``delta``, ``checkpoint_candidate``,
    ``recommendation``, ``ops``. ``kind`` is ``builder`` (add/measure/collapse),
    ``checkpoint`` (keep/measure/remove), or ``action`` (add/measure).
    Returns an empty-schema DataFrame when there are no records.
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
            StructField("recommendation", StringType(), True),
            StructField("ops", StringType(), True),
        ]
    )

    try:
        limit = int(threshold)
    except (TypeError, ValueError):
        limit = _DEFAULT_THRESHOLD

    normalized_kind = str(kind).strip().lower()
    report_kind = (
        normalized_kind
        if normalized_kind in {"builder", "checkpoint", "action"}
        else "builder"
    )
    ranked = sorted(
        list(records or []),
        key=lambda r: int(r.get("delta", 0) or 0),
        reverse=True,
    )
    rows = []
    for raw in ranked:
        rec = dict(raw)
        recommendation = rec.get("recommendation") or classify_plan_recommendation(
            rec, limit, report_kind
        )
        candidate = "yes" if recommendation in {"add", "keep"} else ""
        rows.append(
            (
                str(rec.get("func")),
                int(rec.get("nodes", 0) or 0),
                int(rec.get("depth", 0) or 0),
                int(rec.get("delta", 0) or 0),
                candidate,
                recommendation,
                _format_ops(rec.get("ops")),
            )
        )
    return spark.createDataFrame(rows, schema=schema)
