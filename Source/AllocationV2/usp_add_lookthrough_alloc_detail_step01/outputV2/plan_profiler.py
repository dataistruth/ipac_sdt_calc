"""Slim plan-profiler shim. No-op unless AllocationV2.plan_profiler is present.

measure_plan / df.explain run only when ProfilePlan armed the invocation.
"""

from __future__ import annotations

LAST_PLAN_PROFILE = {"builder": [], "checkpoint": [], "action": []}


def _noop(*args, **kwargs):
    return None


def _empty_start(*args, **kwargs):
    return None, []


def _identity_track(fn):
    return fn


def _passthrough_action(name, df, action, cfg=None):
    return action()


def plan_profile_report(source, threshold=None, label=""):
    return list(source or [])


def disable_plan_profile():
    return None


def profiling_enabled(cfg=None):
    return False


def build_plan_profile_display(spark, records, threshold=30, kind="builder"):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

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
    rows = []
    for raw in list(records or []):
        rec = dict(raw)
        recommendation = rec.get("recommendation") or ""
        candidate = "yes" if recommendation in {"add", "keep"} else ""
        rows.append(
            (
                str(rec.get("func")),
                int(rec.get("nodes", 0) or 0),
                int(rec.get("depth", 0) or 0),
                int(rec.get("delta", 0) or 0),
                candidate,
                recommendation,
                str(rec.get("ops") or ""),
            )
        )
    return spark.createDataFrame(rows, schema=schema)


def store_last_plan_profile(builder=None, checkpoint=None, action=None):
    LAST_PLAN_PROFILE["builder"] = list(builder or [])
    LAST_PLAN_PROFILE["checkpoint"] = list(checkpoint or [])
    LAST_PLAN_PROFILE["action"] = list(action or [])


try:
    from AllocationV2.plan_profiler import (
        build_plan_profile_display,
        disable_plan_profile,
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        measure_plan,
        plan_profile_report,
        profile_action,
        profiling_enabled,
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
        track_action_plan,
        track_checkpoint_plan,
        track_plan,
    )
except Exception:
    finish_action_profile = _noop
    finish_checkpoint_plan_profile = _noop
    finish_plan_profile = _noop
    measure_plan = _noop
    profile_action = _passthrough_action
    start_action_profile = _empty_start
    start_checkpoint_plan_profile = _empty_start
    start_plan_profile = _empty_start
    track_action_plan = _noop
    track_checkpoint_plan = _noop
    track_plan = _identity_track

__all__ = [
    "LAST_PLAN_PROFILE",
    "build_plan_profile_display",
    "disable_plan_profile",
    "finish_checkpoint_plan_profile",
    "finish_action_profile",
    "finish_plan_profile",
    "measure_plan",
    "plan_profile_report",
    "profile_action",
    "profiling_enabled",
    "start_action_profile",
    "start_checkpoint_plan_profile",
    "start_plan_profile",
    "store_last_plan_profile",
    "track_action_plan",
    "track_checkpoint_plan",
    "track_plan",
]
