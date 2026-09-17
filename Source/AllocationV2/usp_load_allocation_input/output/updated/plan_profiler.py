"""Thin re-export of the shared plan-size profiler (``AllocationV2.plan_profiler``).

Kept as a module so existing relative imports (``from .plan_profiler import
...``) keep working unchanged. All logic lives in the shared package
(``Source/AllocationV2/plan_profiler``); this file only forwards its public API
and provides:

* ``profile_dataframe(label, df, cfg, *, kind=...)`` — compatibility adapter
  used by ``checkpoint.py`` (kind="checkpoint") and the final Delta/Parquet
  writers (kind="action"). Builders are profiled by the ``track_plan``
  decorator, so ``kind="builder"`` is a no-op here.
* safe no-op fallbacks when the shared package is not deployed (plan profiling
  is opt-in and must never break a run).

When enabled (``cfg['profile_plan']`` truthy), ``plan_profile_report(cfg)``
prints the grouped ``[PLAN REPORT BUILDER] / [CHECKPOINT] / [ACTION]`` tables —
node count, depth, (+delta), operator mix, and an add/collapse/remove
(keep/measure) suggestion per stage.
"""

from __future__ import annotations

import logging as _logging

try:
    from AllocationV2.plan_profiler import (
        classify_plan_recommendation,
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        measure_plan,
        plan_profile_report,
        profile_action,
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
        track_action_plan,
        track_checkpoint_plan,
        track_plan,
    )
except Exception as _exc:  # pragma: no cover - deploy safety net
    _logging.getLogger(__name__).warning(
        "[PLAN] shared AllocationV2.plan_profiler unavailable (%s); plan "
        "profiling disabled (no-op). Sync Source/AllocationV2/plan_profiler "
        "to enable it.",
        _exc,
    )

    def track_plan(fn):
        return fn

    def measure_plan(df):
        return None

    def plan_profile_report(source, threshold=None, label=""):
        return []

    def classify_plan_recommendation(record, threshold, kind="builder"):
        return "measure"

    def start_plan_profile():
        return None, []

    def finish_plan_profile(token):
        return None

    def start_checkpoint_plan_profile():
        return None, []

    def finish_checkpoint_plan_profile(token):
        return None

    def start_action_profile():
        return None, []

    def finish_action_profile(token):
        return None

    def track_checkpoint_plan(name, df, cfg=None):
        return None

    def track_action_plan(name, df, cfg=None, elapsed_seconds=None):
        return None

    def profile_action(name, df, action, cfg=None):
        return action()


def profile_dataframe(label, df, cfg, *, kind="builder"):
    """Compatibility adapter for the ``profile_dataframe`` call sites.

    * ``kind="checkpoint"`` → record the plan a checkpoint truncates.
    * ``kind="action"``     → record a Spark action's input plan.
    * ``kind="builder"``    → no-op (builders use the ``track_plan`` decorator).

    Always returns ``df`` and never raises, so it is safe to wrap any frame.
    """
    try:
        if kind == "checkpoint":
            track_checkpoint_plan(label, df, cfg)
        elif kind == "action":
            track_action_plan(label, df, cfg)
    except Exception:  # pragma: no cover - profiling must never break a run
        _logging.getLogger(__name__).debug(
            "[PLAN] profile_dataframe(%s, kind=%s) failed", label, kind,
            exc_info=True,
        )
    return df


__all__ = [
    "classify_plan_recommendation",
    "measure_plan",
    "plan_profile_report",
    "profile_action",
    "profile_dataframe",
    "start_plan_profile",
    "finish_plan_profile",
    "start_checkpoint_plan_profile",
    "finish_checkpoint_plan_profile",
    "start_action_profile",
    "finish_action_profile",
    "track_action_plan",
    "track_checkpoint_plan",
    "track_plan",
]
