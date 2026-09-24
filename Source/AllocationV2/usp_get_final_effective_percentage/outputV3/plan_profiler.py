"""Safe shim for the shared AllocationV2 plan profiler."""

from __future__ import annotations

import logging

try:
    from AllocationV2.plan_profiler import (
        finish_action_profile,
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        plan_profile_report,
        profile_action,
        start_action_profile,
        start_checkpoint_plan_profile,
        start_plan_profile,
        track_checkpoint_plan,
        track_plan,
    )
except Exception as exc:  # pragma: no cover - deployment safety
    logging.getLogger(__name__).warning(
        "[PLAN] shared profiler unavailable; profiling disabled: %s", exc
    )

    def track_plan(fn):
        return fn

    def plan_profile_report(source, threshold=None, label=""):
        return []

    def start_plan_profile():
        return None, []

    def start_checkpoint_plan_profile():
        return None, []

    def start_action_profile():
        return None, []

    def finish_plan_profile(token):
        return None

    def finish_checkpoint_plan_profile(token):
        return None

    def finish_action_profile(token):
        return None

    def track_checkpoint_plan(name, df, cfg=None):
        return None

    def profile_action(name, df, action, cfg=None):
        return action()


__all__ = [
    "finish_action_profile",
    "finish_checkpoint_plan_profile",
    "finish_plan_profile",
    "plan_profile_report",
    "profile_action",
    "start_action_profile",
    "start_checkpoint_plan_profile",
    "start_plan_profile",
    "track_checkpoint_plan",
    "track_plan",
]
