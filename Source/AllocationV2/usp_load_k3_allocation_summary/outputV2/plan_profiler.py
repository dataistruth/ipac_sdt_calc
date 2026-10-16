"""Slim plan-profiler shim. No-op unless AllocationV2.plan_profiler is present."""

from __future__ import annotations


def _noop(*args, **kwargs):
    return None


def _empty_start(*args, **kwargs):
    return None, []


def plan_profile_report(source, threshold=None, label=""):
    return None


try:
    from AllocationV2.plan_profiler import (
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
except Exception:
    finish_action_profile = _noop
    finish_checkpoint_plan_profile = _noop
    finish_plan_profile = _noop
    measure_plan = _noop
    profile_action = _noop
    start_action_profile = _empty_start
    start_checkpoint_plan_profile = _empty_start
    start_plan_profile = _empty_start
    track_action_plan = _noop
    track_checkpoint_plan = _noop
    track_plan = _noop

__all__ = [
    "finish_checkpoint_plan_profile",
    "finish_action_profile",
    "finish_plan_profile",
    "measure_plan",
    "plan_profile_report",
    "profile_action",
    "start_action_profile",
    "start_checkpoint_plan_profile",
    "start_plan_profile",
    "track_action_plan",
    "track_checkpoint_plan",
    "track_plan",
]
