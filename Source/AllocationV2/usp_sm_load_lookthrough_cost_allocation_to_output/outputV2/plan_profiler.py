"""Safe shim over the shared AllocationV2 plan profiler."""

from __future__ import annotations

import functools

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
        track_plan as _shared_track_plan,
    )

    def _is_dataframe(value):
        return (
            hasattr(value, "explain")
            and hasattr(value, "schema")
            and hasattr(value, "columns")
        )

    def _first_dataframe(value):
        if _is_dataframe(value):
            return value
        values = value.values() if isinstance(value, dict) else value
        if isinstance(values, (tuple, list)) or hasattr(values, "__iter__"):
            for item in values:
                if _is_dataframe(item):
                    return item
        return None

    def track_plan(fn):
        """Record tuple-returning production builders as well as DataFrames."""
        shared = _shared_track_plan(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            cfg = kwargs.get("cfg")
            if not isinstance(cfg, dict):
                cfg = next(
                    (
                        item
                        for item in args
                        if isinstance(item, dict)
                        and (
                            "run_id" in item
                            or "profile_plan" in item
                        )
                    ),
                    None,
                )
            if not (isinstance(cfg, dict) and cfg.get("profile_plan")):
                return shared(*args, **kwargs)
            input_nodes = 0
            for value in (*args, *kwargs.values()):
                if _is_dataframe(value):
                    metrics = measure_plan(value)
                    if metrics:
                        input_nodes = max(input_nodes, metrics["nodes"])
            result = fn(*args, **kwargs)
            output = _first_dataframe(result)
            metrics = measure_plan(output) if output is not None else None
            if metrics:
                cfg.setdefault("_plan_profile", []).append(
                    {
                        "func": fn.__name__,
                        "nodes": metrics["nodes"],
                        "depth": metrics["depth"],
                        "delta": metrics["nodes"] - input_nodes,
                        "ops": metrics["ops"],
                    }
                )
            return result

        return wrapper
except Exception:
    def track_plan(fn):
        return fn

    def measure_plan(df):
        return None

    def plan_profile_report(source, threshold=None, label=""):
        return []

    def start_plan_profile():
        return None, []

    start_checkpoint_plan_profile = start_plan_profile
    start_action_profile = start_plan_profile

    def finish_plan_profile(token):
        return None

    finish_checkpoint_plan_profile = finish_plan_profile
    finish_action_profile = finish_plan_profile

    def track_checkpoint_plan(name, df, cfg=None):
        return None

    track_action_plan = track_checkpoint_plan

    def profile_action(name, df, action, cfg=None):
        return action()


__all__ = [
    "finish_action_profile",
    "finish_checkpoint_plan_profile",
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
