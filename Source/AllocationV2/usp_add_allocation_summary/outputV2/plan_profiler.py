"""Safe shim over the shared AllocationV2 plan profiler."""

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
