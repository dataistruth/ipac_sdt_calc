"""SP-local shim for the shared AllocationV2 plan profiler."""

try:
    from AllocationV2.plan_profiler import (
        finish_checkpoint_plan_profile,
        finish_plan_profile,
        measure_plan,
        plan_profile_report,
        start_checkpoint_plan_profile,
        start_plan_profile,
        track_checkpoint_plan,
        track_plan,
    )
except Exception:
    def measure_plan(df):
        return None

    def track_plan(fn):
        return fn

    def plan_profile_report(source, threshold=None, label=""):
        return []

    def start_plan_profile():
        return None, []

    def finish_plan_profile(token):
        return None

    def start_checkpoint_plan_profile():
        return None, []

    def finish_checkpoint_plan_profile(token):
        return None

    def track_checkpoint_plan(name, df):
        return None


__all__ = [
    "finish_checkpoint_plan_profile", "finish_plan_profile", "measure_plan",
    "plan_profile_report", "start_checkpoint_plan_profile", "start_plan_profile",
    "track_checkpoint_plan", "track_plan",
]
