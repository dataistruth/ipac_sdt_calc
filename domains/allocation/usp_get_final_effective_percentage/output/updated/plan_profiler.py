# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set. This module now re-exports the shared
# AllocationV2.plan_profiler package — also sync domains/allocation/plan_profiler
# (deploys to Source/AllocationV2/plan_profiler). If it is missing, this shim
# degrades to no-ops so the pipeline still imports and runs.
"""Thin re-export of the shared plan-size profiler (``AllocationV2.plan_profiler``).

Kept as a module so existing relative imports (``from .plan_profiler import
...``) keep working unchanged. All logic lives in the shared package; this file
only forwards its public API (and provides safe no-op fallbacks when the shared
package is not deployed, since plan profiling is opt-in and non-essential).
"""

from __future__ import annotations

import logging as _logging

try:
    from AllocationV2.plan_profiler import (
        finish_plan_profile,
        measure_plan,
        plan_profile_report,
        start_plan_profile,
        track_plan,
    )
except Exception as _exc:  # pragma: no cover - deploy safety net
    _logging.getLogger(__name__).warning(
        "[PLAN] shared AllocationV2.plan_profiler unavailable (%s); plan "
        "profiling disabled (no-op). Sync domains/allocation/plan_profiler "
        "(-> Source/AllocationV2/plan_profiler) to enable it.",
        _exc,
    )

    def track_plan(fn):
        return fn

    def measure_plan(df):
        return None

    def plan_profile_report(source, threshold=None):
        return []

    def start_plan_profile():
        return None, []

    def finish_plan_profile(token):
        return None


__all__ = [
    "measure_plan",
    "track_plan",
    "plan_profile_report",
    "start_plan_profile",
    "finish_plan_profile",
]
