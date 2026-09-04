"""Shared plan-size profiler for the optimized Allocation SPs.

Runtime package: ``AllocationV2.plan_profiler`` (this folder deploys to
``Source/AllocationV2/plan_profiler`` per the migration folder map, so every SP
under ``AllocationV2`` — and every benchmark notebook — can import it).

Public API::

    from AllocationV2.plan_profiler import (
        measure_plan, track_plan, plan_profile_report,
        start_plan_profile, finish_plan_profile,
        build_plan_profile_display,
    )

Purpose
-------
Measure how much each builder function grows the Spark *logical plan* (the DAG
that Catalyst analyzes and whole-stage-codegens). On small inputs the dominant
cost of these pipelines is planning/codegen of very large logical plans, not
data movement — so knowing *which builder* inflates the plan tells you exactly
where a ``checkpoint`` (lineage break) will help most.

The profiler is Spark Connect safe, opt-in (zero overhead when disabled), and
fully guarded so it can never break a run. See ``profiler`` for details.
"""

from __future__ import annotations

from .display import build_plan_profile_display
from .profiler import (
    finish_plan_profile,
    measure_plan,
    plan_profile_report,
    start_plan_profile,
    track_plan,
)

__all__ = [
    "measure_plan",
    "track_plan",
    "plan_profile_report",
    "start_plan_profile",
    "finish_plan_profile",
    "build_plan_profile_display",
]
