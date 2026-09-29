"""Optimized candidate for usp_SM_LoadLookThroughEffectiveAllocationPercentage."""

from __future__ import annotations

__all__ = ["run_sm_load_lt_effective_alloc_pct"]


def __getattr__(name):
    if name == "run_sm_load_lt_effective_alloc_pct":
        from .orchestrator import run_sm_load_lt_effective_alloc_pct
        return run_sm_load_lt_effective_alloc_pct
    raise AttributeError(name)
