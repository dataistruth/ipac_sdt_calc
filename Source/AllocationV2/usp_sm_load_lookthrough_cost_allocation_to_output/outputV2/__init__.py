"""Optimized candidate for usp_SM_LoadLookThroughCostAllocationToOutput."""

from __future__ import annotations

__all__ = ["run_sm_load_lookthrough_cost_allocation_to_output"]


def __getattr__(name):
    if name == "run_sm_load_lookthrough_cost_allocation_to_output":
        from .orchestrator import (
            run_sm_load_lookthrough_cost_allocation_to_output,
        )
        return run_sm_load_lookthrough_cost_allocation_to_output
    raise AttributeError(name)
