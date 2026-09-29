"""Optimized candidate for uspLoadK3AllocationSummary."""

from __future__ import annotations

__all__ = ["run_usp_load_k3_allocation_summary"]


def __getattr__(name):
    if name == "run_usp_load_k3_allocation_summary":
        from .usp_load_k3_allocation_summary import run_usp_load_k3_allocation_summary
        return run_usp_load_k3_allocation_summary
    raise AttributeError(name)
