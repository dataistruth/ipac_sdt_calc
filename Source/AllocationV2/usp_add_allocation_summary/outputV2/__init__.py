"""Optimized candidate for uspAddAllocationSummary."""

from __future__ import annotations

__all__ = ["run_add_allocation_summary"]


def __getattr__(name):
    if name == "run_add_allocation_summary":
        from .add_allocation_summary import run_add_allocation_summary
        return run_add_allocation_summary
    raise AttributeError(name)
