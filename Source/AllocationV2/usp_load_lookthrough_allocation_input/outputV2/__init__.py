"""Optimized candidate for uspLoadLookThroughAllocationInput."""

from __future__ import annotations

__all__ = ["run_load_lookthrough_allocation_input"]


def __getattr__(name):
    if name == "run_load_lookthrough_allocation_input":
        from .load_lookthrough_allocation_input import (
            run_load_lookthrough_allocation_input,
        )
        return run_load_lookthrough_allocation_input
    raise AttributeError(name)
