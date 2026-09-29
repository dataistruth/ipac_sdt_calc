"""Optimized candidate for uspAddLookThroughAllocationDetail_Step_01."""

from __future__ import annotations

__all__ = ["run_add_lookthrough_allocation_detail_step01"]


def __getattr__(name):
    if name == "run_add_lookthrough_allocation_detail_step01":
        from .add_lookthrough_allocation_detail_step01 import run_add_lookthrough_allocation_detail_step01
        return run_add_lookthrough_allocation_detail_step01
    raise AttributeError(name)
