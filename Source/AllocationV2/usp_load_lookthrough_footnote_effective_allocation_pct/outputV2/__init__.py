"""Optimized candidate for uspLoadLookThroughFootnoteEffectiveAllocationPercentage."""

from __future__ import annotations

__all__ = ["run_load_lt_footnote_effective_allocation_pct"]


def __getattr__(name):
    if name == "run_load_lt_footnote_effective_allocation_pct":
        from .load_lt_footnote_effective_allocation_pct import (
            run_load_lt_footnote_effective_allocation_pct,
        )
        return run_load_lt_footnote_effective_allocation_pct
    raise AttributeError(name)
