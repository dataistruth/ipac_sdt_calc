"""SM look-through cost allocation outputV2 candidate."""

from __future__ import annotations

__all__ = [
    "get_last_run_profile",
    "run_sm_load_lookthrough_cost_allocation_to_output",
]


def __getattr__(name):
    if name in __all__:
        from . import orchestrator

        return getattr(orchestrator, name)
    raise AttributeError(name)
