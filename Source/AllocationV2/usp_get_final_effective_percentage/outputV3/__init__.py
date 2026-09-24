"""Isolated Final Effective Percentage outputV3 package."""

from __future__ import annotations

__all__ = [
    "get_last_run_profile",
    "run_final_effective_percentages",
    "run_mode",
    "run_modes",
]


def __getattr__(name):
    if name in __all__:
        from . import orchestrator

        return getattr(orchestrator, name)
    raise AttributeError(name)
