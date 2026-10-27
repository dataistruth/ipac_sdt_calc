"""Production output package. Lazy exports only."""

from __future__ import annotations

__all__ = ['get_last_run_profile', 'run_final_effective_percentages', 'run_mode', 'run_modes']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import orchestrator as _mod
    return getattr(_mod, name)
