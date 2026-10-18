"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_sm_load_lt_effective_alloc_pct']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import orchestrator as _mod
    return getattr(_mod, name)
