"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_load_lookthrough_cost_alloc']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import load_lookthrough_cost_alloc_to_output as _mod
    return getattr(_mod, name)
