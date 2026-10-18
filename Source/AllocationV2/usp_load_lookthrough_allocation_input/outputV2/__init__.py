"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_load_lookthrough_allocation_input']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import load_lookthrough_allocation_input as _mod
    return getattr(_mod, name)
