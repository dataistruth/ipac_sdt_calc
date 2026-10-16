"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_usp_load_k3_allocation_summary']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import usp_load_k3_allocation_summary as _mod
    return getattr(_mod, name)
