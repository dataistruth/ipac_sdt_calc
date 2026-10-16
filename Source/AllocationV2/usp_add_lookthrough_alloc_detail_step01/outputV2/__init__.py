"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_add_lookthrough_allocation_detail_step01']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import add_lookthrough_allocation_detail_step01 as _mod
    return getattr(_mod, name)
