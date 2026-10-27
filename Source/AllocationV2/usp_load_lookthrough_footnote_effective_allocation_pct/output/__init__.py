"""Production output package. Lazy exports only."""

from __future__ import annotations

__all__ = ['run_load_lt_footnote_effective_allocation_pct']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import load_lt_footnote_effective_allocation_pct as _mod
    return getattr(_mod, name)
