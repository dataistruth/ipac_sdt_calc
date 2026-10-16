"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

__all__ = ['apply_investment_level_rounding']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import apply_investment_level_rounding as _mod
    return getattr(_mod, name)
