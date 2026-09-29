"""Optimized candidate for uspApplyInvestmentLevelRounding."""

from __future__ import annotations

__all__ = ["apply_investment_level_rounding"]


def __getattr__(name):
    if name == "apply_investment_level_rounding":
        from .apply_investment_level_rounding import (
            apply_investment_level_rounding,
        )
        return apply_investment_level_rounding
    raise AttributeError(name)
