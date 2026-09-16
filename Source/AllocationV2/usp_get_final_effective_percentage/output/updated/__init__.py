# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set. All modules here import each other
# relatively (from .cost_pct_loader / .checkpoint / .parent ...), so a
# partial upload leaves the package unimportable.
"""Isolated optimized variant of usp_get_final_effective_percentage.

Import the entry point from ``output.updated.orchestrator``. Keeping package
initialization lazy is important for the A/B notebook's fresh-module isolation.
"""
