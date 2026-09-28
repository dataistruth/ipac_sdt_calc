"""Load production helpers from this SP's live `output/` package."""

from __future__ import annotations

import importlib

PARENT = __package__.rsplit(".", 1)[0]


def output_module(name: str):
    """Import `AllocationV2.<sp>.<name>` (use `output.<module>`)."""
    return importlib.import_module(f"{PARENT}.{name}")
