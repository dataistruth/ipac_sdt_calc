"""
Import sibling modules from monolith output/ (ai_*.py, load_allocation_input.py).

These files are NOT copied with output/updated/ — they must already exist at:

  AllocationV2/usp_load_allocation_input/output/
"""

from __future__ import annotations

import importlib
from types import ModuleType

_PARENT_PKG = (
    __package__.rsplit(".", 1)[0]
    if __package__ and __package__.endswith(".updated")
    else ""
)


def output_module(short_name: str) -> ModuleType:
    """Import e.g. ai_config_service from parent output package."""
    if not _PARENT_PKG:
        raise ImportError(
            "updated.parent must be imported as AllocationV2.*.output.updated.parent"
        )
    full_name = f"{_PARENT_PKG}.{short_name}"
    try:
        return importlib.import_module(full_name)
    except ImportError as exc:
        raise ImportError(
            f"Cannot import {full_name}. "
            f"Sync only updates output/updated/; monolith must still have "
            f"output/{short_name}.py beside updated/. "
            "Add Source/ to sys.path before importing updated package."
        ) from exc
