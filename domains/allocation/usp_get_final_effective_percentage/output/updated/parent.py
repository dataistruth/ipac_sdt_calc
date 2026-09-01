# updated-package sync marker v2 (2026-09-01): resync the ENTIRE
# output/updated/ folder as one set.
"""Load parent output modules without changing production files."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_PARENT_PACKAGE = (
    __package__.rsplit(".", 1)[0]
    if __package__ and __package__.endswith(".updated")
    else ""
)
_OUTPUT_DIR = Path(__file__).resolve().parent.parent


def output_module(short_name: str) -> ModuleType:
    """Import a normal sibling module from the parent output package."""
    if not _PARENT_PACKAGE:
        raise ImportError("updated must be imported from its full AllocationV2 package")
    return importlib.import_module(f"{_PARENT_PACKAGE}.{short_name}")


def isolated_output_module(short_name: str) -> ModuleType:
    """Execute a parent module in an updated-only module namespace.

    This lets the optimized orchestrator replace selected globals (checkpoint
    and read helpers) without mutating the imported production module.
    """
    module_name = f"{__package__}._base_{short_name}"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached

    source_path = _OUTPUT_DIR / f"{short_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load isolated module from {source_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module
