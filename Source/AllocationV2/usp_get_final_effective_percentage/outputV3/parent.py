"""Load production FEP modules into an isolated outputV3 namespace."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_PARENT_PACKAGE = (
    __package__.rsplit(".", 1)[0]
    if __package__ and __package__.endswith(".outputV3")
    else ""
)
_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def output_module(short_name: str) -> ModuleType:
    if not _PARENT_PACKAGE:
        raise ImportError("outputV3 must be imported from its AllocationV2 package")
    return importlib.import_module(f"{_PARENT_PACKAGE}.output.{short_name}")


def isolated_output_module(short_name: str) -> ModuleType:
    """Execute unchanged production source without mutating its module object."""
    module_name = f"{__package__}._production_{short_name}"
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


__all__ = ["isolated_output_module", "output_module"]
