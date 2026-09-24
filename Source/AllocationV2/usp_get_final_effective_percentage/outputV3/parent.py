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


def sibling_module(short_name: str) -> ModuleType:
    """Load an outputV3 module even when Databricks hides Workspace Files.

    Databricks Workspace notebooks in a folder are importable as a package, but
    newly uploaded Workspace Files next to them are often invisible to
    ``importlib.import_module``. Loading by path next to this file works for
    both notebooks and files.
    """
    module_name = f"{__package__}.{short_name}"
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        pass
    here = Path(__file__).resolve().parent
    candidates = (
        here / f"{short_name}.py",
        here / short_name,
        Path(str(here) + f"/{short_name}.py"),
    )
    last_error = None
    for source_path in candidates:
        try:
            if not source_path.exists():
                continue
        except Exception as exc:
            last_error = exc
            continue
        spec = importlib.util.spec_from_file_location(
            module_name, str(source_path)
        )
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        module.__package__ = __package__
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module
    detail = f" last_error={last_error}" if last_error else ""
    raise ModuleNotFoundError(
        f"Cannot import {module_name} from {here}. "
        f"Upload {short_name}.py as a Workspace File next to orchestrator.py"
        f"{detail}"
    )


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


__all__ = ["isolated_output_module", "output_module", "sibling_module"]
