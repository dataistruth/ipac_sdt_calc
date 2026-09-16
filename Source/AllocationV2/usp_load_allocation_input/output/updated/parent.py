"""Load unchanged services from the production parent ``output`` package."""

from __future__ import annotations

import importlib
from types import ModuleType


def output_module(name: str) -> ModuleType:
    parent = __package__.rsplit(".", 1)[0]
    return importlib.import_module(f"{parent}.{name}")
