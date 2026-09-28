"""Load production helpers from this SP's live `output/` package."""

from __future__ import annotations

import importlib
import sys

_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"
# Production runner uses flat `from _data_loading import ...` imports.
FLAT_SIBLINGS = ("_data_loading", "_hierarchy", "_allocation")


def register_flat_aliases(output_package: str = _OUTPUT_PACKAGE) -> None:
    for name in FLAT_SIBLINGS:
        sys.modules[name] = importlib.import_module(f"{output_package}.{name}")


def output_module(name: str):
    register_flat_aliases()
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")


__all__ = ["FLAT_SIBLINGS", "output_module", "register_flat_aliases"]
