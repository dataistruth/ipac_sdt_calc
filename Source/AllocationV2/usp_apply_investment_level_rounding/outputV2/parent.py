"""Import unchanged production services without modifying production output."""

from __future__ import annotations

import importlib

_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"


def output_module(name: str):
    """Return an unchanged module from the sibling production package."""
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")


__all__ = ["output_module"]
