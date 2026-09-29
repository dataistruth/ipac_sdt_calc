"""Load production helpers from this SP's live `output/` package."""

from __future__ import annotations

import importlib

_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"


def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")


__all__ = ["output_module"]
