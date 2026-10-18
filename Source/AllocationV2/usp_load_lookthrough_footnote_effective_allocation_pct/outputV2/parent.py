"""outputV2 is self-contained. Helpers live in this package."""

from __future__ import annotations

import importlib

_OUTPUT_PACKAGE = __package__


def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")
