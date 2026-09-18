"""Import unchanged production code from the read-only output package."""

from __future__ import annotations

import importlib

_SP_PACKAGE = __package__.rsplit(".", 1)[0]


def output_module(name: str):
    return importlib.import_module(f"{_SP_PACKAGE}.output.{name}")
