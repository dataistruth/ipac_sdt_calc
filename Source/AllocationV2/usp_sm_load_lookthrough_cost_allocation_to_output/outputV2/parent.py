"""Import the unchanged production implementation from the sibling package."""

from __future__ import annotations

import importlib

_SP_PACKAGE = __package__.rsplit(".", 1)[0]
_OUTPUT_PACKAGE = f"{_SP_PACKAGE}.output"


def output_module(name: str):
    """Load one module from the read-only production output package."""
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")


def production_orchestrator():
    return output_module("orchestrator")


__all__ = ["output_module", "production_orchestrator"]
