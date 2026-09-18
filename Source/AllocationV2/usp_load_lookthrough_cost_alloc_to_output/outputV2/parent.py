"""Imports unchanged services from the read-only production package."""

import importlib
import sys

PARENT = __package__.rsplit(".", 1)[0]


def output_module(name: str):
    # The converted production entry retains standalone-style absolute helper
    # imports. Register package-qualified helpers under those legacy names
    # before importing it.
    if name == "load_lookthrough_cost_alloc_to_output":
        for helper in ("_data_loading", "_hierarchy", "_allocation"):
            qualified = f"{PARENT}.output.{helper}"
            sys.modules.setdefault(
                helper, importlib.import_module(qualified)
            )
    return importlib.import_module(f"{PARENT}.output.{name}")


__all__ = ["output_module"]
