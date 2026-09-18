"""Access unchanged production modules without duplicating business logic."""

import importlib

PARENT = __package__.rsplit(".", 1)[0]


def output_module(name: str):
    return importlib.import_module(f"{PARENT}.output.{name}")


__all__ = ["output_module"]
