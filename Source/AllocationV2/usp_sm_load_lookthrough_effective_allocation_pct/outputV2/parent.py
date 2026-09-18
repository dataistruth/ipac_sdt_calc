"""Load unchanged production modules without modifying the production tree."""

from __future__ import annotations

import importlib
import sys

_SP_PACKAGE = __package__.rsplit(".", 1)[0]
_OUTPUT_PACKAGE = f"{_SP_PACKAGE}.output"


def output_module(name: str):
    """Import a production module, supporting its legacy ``services`` imports."""
    services_name = f"{_OUTPUT_PACKAGE}.services"
    services = importlib.import_module(services_name)
    sys.modules.setdefault("services", services)
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")


def service_module(name: str):
    """Import an unchanged production service module."""
    services = importlib.import_module(f"{_OUTPUT_PACKAGE}.services")
    sys.modules.setdefault("services", services)
    module = importlib.import_module(f"{_OUTPUT_PACKAGE}.services.{name}")
    sys.modules.setdefault(f"services.{name}", module)
    return module
