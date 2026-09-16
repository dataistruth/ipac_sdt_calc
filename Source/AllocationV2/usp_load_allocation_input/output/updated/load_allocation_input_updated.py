"""
Legacy shim — same import path inside updated/ (no parent output/ shim required).

Import:
  AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input_updated
"""

from .load_allocation_input import run_load_allocation_input

__all__ = ["run_load_allocation_input"]
