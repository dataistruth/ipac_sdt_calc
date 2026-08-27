"""
Backward-compatible shim for benchmarks and runner notebook.

Prefer:
  AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input
"""
from .updated.load_allocation_input import run_load_allocation_input

__all__ = ["run_load_allocation_input"]
