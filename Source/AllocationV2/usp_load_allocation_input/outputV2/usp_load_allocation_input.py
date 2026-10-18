"""Entry point for run_conversion.py runner (naming convention: run_<sp_name>)."""
from .load_allocation_input import run_load_allocation_input


def run_usp_load_allocation_input(spark, cfg: dict = None, **kwargs) -> dict:
    """Adapter to match the runner convention: run_<folder_name>(spark, cfg).

    Supports all three execution modes:
      Mode 1 (Job):         cfg passed via taskValues JSON
      Mode 2 (Orchestrator): cfg passed as dict
      Mode 3 (Standalone):  individual params passed as kwargs
    """
    return run_load_allocation_input(spark, cfg=cfg, **kwargs)
