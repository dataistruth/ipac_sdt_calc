"""Entry point adapter matching production ``run_usp_load_allocation_input``."""

from .load_allocation_input import run_load_allocation_input


def run_usp_load_allocation_input(spark, cfg: dict = None, **kwargs) -> dict:
    return run_load_allocation_input(spark, cfg=cfg, **kwargs)
