"""Sequential SM Output then Input writes (never parallel)."""

from __future__ import annotations

from .parent import output_module

_prod = output_module("orchestrator")


def flush_result_tables(spark, cfg, final_output, alloc_output):
    written = _prod.write_allocation_output(spark, cfg, final_output)
    _prod.write_update_allocation_input(spark, cfg, alloc_output)
    return written


__all__ = ["flush_result_tables"]
