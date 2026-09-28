"""Sequential result writes matching production tables, production order.

Production used ThreadPoolExecutor for Output append + Input overwrite.
That pattern mismatched row counts on a sibling look-through SP. Keep
writes sequential here: LookThroughAllocationOutput then Input.
"""

from __future__ import annotations

from .parent import output_module

_prod = output_module("load_lookthrough_cost_alloc_to_output")


def flush_result_tables(spark, cfg, allocation_output):
    written = _prod.write_allocation_output(spark, cfg, allocation_output)
    _prod.update_input_table(spark, cfg, allocation_output)
    return written


__all__ = ["flush_result_tables"]
