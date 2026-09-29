"""Sequential SM Output append then Input update (never parallel)."""

from __future__ import annotations

from .parent import output_module

_writer = output_module("services.writer_service")


def flush_result_tables(spark, cfg, effective_amounts, partner_snapshot):
    written = _writer.write_allocation_output(
        spark, cfg, effective_amounts, partner_snapshot
    )
    _writer.update_allocation_input(spark, cfg, effective_amounts)
    return written


__all__ = ["flush_result_tables"]
