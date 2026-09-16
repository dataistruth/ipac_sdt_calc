"""Compatibility entry for ordered output collection."""

from .parent import output_module

_final = output_module("ai_finalization_service")


def collect_outputs(
    spark, cfg, allocation_input_df, pfic_flowup_df, max_threads=4, workers=None, **_kwargs
):
    del max_threads, workers, _kwargs
    # These builders register temp views and mutate cfg["_parquet_results"].
    # Keep production order; the orchestrator parallelizes only final writes
    # to distinct tables.
    _final.write_allocation_input(spark, cfg, allocation_input_df)
    _final.write_pfic_flowup(spark, cfg, pfic_flowup_df)
    _final.write_form_flowups(spark, cfg)


def collect_results_parallel(
    spark,
    cfg,
    allocation_input_df,
    pfic_flowup_df,
    workers=None,
    max_threads=4,
    **kwargs,
):
    """Public name used by the orchestrator."""
    return collect_outputs(
        spark,
        cfg,
        allocation_input_df,
        pfic_flowup_df,
        max_threads=max_threads,
        workers=workers,
        **kwargs,
    )
