"""Build three independent output groups with isolated mutable collector state."""

from .parallel_helpers import isolated_cfg, merge_collector_cfg, run_parallel
from .parent import output_module

_final = output_module("ai_finalization_service")


def collect_outputs(spark, cfg, allocation_input_df, pfic_flowup_df, max_threads=4):
    def task(fn, *args):
        local = isolated_cfg(cfg)
        fn(spark, local, *args)
        return local

    tasks = [
        ("allocation", lambda: task(_final.write_allocation_input, allocation_input_df)),
        ("pfic_flowup", lambda: task(_final.write_pfic_flowup, pfic_flowup_df)),
        ("form_flowups", lambda: task(_final.write_form_flowups)),
    ]
    for _, local in run_parallel(tasks, max_threads, "final_collectors"):
        merge_collector_cfg(cfg, local)
