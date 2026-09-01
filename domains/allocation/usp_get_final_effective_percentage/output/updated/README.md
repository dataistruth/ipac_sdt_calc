# Optimized Final Effective Percentage

This folder contains the isolated performance variant of
`usp_get_final_effective_percentage`. Files under the parent `output/` folder
and the production reference under `sdt_d` are not modified.

## Initial optimizations

1. **Fast UC Delta checkpoints**
   - Data-skipping statistics collection is disabled for temporary lineage
     breaks.
   - Checkpoint files are written uncompressed.
   - Names are sanitized and use an `updated` prefix, so they cannot collide
     with production checkpoint tables.

2. **Removed three logging-only Spark jobs**
   - `load_line_items`
   - `load_quarters`
   - `build_lookthrough_input_modes14`

   Production calls `isEmpty()` in these builders only to emit warnings. Each
   probe launches a Spark job and the result is not used for control flow.
   The updated builders return the identical lazy DataFrame without that probe.

3. **Detailed timing**
   - Checkpoint durations are printed individually.
   - Major builder durations are aggregated and returned as `result["timings"]`.
   - Checkpoint write and bypass counts are reported for each run.

4. **Checkpoint profiles**
   - `full`: writes every production lineage break.
   - `conservative` (default): bypasses four single-consumer checkpoints that
     are immediately followed by retained materialization barriers:
     `underlyings_common`, `nde_post_miss_fused`, `eff_dt_fused`, and
     `eff_nd_fused`.
   - `balanced`: also bypasses `all_ent_pre_tag_m0` and `eff_dated_s6_m0`.
     Promote this profile only after multi-pass parity and performance testing.

   High-fan-out and plan-size circuit breakers remain enabled, including the
   common inputs, pre-CPBT inputs, `tcp_post_et_m0`, `all_ent_m0`,
   `parent_ord_m0`, `eff_dated_s5_m0`, and
   `pickup_order_dated_pre_yearly`.

## Entry point

```python
from AllocationV2.usp_get_final_effective_percentage.output.updated.orchestrator import (
    run_final_effective_percentages,
)

result = run_final_effective_percentages(
    spark,
    Mode=0,
    EntityID=4137,
    ClientID=15348,
    TaxPeriodID=1,
    RunID=17376,
    CatalogName="QA7",
    SchemaName="iPC_2025_QA7_15348",
    CheckpointProfile="conservative",
)
```

## Benchmark

Run `notebooks/benchmark_final_effective_percentage.py` in Databricks.

For every pass the notebook:

1. Deletes the benchmark `RunID` from all three output tables.
2. Runs the unchanged original module.
3. Captures row counts, schemas, measure sums, and row fingerprints.
4. Deletes the partitions again and runs the updated module.
5. Fails immediately if any output fingerprint differs.
6. Displays original/updated wall times, checkpoint counts, and updated
   per-step timings.

Use `ResultType=deltalake` for direct table reconciliation. Mode `0` benchmarks
the fused modes 1+2+3 path; Mode `4` benchmarks the 704c path.

Use at least three A/B passes. The notebook alternates original/updated
execution order on successive passes to reduce warm-cache bias. Accept a
reduced profile only when all output fingerprints match, Spark completes
without Analyzer or driver failures, and median wall time improves over
`full`. If `balanced` regresses, use `conservative`; `full` is the immediate
fallback.
