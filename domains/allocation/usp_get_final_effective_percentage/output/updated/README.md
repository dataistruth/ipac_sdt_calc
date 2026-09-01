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

## Entry point

```python
from AllocationV2.usp_get_final_effective_percentage.output.updated.orchestrator import (
    run_final_effective_percentages,
)

result = run_final_effective_percentages(
    spark,
    Mode=0,
    EntityID=5051,
    ClientID=15347,
    TaxPeriodID=1,
    RunID=3517,
    CatalogName="QA7",
    SchemaName="iPC_2025_QA7_15347",
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
6. Displays original/updated wall times and updated per-step timings.

Use `ResultType=deltalake` for direct table reconciliation. Mode `0` benchmarks
the fused modes 1+2+3 path; Mode `4` benchmarks the 704c path.
