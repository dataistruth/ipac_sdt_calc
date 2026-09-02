# Updated usp_load_footnotes_allocation_to_output

This is an isolated optimization package for
`usp_load_footnotes_allocation_to_output`. The unchanged production reference
is under:

`/Users/mukesh.singh/spark/sdt_d/Source/AllocationV2/usp_load_footnotes_allocation_to_output/output/`

No production files are modified.

## Initial updated behavior

- Preserves the production S1-S13 business flow and public entry-point
  signature.
- Uses five namespaced UC Delta lineage breaks with data-skipping statistics
  disabled: `temp_alloc_input`, `cost_snapshot`, `all_underlyings`,
  `underlyings_fn`, and `alloc_input`. The `cost_snapshot` break materializes
  the cost 4-way union + `distinct` once so the S6 hierarchy branches do not
  re-evaluate it (this is the dominant cost in the `all_underlyings` checkpoint).
- Records per-stage and per-checkpoint elapsed time.
- Does not change AQE, CBO, shuffle, or auto-broadcast Spark session settings.
- Broadcasts only bounded lookup/update sets: quarter update keys, Part-V
  exclusion keys, custom footnote line types, zero-exclusion keys, and the
  existing entity-scoped partner lookup. Large facts are never broadcast.
- Uses a thread pool for the independent S3/S5 plan builders and for the two
  final writes to separate Delta tables. Quarter updates remain sequential
  because every update consumes the previous update's DataFrame.

## Entry point

```python
from AllocationV2.usp_load_footnotes_allocation_to_output.output.updated.orchestrator import (
    run_load_footnotes_allocation_to_output,
)

status = run_load_footnotes_allocation_to_output(
    spark,
    EntityID=115,
    ClientID=15348,
    TaxPeriodID=1,
    RunID=16560,
    CatalogName="QA7",
    SchemaName="IPC_2025_QA7_15348",
    RankForRulePickup=1,
    parallel_workers=4,
)
```

## Benchmark

Run:

`notebook/benchmark_load_footnotes_allocation_to_output.py`

The SP both appends `AllocationOutput` and deducts amounts from
`AllocationInput`. The notebook therefore:

1. Snapshots the complete `AllocationInput` RunID partition and existing
   footnote output rows.
2. Restores input and purges generated footnote output before every variant.
3. Runs original and updated in alternating order.
4. Compares row counts, schemas, amount sums, and row fingerprints for both
   tables.
5. Restores the exact pre-benchmark table state in a `finally` block.

`ParallelWorkers` defaults to `4` for the updated variant. Set it to `1` to
disable concurrent execution while preserving all join hints.

Do not run another process for the same RunID during the benchmark. If final
restoration fails, backup tables are intentionally retained for manual
recovery and their names are printed in the notebook output.

## Sync to Databricks

Sync this whole directory to the existing monolith package:

```powershell
databricks workspace import-dir `
  "...\usp_load_footnotes_allocation_to_output\output\updated" `
  "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source/AllocationV2/usp_load_footnotes_allocation_to_output/output/updated" `
  --overwrite
```

Importable modules must remain directly under `output/updated/`. Only the
benchmark belongs under `output/updated/notebook/`. Restart Python after sync.

Use at least three A/B passes before drawing a performance conclusion.
