# Updated usp_load_footnotes_allocation_to_output

This is an isolated optimization package for
`usp_load_footnotes_allocation_to_output`. The unchanged production reference
is under:

`/Users/mukesh.singh/spark/sdt_d/Source/AllocationV2/usp_load_footnotes_allocation_to_output/output/`

No production files are modified.

## Initial updated behavior

- Preserves the production S1-S13 business flow and public entry-point
  signature.
- Uses namespaced UC Delta lineage breaks with data-skipping statistics
  disabled: `temp_alloc_input`, `cost_snapshot`, `all_underlyings`,
  `underlyings_fn`, and `alloc_input`. The `cost_snapshot` break materializes
  the cost 4-way union + `distinct` once so the S6 hierarchy branches do not
  re-evaluate it.
- Adds intra-builder lineage breaks in `plan_break_optimizations.py` (see
  below) to attack the two plans that dominate on small inputs:
  - `entity_levels` — inside `build_entity_hierarchy`, right after the 8-level
    unrolled union loop, so the join-back + `distinct` + four unions are
    analyzed/codegen'd against a materialized table instead of the full union
    tree (`all_underlyings` was ~74s on 64 rows, ~70s of which was
    Catalyst/codegen of this plan, not data).
  - `alloc_pass1`..`alloc_pass4` — inside `build_allocation_input`, after each
    pass's left-anti delete, so the accumulating anti-join chain is truncated
    and every later pass / the final 6-way union is planned against a small
    table (`alloc_input` was ~31s on 64 rows for the same reason).
  These two builders are copied verbatim from `..underlyings` and
  `..allocation_input`; the *only* difference is the added `checkpoint` calls.
  Semantics are identical and verified by the benchmark's row-count,
  amount-sum, and xxhash64 fingerprint parity checks. If parity ever fails,
  suspect a transcription drift in `plan_break_optimizations.py` first.
  On large production inputs these extra breaks add Delta-write cost that
  scales with data; if a large-RunID benchmark regresses, reduce or remove the
  per-pass breaks (they are the tunable part).
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

### Interpreting benchmark numbers and driver memory

All A/B passes run in one long-lived driver JVM, so JIT/GC/CodeCache state
accumulates across runs and inflates later passes. On the reference RunID the
input is tiny (~64 rows, 0 output rows), so wall time is dominated by Catalyst
planning, whole-stage codegen, and Unity Catalog checkpoint metadata — not data
volume. Broadcast hints and the S3/S5 thread pool only pay off on realistically
sized inputs; validate them there.

Driver GC logs from this benchmark showed the JVM CodeCache filling to its
~240 MB cap mid-run, which disables the JIT compiler and slows every subsequent
pass (frequent `Pause Full (CodeCache GC Threshold)` events). This is a
benchmark artifact — production runs the SP once per JVM (and serverless manages
the CodeCache), so it does not occur in production. To keep benchmark numbers
trustworthy on a classic cluster:

- Set `spark.driver.extraJavaOptions=-XX:ReservedCodeCacheSize=512m` on the
  benchmark cluster.
- Prefer a fresh Python/JVM (detach-reattach) between passes, or keep the pass
  count low, so JIT state from one variant does not bias the next.

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
