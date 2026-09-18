# SM Look-Through Effective Allocation outputV2

## Scope and invariants

The public entry remains `run_sm_load_lt_effective_alloc_pct`. Production
`output/` is unchanged. Candidate code uses only
`Common_V2.core.checkpoint_V2`, inherits its shared default when checkpoint mode
is blank, does not call checkpoint cleanup on the hot path, and adds no
checkpoint profile, bypass, coalesce, repartition, or mid-run drop behavior.

## Dependency map

1. Common and SP configuration, then the allocation-type gate.
2. Mapping construction and tagged K1/UBTI mapping checkpoint.
3. Sequential conditional flow-up build, output write, and input update.
4. Shared look-through inputs and their two production checkpoints.
5. Independent K1 and UBTI amount builders.
6. State-mapped amounts.
7. Effective amounts and their production checkpoint.
8. Sequential final output write followed by allocation-input update.

The only pool contains `build_k1_amounts` and `build_ubti_amounts`. It starts
after `fed_lines`, `non_sp_fp`, mapping data, and K1 side-pocket inputs exist.
The builders only read those shared DataFrames and return separate tuples.
Workers are capped at `min(MaxThreads, 2, 4)`. Flow-up and both final mutations
remain sequential.

## Preserved checkpoint seams

| Seam | Production reason | Candidate |
| --- | --- | --- |
| mapping tagged union | mapping fan-out across later joins | `checkpoint_V2` |
| filtered look-through input | two consumers | `checkpoint_V2` |
| aggregated look-through input | state lines and effective join | `checkpoint_V2` |
| K1 amount union | partner and total aggregations | `checkpoint_V2` |
| effective amounts | exclude/PE-book/final-write fan-out | `checkpoint_V2` |

All five seams are retained until Databricks measurements prove a direct code
change is both faster and parity-safe. Mode 2 alternates local and stats-off
Delta by sequence; sequence assignment is shared and locked across the K1/UBTI
pool.

## Profiling and timing

`ProfilePlan=on` emits complete BUILDER, CHECKPOINT, and ACTION reports with a
default node threshold of 30. Explicit flow-up existence, flow-up write, final
result write, and final input update actions are timed and profiled. The
orchestrator also records section timings, per-task/pool timing, thread names,
checkpoint backend/sequence/timing, and exposes them through
`get_last_run_profile()`.

Initial checkpoint recommendations are conservative:

- **Keep pending measurement:** mapping and effective checkpoints have high
  known fan-out.
- **Measure:** filtered input, aggregated input, and K1 amount seams each have
  two consumers; retain unless write cost exceeds avoided recomputation.
- **No new checkpoints:** no additional seam has measured fan-out and action
  cost evidence yet.

The notebook enriches these recommendations with measured nodes, depth,
operator mix, checkpoint duration, and known consumer count.

## Reconciliation and benchmark

`notebook/benchmark_sm_load_lookthrough_effective_allocation_pct.py`:

- runs both variants in either order (alternating by default);
- evicts production, outputV2, profiler, legacy `services`, and `Common_V2`
  modules before every import;
- snapshots both RunID-scoped input and pre-existing output rows;
- restores input and purges output before each variant;
- compares row count, schema, key null counts, decimal amount sum, and an
  order-independent row fingerprint for output and mutated input;
- restores the user's original input and output rows in `finally`;
- displays benchmark delta, parity, checkpoint, parallel, timing, BUILDER,
  CHECKPOINT, ACTION, and checkpoint-recommendation tables.

Use `ResultType=deltalake` for table reconciliation. The default IDs and names
come from the production orchestrator's standalone example and should be
replaced with a valid isolated RunID before execution.

## Verification status

Local verification is limited to source inspection and Python syntax
compilation because this environment has no Databricks Spark/catalog data.
Performance and parity are therefore **not yet accepted**. Run the benchmark
notebook with a valid RunID; acceptance requires both execution orders to match
and wall-time improvement beyond normal variance.
