# uspApplyInvestmentLevelRounding optimization report

## Scope and invariants

- Public entry remains `apply_investment_level_rounding`.
- Production `output/*.py` is unchanged; this candidate is isolated in `outputV2`.
- The similarly named `usp_sm_apply_investment_level_rounding` is not used.
- `CheckpointMode` is resolved by `resolve_checkpoint_mode` and initialized exactly
  once per invocation; the shared Common V2 default is not duplicated locally.
- Production seams `temp_alloc_output` and `rounded_diff` are retained.
- There is no `CheckpointProfile`, checkpoint coalescing/repartitioning, or hot-path
  checkpoint cleanup.

## Dependency map

1. Common and SP configuration run sequentially because they perform actions and mutate
   `cfg`.
2. Exactly one lookthrough load is selected by investment-level and `CallFrom` flags.
   The four alternatives are mutually exclusive and remain an `if/elif/elif/else`.
3. `build_allocation_input` consumes lookthrough input.
4. After that point, four independent preparation tasks form the sole pool:
   allocation/UBTI input, temp-allocation output, max-allocation type, and partner
   snapshots. Each receives an isolated `cfg`; shared profile/activity lists are only
   appended through thread-safe profiler/checkpoint code. No task writes business
   output or consumes another task's result.
5. `compute_rounding_diff` waits for all pool outputs. The partner task's isolated
   `has_rounding_override` result is merged into the main `cfg` only after all futures
   finish.
6. Both production checkpoint seams materialize before rounding.
7. Exactly one rounding strategy is selected. Highest-percent preparation remains
   inside that branch. No strategy is parallelized.
8. Passthrough, writes, `IsRounded` mutation, and allocation-summary writes remain
   ordered.

## Applied changes

- Added a bounded `ThreadPoolExecutor` pool (`1..4` workers) for four independent
  read-only preparation tasks. Stable task names, timings, isolated mutable config,
  deterministic result order, cancellation, and exception propagation are recorded.
- Added opt-in builder, checkpoint, and action profiling. Builders are decorated with
  the shared `AllocationV2.plan_profiler`; the two V2 checkpoints profile their incoming
  plans; scalar lookups and top-level result stores are timed as actions.
- Pushed RunID filters into both sides of call-from lookthrough joins and retained
  existing RunID pruning elsewhere; pushed client/tax-period/nonzero filters into
  snapshot reads. Current-RunID workflow and lower-tier key sets are narrow, bounded
  projections and are broadcast into their fact joins. No unbounded fact is broadcast.
  No cache was added because there is no repeated materialized action whose measured
  reuse benefit exceeds its cost.
- Added mutation-safe A/B reconciliation for all eight output tables and
  `LookThroughOffsetUnRoundedLines`. The notebook restores the exact RunID snapshot
  before each variant and restores the original state after the benchmark.

## Checkpoint recommendation

Both seams are **keep pending measurement**:

- `temp_alloc_output`: high fan-out (rounding strategies, passthrough-derived work, and
  final summaries) makes recomputation potentially expensive.
- `rounded_diff`: reused by every non-`None` rounding strategy and several subplans.

Node count alone is not acceptance evidence. Run the notebook with `ProfilePlan=on` in
both execution orders and compare builder delta, plan depth/operator mix, downstream
consumer count, checkpoint wall time, action wall time, and total A/B wall time before
considering any seam removal.

## Benchmark and acceptance

Run:

`outputV2/notebook/benchmark_usp_apply_investment_level_rounding.py`

Use `ResultType=deltalake`, `number_of_runs=2`, `ExecutionOrder=alternate`, and
`ProfilePlan=on`. Leave `CheckpointMode=default` to inherit Common V2, or select an
explicit mode for a controlled comparison. Acceptance requires exact schema, row count,
null-count, numeric-sum, and order-independent fingerprint parity for every mutated
table in both orders, plus an improvement beyond normal run variance.

Local verification is limited to syntax and static invariant checks; Spark parity and
timings require the Databricks data/catalog environment.
