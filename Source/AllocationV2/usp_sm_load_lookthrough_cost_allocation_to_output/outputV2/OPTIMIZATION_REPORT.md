# SM Look-Through Cost Allocation outputV2

## Scope and compatibility

This package preserves the production public entry
`run_sm_load_lookthrough_cost_allocation_to_output` and its S1-S22 business
flow. Production under `output/` and the `sdt_d` source were not modified.
Unchanged builders are loaded through `parent.py`; the candidate redirects
checkpointed orchestration seams to shared
`Common_V2.core.checkpoint_V2`.

Checkpoint mode is normalized with `normalize_checkpoint_mode`; explicit
PascalCase/snake-case values override cfg and otherwise fall back to mode 2.
`initialize_checkpoint_V2` runs once before any pool.

## Dependency and write ordering

Safe independent work:

- Four lookup-load tasks: bounded state/DAR references, allocation enums,
  underlying-type enum, and the unhinted Entity lookup.
- Five independent source builders with a pool capped at four workers:
  book-effective, current-RunID allocation input, cost percentage snapshot,
  entity asset-class relationship, and DAR state mapping.
- After allocation input exists, entity partners and current-RunID/rank final
  effective percentages are built independently.

Sequential work:

- Cost-underlying derivation and every hierarchy level.
- All-underlyings combination, asset-class filtering, and state ranking.
- Allocation passes 1 through 4 because each consumes the prior pass's
  remaining input/book-effective rows.
- Amount allocation, amount deduction, then percentage allocation.
- `SM_LookThroughAllocationOutput` write before the related
  `SM_LookThroughAllocationInput` deduction write.

Pools never exceed four workers and failures propagate with the failed task
name represented in telemetry.

## Checkpoint topology

All active production seams are retained:

- `hier_level_{n}` after each dependent hierarchy expansion and before its
  emptiness action.
- `cost_pct_snapshot`, which has three downstream consumers.
- `alloc_input_final`, which feeds amount and percentage paths.

No checkpoint profile, bypass list, `coalesce`, `repartition`, or hot-path
checkpoint drop exists. Cleanup remains exported only for explicit debugging.

## Read pruning and broadcasts

- `SM_LookThroughAllocationInput` is pruned by RunID and ClientID before its
  projection, matching production.
- `SM_FinalEffectivePercentages` is pruned by RunID and rule rank before
  projection; ClientID and TaxPeriodID are also pushed when those columns are
  present.
- Partner and book-effective reads retain entity/client/tax/workflow scoping.
- Enum and transaction-filtered reference plans retain bounded broadcast
  hints. Final effective percentages and pass-4 underlyings are scoped to the
  current run/rank or current entity flow before broadcast.
- The unbounded all-client `Entity` lookup and client/tax
  `EntityRelationship` plan are deliberately not forced to broadcast.

No fact table receives a new unbounded broadcast hint.

## Profiling and telemetry

`ProfilePlan` is off by default. When enabled, the run emits complete BUILDER,
CHECKPOINT, and ACTION reports. The saved profile includes:

- section timings,
- per-task and pool wall times,
- checkpoint backend/activity,
- builder plan growth,
- checkpoint input plans,
- explicit hierarchy probes and final write action timing.

Checkpoint recommendations are evidence only. The benchmark combines plan
size/depth/operator mix, known consumer counts, and measured checkpoint time;
it does not automatically remove or add seams.

## Mutation-safe A/B acceptance

Run:

`outputV2/notebook/benchmark_sm_load_lookthrough_cost_allocation_to_output.py`

The notebook:

1. Snapshots both RunID-scoped mutated tables.
2. Restores the exact baseline before each variant.
3. Purges only the output RunID before each call.
4. Alternates original/updated order by default.
5. Compares schema, row count, key null counts, Amount/Amount704b sums, and
   order-independent row fingerprints for both tables.
6. Restores the original state in `finally`; backup snapshots are retained if
   restoration fails.
7. Displays timing, parity, pool, checkpoint, builder/action profile, and
   checkpoint-recommendation reports.

Acceptance requires all-table parity in both execution orders and a wall-time
improvement beyond run variance. No Databricks benchmark was executed locally;
runtime performance and reconciliation remain a deployment acceptance gate.
