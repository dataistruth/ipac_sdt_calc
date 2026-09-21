# LookThrough Cost Allocation Output — outputV2

## Scope and parity boundary

- Entry: `run_load_lookthrough_cost_alloc`
- Read-only source: `usp_load_lookthrough_cost_alloc_to_output/output/`
- Candidate: `usp_load_lookthrough_cost_alloc_to_output/outputV2/`
- Mutated tables: `LookThroughAllocationOutput` and
  `LookThroughAllocationInput`, both scoped by `RunID`.
- Business filters, joins, schemas, allocation ordering, and write behavior are
  unchanged. This is the non-SM procedure.

## Dependency map

1. Common and SP configuration, followed by the gating FEP eligibility check.
2. Independent loads: partners, line items, look-through input, allocation
   rules, and cost percentages.
3. Optional 704c-to-K1 mapping.
4. Entity hierarchy recursion, rule ordering, book-effective inheritance, and
   look-through input preparation.
5. Final effective percentages and warning validation.
6. Allocation: 704c branch, or sequential by-amount adjustment followed by
   by-percentage allocation.
7. `alloc_output` lineage break.
8. Concurrent writes to distinct tables: append output and overwrite the input
   RunID partition.

## Parallelism

- `lookthrough-load`: up to `MaxThreads` (bounded to 1–4) for five independent,
  read-only plan builders. Each task receives a shallow cfg copy with isolated
  checkpoint state.
- `lookthrough-write`: exactly two workers when both mutations run. This
  preserves the production pool because the targets are distinct tables;
  each writer receives its own cfg copy.
- Hierarchy recursion remains sequential.
- By-amount, input deduction, and by-percentage remain sequential because each
  stage changes the logical input to the next.

## Checkpoints

All production seams are preserved and routed through
`Common_V2.core.checkpoint_V2`. `initialize_checkpoint_V2` runs once, with
the backend selected by `resolve_checkpoint_mode`; only the shared Common V2
default applies when no override is supplied.

- `cost_underlyings`
- `entity_relationship_pruned` (outputV2-only; ClientID/TaxPeriodID-filtered
  `EntityRelationship` before hierarchy recursion)
- each `hierarchy_lvl_<n>`
- `entity_hier_final`
- `all_underlyings`
- `asset_class_rel` when the asset-class path executes
- `alloc_output`

There is no `CheckpointProfile`, no `coalesce`/`repartition` checkpoint
narrowing, and no checkpoint deletion on the hot path.

## Initial checkpoint recommendation

- **Keep `entity_relationship_pruned`**: the filtered relationship frame is
  joined at the base level and every hierarchy iteration. Without this seam,
  each eager `hierarchy_lvl_*` checkpoint re-scans `EntityRelationship`.
  A/B with inherited/default `CheckpointMode` first; isolate the extra seam
  with `CheckpointMode=1` if later mode-2 backends need a clean rematch.
- **Keep `hierarchy_lvl_<n>`**: loop guards consume each level and the next
  iteration depends on it; truncation prevents recursive lineage replay.
- **Keep `cost_underlyings`**: it fans out to the base hierarchy,
  entity-total join, and asset-class branches.
- **Keep `entity_hier_final`**: the unioned recursive hierarchy feeds a
  downstream join.
- **Keep `all_underlyings`**: reused by asset-class checks/filtering and rule
  ordering.
- **Keep `asset_class_rel`**: reused by emptiness/filter branches when present.
- **Keep `alloc_output`**: one plan feeds two concurrent write actions.

These recommendations are dependency-based, not measured acceptance results.
Run the notebook with `ProfilePlan=on` to collect builder delta, depth,
operator mix, checkpoint input size/time, and action timing before considering
any seam removal.

## Profiling and benchmark

The candidate emits separate BUILDER, CHECKPOINT, and ACTION reports. The
mutation-safe Databricks notebook snapshots both affected RunID partitions,
restores them before every variant, compares schema, row count, exact
order-independent hash metrics, and numeric sums, and restores original state
in `finally`.

Notebook:
`outputV2/notebook/benchmark_load_lookthrough_cost_alloc_to_output.py`

Runtime parity and performance are not claimed by this source-only conversion;
acceptance requires successful A/B runs, preferably in both execution orders.
