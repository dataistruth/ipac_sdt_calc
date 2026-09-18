# Look-Through Allocation Input outputV2

## Scope

The public entry remains
`outputV2/load_lookthrough_allocation_input.py::run_load_lookthrough_allocation_input`.
Production `output/` and the `sdt_d` source are unchanged. Unmodified business
services are loaded through `parent.py`; orchestration changes are isolated here.

The candidate uses only `Common_V2.core.checkpoint_V2`, calls
`initialize_checkpoint_V2` once before any pool, and inherits the single
shared default through `resolve_checkpoint_mode`. It has
no checkpoint profile/bypass, coalesce/repartition control, or hot-path cleanup.

## Complete dependency map

1. Common config -> SP config aliases and TechConfig action -> RunStatus gate.
2. Independent lazy loads: workflows, current RunID lower-tier funds, and
   current RunID reclass K1 data.
3. FX rates depends on K1 workflows.
4. K1 input depends on K1 workflow + FX; adjustments depend on adjustment
   workflow + FX; lower-tier K1 depends on reclass data.
5. Rounding depends on lower-tier K1, then all K1/adjustment/lower-tier sources
   enter one ordered `alloc_input_df` union pipeline.
6. Adjustment and M1 flow-up each depend on lower-tier funds and append
   sequentially; the first production checkpoint follows.
7. PFIC elections -> PFIC mapped lines -> PFIC conversion -> PFIC income
   attributes. These share election/mapping/lower-tier inputs and remain ordered.
8. PFIC rows append to the shared allocation input; the second production
   checkpoint follows.
9. Box JKL -> master-feed exclusion -> blocker logic -> tag percentages ->
   line exclusions -> gating K3 validation -> final writes.
10. Final writes append LookThroughAllocationInput, SchKTaxableIncome, and
    optionally PFICtoK1IncomeAttributePercentages. K3 may append
    AllocationRunErrors and update AllocationRun. Every mutation is sequential.

## Thread pools and safety

Both pools cap workers at `min(MaxThreads, task_count, 4)` and deterministically
return results in declared task order.

- `independent_early_loads` (3 tasks): `load_workflows`,
  `load_lower_tier_funds`, and `load_reclass_k1_data`. They only construct
  disjoint, RunID-pruned DataFrames and do not mutate cfg, create temp views, or
  write.
- `independent_input_builders` (3 tasks): `build_k1_input`,
  `build_adjustments_input`, and `build_lt_flowup_k1`. Their prerequisites are
  complete, they consume immutable DataFrames/cfg, and return disjoint plans.
  The first two may run independent lookup/existence actions.

FX construction stays outside the pools because it creates a temp view consumed
by its own SQL. The shared allocation union, PFIC pipeline, gating validation,
all finalization, and all RunID writes remain sequential.

## Run pruning, joins, and reuse

Production already pushes `RunID` into AllocationInputWorkflow,
LowerTierFunds, ReclassK1LookThroughAllocationData, PFIC flow-up,
ReclassFootnoteAllocationData, and ReclassBoxJKL reads. Those filters are
preserved. Existing client/tax-period pruning and bounded broadcasts are also
preserved. No speculative broadcast or cache was added: several named lookup
tables are not proven bounded globally, and no new repeated-action cache has
measured benefit.

## Checkpoint parity

Exactly two production seams remain:

| Sequence | Seam | Known downstream use | Candidate |
| --- | --- | --- | --- |
| 1 | `alloc_input_post_unions` | PFIC mapping/conversion plus finalization (9 stated consumers) | `checkpoint_V2` |
| 2 | `alloc_input_post_pfic` | ordered finalization, validation, and writes (7 stated consumers) | `checkpoint_V2` |

When selected, mode 2 uses odd localCheckpoint and even stats-off Delta through
the shared implementation. There is no coalesce, repartition, drop, lean
profile, or bypass. Initial recommendation is **keep pending measurement** for
both due to high fan-out; the notebook records incoming
nodes/depth/operator mix and materialization time before any direct removal can
be considered.

## Telemetry

`ProfilePlan=on` records and prints separate BUILDER, CHECKPOINT, and ACTION
reports. Builders include every orchestrator-visible DataFrame producer,
including tuple/dict results. ACTION captures gating K3 validation and the final
write service wall time against the incoming allocation plan. The run also
exposes section, task/pool, and checkpoint activity via
`get_last_run_profile()`.

## Benchmark and reconciliation

`notebook/benchmark_load_lookthrough_allocation_input.py` moves `source_path`
to the front, evicts production/outputV2/profiler/Common_V2 modules, proves the
V2 checkpoint import, alternates execution order, and snapshots all RunID state:

- LookThroughAllocationInput (`RunID`)
- SchKTaxableIncome (`UpperTierRunID`)
- PFICtoK1IncomeAttributePercentages (`RunID`)
- AllocationRunErrors (`RunID`)
- AllocationRun (`RunID`)

Before each variant it restores AllocationRun and purges generated rows. In a
`finally` block it restores all original rows; backups remain if restoration
fails. Parity includes schema, row count, key nulls, decimal business sums, and
an order-independent row fingerprint. It displays timing delta, per-table
parity, checkpoint/parallel/section telemetry, all three plan profiles, and
checkpoint recommendations.

## Validation status

Local validation is limited to source review and Python syntax compilation.
Databricks catalog parity and performance are not yet accepted. Run the
benchmark with a valid isolated RunID. Acceptance requires matching
fingerprints in both orders and improvement beyond normal run-to-run variance.
