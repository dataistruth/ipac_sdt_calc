# Footnote Allocation outputV2

## Scope

This sibling package preserves the production
`uspLoadFootnotesAllocationToOutput` S1-S13 flow and public
`run_load_footnotes_allocation_to_output` entry. Migration changes only the
module segment from `.output.orchestrator` to `.outputV2.orchestrator`.

The SP mutates two tables: it inserts generated footnote rows into
`AllocationOutput` and deducts allocated amounts from the selected RunID
partition in `AllocationInput`.

## Proven checkpoint topology

The mature candidate uses three complementary plan-break groups:

- Top-level `cost_snapshot` materializes the four-way cost union plus
  `distinct`. The underlying-type subset is then rederived from that
  materialized snapshot, preventing repeated replay in hierarchy consumers.
- `entity_levels` is inside `build_entity_hierarchy`, immediately after the
  fixed eight-level `all_levels` union loop and before join-back, `distinct`,
  and downstream unions.
- `alloc_pass1` through `alloc_pass4` are inside
  `build_allocation_input`, each immediately after its pass's left-anti
  removal. They truncate the accumulating anti-join lineage before the next
  pass and final union.

Historical production runs around 118-160 seconds included approximately
70.6 seconds in `checkpoint:all_underlyings` and 25.2 seconds in
`checkpoint:alloc_input`. With the full mature plan-break package, alternating
A/B passes measured original 149.6/161.5 seconds versus updated 51.2/51.5
seconds with output parity. The later roughly 35-second recollection likely
also reflects local/hybrid checkpoint backends. These are prior-candidate
results; this shared-V2 combination still requires its own A/B parity and
performance acceptance.

## Active optimizations

- Production checkpoint seams remain active: `temp_alloc_input`,
  `all_underlyings`, `underlyings_fn`, and `alloc_input`.
- Top-level plan break: `cost_snapshot`.
- Intra-builder plan breaks: `entity_levels`, `alloc_pass1`,
  `alloc_pass2`, `alloc_pass3`, and `alloc_pass4`.
- Shared `Common_V2.core.checkpoint_V2` supplies all checkpoint backends.
  Mode 2 is the default. No checkpoint coalesce/repartition and no hot-path
  cleanup are present.
- S3's four independent lazy plan builders and S5's independent cost plan
  share a pool capped at four workers. Dependent quarter updates remain
  sequential.
- The final AllocationOutput insert and AllocationInput deduction run in
  parallel only when more than one worker is enabled. They write distinct
  tables and were already exercised by the existing candidate.
- Broadcast hints are limited to bounded quarter-update keys, Part-V keys,
  zero-exclusion keys, custom line-type IDs, and the entity-scoped partner
  lookup. Fact inputs are not broadcast.
- Stage timings and optional builder/checkpoint/action plan reports are
  emitted. Profiling is off by default.

## Reconciliation and acceptance gate

The benchmark snapshots the exact pre-run AllocationInput RunID partition and
existing generated footnote AllocationOutput rows. Before every variant it
restores AllocationInput and purges the target generated output rows. It
compares both tables by schema, row count, decimal amount sums, and
order-independent xxhash64 aggregates.

The original state is restored in `finally`. Backup tables are dropped only
after restoration and restoration verification succeed; otherwise their names
are printed and they remain available for recovery.

Acceptance requires:

1. Both affected tables match for every pass.
2. Both execution orders pass when at least two runs are practical.
3. outputV2 improves beyond normal run-to-run variance.
4. Plan/timing evidence confirms the full topology removes both hierarchy and
   multi-pass allocation replay costs.

Run
`outputV2/notebook/benchmark_load_footnotes_allocation_to_output.py` on an
isolated RunID to complete this gate.
