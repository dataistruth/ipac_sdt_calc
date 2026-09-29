---
name: optimize-usp-load-footnotes-allocation-to-output
description: Optimizes the Spark implementation of uspLoadFootnotesAllocationToOutput by applying the proven plan-break checkpoints, S3/S5 parallel planning, S13 dual-table writes, and bounded broadcasts on top of production output. Use when implementing, benchmarking, diagnosing, or extending performance work for this specific SP.
---

# Optimize uspLoadFootnotesAllocationToOutput

Use this skill only for:

`AllocationV2/usp_load_footnotes_allocation_to_output`

The production implementation is the correctness baseline. This SP-specific
skill keeps the locked candidate in `outputV2`. Portable two-mode generation
(Production inline `output/` vs Development `outputV2` A/B) lives in the
parent [optimize-spark-sp](../SKILL.md) skill. On that path the **SP
orchestrator** resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles`; `Common_V2.core.__init__` does not.
Locked defaults (`CheckpointMode=4`, shuffle `32`, `MaxThreads=4`) match
profile `low`.

Never improve benchmark results by weakening business logic, validation,
output persistence, or exact result comparison.

Read [SP_OPTIMIZATION_GUIDE.md](SP_OPTIMIZATION_GUIDE.md) before making a
structural change or interpreting a benchmark.

Do not copy FEP CPBT / mode-prep / yearly / three-table FEP rewrites into
this SP. Development: **flat** `outputV2/` (no `business/`, `tests/`,
`updated/`). Production: **inline** in `output/`.

## Paths

- Production baseline:
  `Source/AllocationV2/usp_load_footnotes_allocation_to_output/output/`
- Optimized implementation:
  `Source/AllocationV2/usp_load_footnotes_allocation_to_output/outputV2/`
- Shared checkpoint implementation:
  `Source/Common_V2/core/checkpoint_V2.py`
- Benchmark:
  `Source/AllocationV2/usp_load_footnotes_allocation_to_output/outputV2/notebook/benchmark_load_footnotes_allocation_to_output.py`

## Non-negotiable contracts

1. Treat production `output` behavior as authoritative. Keep S1–S13 order.
2. This SP **mutates two tables**:
   - insert generated footnote rows into `AllocationOutput`
   - deduct allocated amounts from the selected RunID partition in
     `AllocationInput`
3. Require exact fingerprints on **both** tables: schema, row count,
   decimal amount sums, and order-independent `xxhash64` aggregates.
4. Preserve RunID-scoped writes and production error handling, including
   SKIPPED when preconditions fail or allocation input is empty.
5. Keep production checkpoint seams: `temp_alloc_input`, `all_underlyings`,
   `underlyings_fn`, `alloc_input`.
6. Add the proven extra seams; do not drop production seams to chase time.
7. Never use a runtime improvement from a failed or non-parity run.
8. Do not leave `__pycache__` or `.pyc` files in the repository.
9. Mode 1 Production: no plan profiler. Mode 2 Development: slim profiler
   off unless ProfilePlan is on.

## Target configuration

Use these promoted defaults unless a new exact-parity benchmark disproves
them.

- `ExecutionProfile=low` (orchestrator-resolved)
- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- ParallelGroups: S3 initial plans + S5 cost + S13 writes when workers > 1
- `cost_snapshot` plan break: **on**
- `entity_levels` plan break: **on**
- `alloc_pass1`–`alloc_pass4` plan breaks: **on**
- bounded broadcasts: **on** (keys only; never fact tables)
- footnote shared lineage / FEP-only flags: **do not apply**

## Implementation workflow

### 1. Establish the production baseline

Read `output/orchestrator.py` (or the production entry) and every helper
touched by the change. Record:

- S1–S13 control-flow order;
- actions (`isEmpty`, writes, checkpoints);
- mutable `cfg` keys;
- both write destinations and deduction semantics.

Run production first. Capture wall time, reported time, and both-table
fingerprints.

### 2. Build or repair outputV2 isolation

Leave production `output/` unchanged unless the user asked for Production
mode. In Development mode, create `outputV2/` that:

- reuses production helpers via `parent.py` / `..output.*` (this SP’s
  live `output/` package);
- keeps public API `run_load_footnotes_allocation_to_output`;
- does **not** copy production modules into `outputV2/` except the
  orchestrator and the two SP-local optimization modules.
- resolves `ExecutionProfile` in the orchestrator at run start (default
  `low`).

Required modules:

- `orchestrator.py`: public API, Checkpoint V2, parallel S3/S5/S13, timings
- `plan_break_optimizations.py`: `build_entity_hierarchy` (`entity_levels`)
  and `build_allocation_input` (`alloc_pass1`–`alloc_pass4`)
- `join_optimizations.py`: bounded broadcasts and
  `derive_cost_underlying_types` from `cost_snapshot`
- `output_reconcile.py`: snapshot / restore / hash both mutated tables
- `plan_profiler.py`: slim shim; no-op unless ProfilePlan on
- `notebook/benchmark_load_footnotes_allocation_to_output.py`: A/B

Do not create `output/updated/` or a package-root `notebooks/` folder.

### 3. Apply proven plan-break checkpoints

These extra seams are the SP-specific win. Production
`all_underlyings` (~70s) and `alloc_input` (~25s) were replaying deep
lineage.

- **`cost_snapshot`**: after `build_cost_percentage_data`, checkpoint the
  four-way cost union + `distinct`. Derive `TempCostUnderlyingTypes` from
  that snapshot. Do not keep a second lazy copy of the union.
- **`entity_levels`**: inside `build_entity_hierarchy`, immediately after
  the fixed eight-level `all_levels` union loop, before join-back,
  `distinct`, and downstream unions.
- **`alloc_pass1`–`alloc_pass4`**: inside `build_allocation_input`, each
  immediately after that pass’s left-anti removal, before the next pass
  and the final union.

Use only `Common_V2.core.checkpoint_V2`. No local `checkpoint.py`. No
coalesce/repartition solely to shrink a checkpoint. No hot-path drop.

### 4. Apply proven parallel phases

- **S3**: submit `build_temp_book_effective`,
  `build_temp_allocation_input`, `build_zero_exclude_lines`, and
  `build_temp_final_effective_pct` on a pool capped at 4. Observe every
  future.
- **S5**: submit `build_cost_percentage_data` with those S3 tasks (same
  pool). Shutdown the pool after cost returns. Then checkpoint
  `cost_snapshot`.
- **S4 quarter updates**: stay **sequential**. They share
  `df_temp_alloc_input`.
- **S6–S12**: stay sequential; later stages consume earlier DataFrames.
- **S13**: when `workers > 1` and `df_combined` is not None, write
  `AllocationOutput` and apply `AllocationInput` deduction in parallel
  (2 workers, isolated `cfg` copies). Distinct tables only. This is the
  `output_writes` phase from the parent skill.

Wave time is `max(task)`, not the sum.

### 5. Apply proven bounded broadcasts

Broadcast only:

- quarter-update key sets (via `quarter_join_hints`);
- Part-V allocable lines;
- zero-exclusion lines;
- custom footnote line-type IDs;
- entity-scoped `Partner_Snapshot` lookup (`df_entity_partners`).

Do not broadcast fact AllocationInput / cost / underlyings frames.

### Notebook defaults

Frozen parent-skill Mode 2 widget **names and count** (10 only). This SP
overrides identity because it does not run on the FEP entity:
EntityID `4032`, RunID `18263`, SchemaName `IPC_2025_QA7_15348`.
Hardcode RankForRulePickup `1`. Widget 10 is `ProfilePlan` (`off`/`on`).
No ExecutionOrder / MaxThreads / shuffle / CheckpointMode widgets.

### 6. Validate

Syntax-check. Then run the Databricks A/B notebook on an isolated RunID:

1. snapshot AllocationInput RunID partition and existing generated
   footnote AllocationOutput rows;
2. restore both before every variant;
3. original (`output`) then updated (`outputV2`);
4. compare both tables; restore original state in `finally`.

Reject the candidate if either table differs.

## Benchmark interpretation

Use wall-clock critical paths, not summed task duration.

Production historically ran **~118–160s**. The expensive checkpoints were
`all_underlyings` (~70.6s) and `alloc_input` (~25.2s).

Recorded exact-parity A/B (pre-shared-V2 candidate): original
**149.6s / 161.5s** vs updated **51.2s / 51.5s**. Later shared-V2 runs
were original **144.5s** vs updated **~45–51s** with both-table match.

A recollection of ~32–35s is consistent with CheckpointMode 4 (all
localCheckpoint) on a warm cluster; it is **not** the locked log. Do not
treat 32s as the official bar until a new isolated A/B prints that wall
with both-table PASS.

Do not treat a slower later run as the new baseline.

## Known bad experiments

Do not promote these without a new isolated experiment:

- dropping `temp_alloc_input`, `all_underlyings`, `underlyings_fn`, or
  `alloc_input`;
- skipping `cost_snapshot` or `entity_levels` or any `alloc_passN`;
- parallelizing S4 quarter updates;
- broadcasting fact tables;
- FEP SkipYearlyEmptyProbe / CPBT fusion / three-table reconcile;
- stacking experiments so the extra seams cannot be attributed.

## Completion report

Report:

- production and outputV2 wall times;
- absolute and percentage improvement;
- exact-parity result for **AllocationOutput** and **AllocationInput**;
- which extra checkpoint names actually ran;
- parallel-wave timings (S3, S5, S13);
- ExecutionProfile / CheckpointMode / shuffle / MaxThreads loaded;
- changes retained or reverted.
