---
name: optimize-usp-load-allocation-input
description: Optimizes the Spark implementation of uspLoadAllocationInput by applying Checkpoint V2 on production seams, parallel form/K1/PFIC-snapshot builders, a parallel output_collect phase, and parallel per-table flow-up writes after the AllocationInput Delta commit. Use when implementing, benchmarking, diagnosing, or extending performance work for this specific SP.
---

# Optimize uspLoadAllocationInput

Use this skill only for:

`AllocationV2/usp_load_allocation_input`

The production implementation is the correctness baseline. This SP-specific
skill keeps the candidate in `outputV2`. Portable two-mode generation
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

Do not copy FEP CPBT / yearly / three-table FEP rewrites, footnotes
`cost_snapshot` / `entity_levels` / `alloc_passN`, or look-through FX /
K3 write lists into this SP. Do not add a `business/` folder.

## Paths

- Production baseline:
  `Source/AllocationV2/usp_load_allocation_input/output/`
- Optimized implementation:
  `Source/AllocationV2/usp_load_allocation_input/outputV2/`
- Shared checkpoint implementation:
  `Source/Common_V2/core/checkpoint_V2.py`
- Benchmark:
  `Source/AllocationV2/usp_load_allocation_input/outputV2/notebook/benchmark_load_allocation_input.py`

Public entry:

`run_load_allocation_input`

## Non-negotiable contracts

1. Treat production `output` behavior as authoritative. Keep stage order
   of business results even when independent builders run concurrently.
2. Compared tables (snapshot and hash when the table exists **and** the
   declared key is on the live schema):
   - `AllocationInput` (`RunID`)
   - `PFICFootnoteFlowup` (`RunID`)
   - `PFICFootnoteFlowupWithTrackingKey` (`RunID`)
   - `Form926Flowup`, `Form199AFlowup`, `Form8865Flowup`, `Form8886Flowup` (`RunID`)
   - `AtRiskFlowup`, `CustomFootnoteFlowup`, `Form200616Flowup` (`RunID`)
   - `AllocationRunErrors` (`RunID`)
   Do **not** snapshot `PFICUpdateAlert` or `PFICAlertDetails` with
   `WHERE RunID`. Those tables have no `RunID`. Inspect columns first;
   skip if the key is absent. Sync the Databricks `source_path` tree
   (notebook + `output_reconcile.py`) before running A/B.
3. Require exact fingerprints: schema, row count, key nulls, decimal
   sums, order-independent `xxhash64`.
4. FAIL when `RunStatus=FAIL` or validations return False. Do not write
   result tables after those gates.
5. Keep production orchestrator checkpoint seams:
   `pfic_snapshot`, `alloc_input`, `pfic_raw`, `pfic_flowup`,
   `alloc_filtered`, and `alloc_tagged` when the tag workflow is on.
   Production helpers may still call the legacy checkpoint helper
   (`reclass_data`, inner `base_flowup`); do not drop those by editing
   `output/`.
6. `register_shared_views`, hierarchy temp view `_entity_hierarchy_*`,
   FX-unrelated shared views, and `build_custom_footnote_input` (temp
   view `_cf_latest_txn_*`) stay **sequential**.
7. PFIC flowup, election deletes, Part V/VII flags, filters, and tags
   stay **sequential**.
8. `AllocationInput` Delta `replaceWhere` stays **first**. The remaining
   distinct flow-up tables write in `output_writes` (cap 4). Do not batch
   them through one `GenericResultStorer.save_results` call.
9. Never use a runtime improvement from a failed or non-parity run.
10. Do not leave `__pycache__` or `.pyc` files in the repository.
11. Mode 1 Production: no plan profiler. Mode 2 Development: slim profiler
    off unless ProfilePlan is on.

## Target configuration

- `ExecutionProfile=low` (orchestrator-resolved at run start)
- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- ParallelGroups `all` includes:
  - `independent_input_builders`
  - `output_collect`
  - `output_writes`
- production orchestrator checkpoints: **keep**
- FEP / footnotes / look-through-only flags: **do not apply**

## Implementation workflow

### 1. Production baseline

Read `output/load_allocation_input.py` and `ai_*` services. Record
checkpoints, temp views, `_parquet_results` keys, and the two-step flush.

### 2. outputV2 isolation

Leave production `output/` unchanged unless the user asked for
Production mode. Development `outputV2/` imports helpers via `parent.py`.
Required modules: orchestrator, `parallel_helpers.py`, `write_helpers.py`,
`output_reconcile.py`, slim `plan_profiler.py`, A/B notebook.

Resolve `ExecutionProfile` in the orchestrator at run start (default
`low`). Use Checkpoint V2 on orchestrator seams.

### 3. Proven parallel phases

- **`independent_input_builders`**: after validations,
  `build_all_form_inputs`, `build_k1_and_related_inputs`, and
  `build_pfic_snapshot`. Isolated `cfg`. No temp views in these three.
- **`output_collect`**: after tags, collect AllocationInput, PFIC flowup,
  and all form flowups as three isolated `_parquet_results` tasks, then
  merge.
- **`output_writes`**: after AllocationInput commits, write each remaining
  distinct flow-up table concurrently. Writer constructed in-task.
  `replaceWhere RunID` when the frame has `RunID`.

Wave time is `max(task)`. Cap workers 1..4.

### 4. Parallel flow-up writes (required)

Production batches the flow-up tables through one
`GenericResultStorer.save_results` call, which writes them one after
another. Replace that batch in `outputV2/write_helpers.py`:

1. Write `AllocationInput` first (Delta `replaceWhere RunID`) on the main
   thread.
2. Submit every remaining `_parquet_results` table under `output_writes`:
   - `PFICFootnoteFlowup`
   - `PFICFootnoteFlowupWithTrackingKey`
   - `Form926Flowup`
   - `Form199AFlowup`
   - `Form8865Flowup`
   - `Form8886Flowup`
   - `AtRiskFlowup`
   - `CustomFootnoteFlowup`
   - plus `Form200616Flowup`, `PFICUpdateAlert`, `PFICAlertDetails` when
     present in `_parquet_results`
3. Each task aligns to the target schema, applies `coalesce(1)` for
   production `SMALL_TABLES`, and builds its own writer. Use Delta
   `replaceWhere RunID` when the frame has `RunID`; otherwise fall back to
   a single-table `GenericResultStorer.save_results` call.
4. Observe every future. Raise after all tasks finish if any failed.

Evidence: 2026-09-28 RunID `16560` production store 17.2s vs updated
8-table wave 5.7s. The exact-parity updated run completed in 48.4s
(49.2s notebook wall). This is the locked Development candidate.

Expected log shape:

```text
[ok] AllocationInput (delta)
[store] Writing 8 flow-up tables in parallel: ...
[parallel] START phase=output_writes task=Form926Flowup ...
[parallel] output_writes: tasks=8 workers=4 wall=...s critical=max-task
```

If the log still shows `Storing to Delta Tables` with one checkmark per
table from a single storer call, the Databricks `source_path` is running a
stale `write_helpers.py`.

### 5. Validate

Syntax-check. Run the Databricks A/B notebook on an isolated RunID.
Reject if any table hash differs.

## Known bad experiments

- dropping `pfic_snapshot` / `alloc_input` / `pfic_raw` / `pfic_flowup` /
  `alloc_filtered` without a new A/B;
- parallelizing `build_custom_footnote_input` or `build_entity_hierarchy`;
- parallelizing the PFIC flowup / election-delete chain;
- splitting production `write_form_flowups` into generated per-table code;
- overlapping the AllocationInput Delta write with flow-up writes;
- dropping orchestrator `pfic_raw` after production `7a-2`;
- batching tables through one sequential
  `GenericResultStorer.save_results` call;
- copying FEP SkipYearlyEmptyProbe or footnotes plan-breaks.

## Completion report

Report production vs outputV2 wall times, per-table hash result, which
parallel groups ran, checkpoint names, and ExecutionProfile loaded.
Include the `output_writes` wall time next to the ~14s batched-storer
baseline.
