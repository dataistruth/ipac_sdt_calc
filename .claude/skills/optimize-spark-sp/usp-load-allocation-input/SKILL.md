---
name: optimize-usp-load-allocation-input
description: Optimizes the Spark implementation of uspLoadAllocationInput by applying Checkpoint V2 on production seams, parallel form/K1/PFIC-snapshot builders, and an output_writes collect phase before the sequential Delta-then-Parquet flush. Use when implementing, benchmarking, diagnosing, or extending performance work for this specific SP.
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
2. Compared tables (snapshot and hash all that exist):
   - `AllocationInput`
   - `PFICFootnoteFlowup`
   - `PFICFootnoteFlowupWithTrackingKey`
   - `Form926Flowup`, `Form199AFlowup`, `Form8865Flowup`, `Form8886Flowup`
   - `AtRiskFlowup`, `CustomFootnoteFlowup`
   - `PFICUpdateAlert`, `PFICAlertDetails`
   - `AllocationRunErrors`
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
8. Disk flush stays **sequential**: AllocationInput Delta `replaceWhere`
   first, then GenericResultStorer Parquet. Do not invert that order.
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
- **`output_writes`**: after tags, collect frames for AllocationInput,
  PFIC flowup tables, and form flowups on isolated `_parquet_results`,
  then merge. Then flush Delta then Parquet on the main thread.

Wave time is `max(task)`. Cap workers 1..4.

### 4. Validate

Syntax-check. Run the Databricks A/B notebook on an isolated RunID.
Reject if any table hash differs.

## Known bad experiments

- dropping `pfic_snapshot` / `alloc_input` / `pfic_raw` / `pfic_flowup` /
  `alloc_filtered` without a new A/B;
- parallelizing `build_custom_footnote_input` or `build_entity_hierarchy`;
- parallelizing the PFIC flowup / election-delete chain;
- running Parquet storer concurrently with the AllocationInput Delta write;
- copying FEP SkipYearlyEmptyProbe or footnotes plan-breaks.

## Completion report

Report production vs outputV2 wall times, per-table hash result, which
parallel groups ran, checkpoint names, and ExecutionProfile loaded.
