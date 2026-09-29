---
name: optimize-usp-load-lookthrough-allocation-input
description: Optimizes the Spark implementation of uspLoadLookThroughAllocationInput by applying Checkpoint V2 on the two production seams, parallel independent loads and K1/adjustment/LT builders, and an output_writes phase for distinct result-table appends. Use when implementing, benchmarking, diagnosing, or extending performance work for this specific SP.
---

# Optimize uspLoadLookThroughAllocationInput

Use this skill only for:

`AllocationV2/usp_load_lookthrough_allocation_input`

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

Do not copy FEP CPBT / yearly / three-table FEP rewrites or footnotes
`cost_snapshot` / `entity_levels` / `alloc_passN` into this SP. Do not add
a `business/` folder.

## Paths

- Production baseline:
  `Source/AllocationV2/usp_load_lookthrough_allocation_input/output/`
- Optimized implementation:
  `Source/AllocationV2/usp_load_lookthrough_allocation_input/outputV2/`
- Shared checkpoint implementation:
  `Source/Common_V2/core/checkpoint_V2.py`
- Benchmark:
  `Source/AllocationV2/usp_load_lookthrough_allocation_input/outputV2/notebook/benchmark_load_lookthrough_allocation_input.py`

Public entry:

`run_load_lookthrough_allocation_input`

## Non-negotiable contracts

1. Treat production `output` behavior as authoritative. Keep S1–S21 order
   of business stages even when independent stages run concurrently.
2. Mutated / compared tables (snapshot and hash all of them):
   - `LookThroughAllocationInput` (`RunID`)
   - `SchKTaxableIncome` (`UpperTierRunID`)
   - `PFICtoK1IncomeAttributePercentages` (`RunID`)
   - `AllocationRunErrors` (`RunID`)
   - `AllocationRun` (`RunID`)
3. Require exact fingerprints: schema, row count, key nulls, decimal
   sums, order-independent `xxhash64`.
4. SKIPPED when `RunStatus=FAIL`. FAIL when K3 validation fails; do not
   run `output_writes` after a K3 FAIL.
5. Keep both production checkpoint seams:
   `alloc_input_post_unions`, `alloc_input_post_pfic`. Do not drop them
   for high fan-out (9 and 7 consumers). Do not add coalesce/repartition
   or hot-path drop.
6. FX rates stay **sequential**. `build_fx_rates` creates a temp view.
7. PFIC elections → mapped lines → conversion → income attributes stay
   **sequential**. They share election/mapping/lower-tier state.
8. Union pipeline, Box JKL through line exclusions, and AllocationRun /
   K3 status updates stay **sequential**.
9. Never use a runtime improvement from a failed or non-parity run.
10. Do not leave `__pycache__` or `.pyc` files in the repository.
11. Mode 1 Production: no plan profiler. Mode 2 Development: slim profiler
    default off. Do not add a ProfilePlan widget.

## Target configuration

Use these promoted defaults unless a new exact-parity benchmark disproves
them.

- `ExecutionProfile=low` (orchestrator-resolved at run start)
- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- ParallelGroups `all` includes:
  - `independent_early_loads`
  - `independent_input_builders`
  - `output_writes`
- production checkpoints: **keep** (`alloc_input_post_unions`,
  `alloc_input_post_pfic`)
- extra Checkpoint V2 seams (reused frames, footnotes-style plan
  breaks): `lower_tier_funds`, `reclass_k1`, `fx_rates`,
  `lower_tier_amount`, `pfic_mapped`, `alloc_input_box_jkl`,
  `alloc_input_pre_write`
- speculative broadcasts / extra caches: **off** unless measured
- FEP / footnotes-only flags: **do not apply**

## Notebook widgets (frozen — identity override)

Exactly the parent-skill 10 widgets. This SP overrides identity from
the QA job (`common_params_json`): EntityID `4755`, ClientID `15348`,
TaxPeriodID `1`, RunID `18266`, CatalogName `qa7`. Job `SchemaName` is
null; the client schema widget is still required:
`iPC_2025_QA7_15348`. `removeAll()` in its own cell.

Do **not** add MaxThreads / shuffle / CheckpointMode / ProfilePlan /
VolumePath / ResultType widgets. Hardcode ResultType `deltalake` and
VolumePath `/Volumes/qa7/datavolume/databrickdata`. Pass
`ExecutionProfile` only to updated. Inspect live columns before
`WHERE RunID` / `UpperTierRunID`.

## Implementation workflow

### 1. Establish the production baseline

Read `output/load_lookthrough_allocation_input.py` and the `lt_*`
services. Record DAG, checkpoints, temp views, and every write.

Run production first. Capture wall time, reported time, and all five
table fingerprints.

### 2. Build or repair outputV2 isolation

Leave production `output/` unchanged unless the user asked for
Production mode. In Development mode, `outputV2/` must:

- import helpers via `parent.py` (`output_module("lt_…")`);
- keep `run_load_lookthrough_allocation_input`;
- copy **no** production business modules except orchestrator-local
  helpers (`parallel_helpers.py`, `write_helpers.py`);
- resolve `ExecutionProfile` in the orchestrator at run start (default
  `low`).

Required modules:

- `load_lookthrough_allocation_input.py`: public API, Checkpoint V2,
  profile apply, parallel phases
- `parent.py`: live `output/` imports
- `parallel_helpers.py`: bounded pools, START/DONE, observe every future
- `write_helpers.py`: distinct-table appends for `output_writes`
- `output_reconcile.py`: snapshot / restore / hash all five tables;
  inspect live columns before `WHERE RunID`
- `plan_profiler.py`: slim shim
- `notebook/benchmark_load_lookthrough_allocation_input.py`: frozen 10
  widgets (Entity 4755 / Run 18266 / catalog `qa7`)

Do not create `output/updated/` or a package-root `notebooks/` folder.

### 3. Apply proven parallel phases

- **`independent_early_loads`**: `load_workflows`,
  `load_lower_tier_funds`, `load_reclass_k1_data`. Isolated `cfg`. No
  writes, no temp views.
- **`independent_input_builders`**: after FX exists,
  `build_k1_input`, `build_adjustments_input`, `build_lt_flowup_k1`.
- **`output_writes`**: after K3 SUCCESS, parallel append
  `LookThroughAllocationInput`, `SchKTaxableIncome`, and optional
  `PFICtoK1IncomeAttributePercentages`. Writer constructed in-task.
  Keep the LookThrough `.count()` log **inside** that task.

Wave time is `max(task)`, not the sum. Cap workers 1..4.

### 4. Apply Checkpoint V2

Use only `Common_V2.core.checkpoint_V2`. Initialize once before pools.
Keep `alloc_input_post_unions` and `alloc_input_post_pfic`. Extra V2
seams on reused frames (`lower_tier_funds`, `reclass_k1`, `fx_rates`,
`lower_tier_amount`, `pfic_mapped`, `alloc_input_box_jkl`,
`alloc_input_pre_write`). After a local backend, `toDF(*columns)`.

### 5. Validate

Syntax-check. Then run the Databricks A/B notebook on an isolated RunID:

1. snapshot all five tables for this RunID;
2. restore AllocationRun and purge generated rows before each variant;
3. original (`output`) vs updated (`outputV2`);
4. compare every table; restore original state in `finally`.

Reject the candidate if any table differs.

## Benchmark interpretation

Use wall-clock critical paths, not summed task duration.

Databricks catalog parity is **not yet accepted** for this candidate.
Do not lock a wall time until both execution orders PASS hashes and the
updated wall beats run-to-run noise.

Writes were the largest sequential drivers. `output_writes` exists so
those appends are not missed in a later rewrite.

## Known bad experiments

Do not promote these without a new isolated experiment:

- dropping `alloc_input_post_unions` or `alloc_input_post_pfic`;
- parallelizing `build_fx_rates`;
- parallelizing the PFIC chain or the union/finalization pipeline;
- running `output_writes` before K3 SUCCESS;
- parallelizing AllocationRun / AllocationRunErrors with result tables;
- broadcasting unproven-large lookups;
- FEP SkipYearlyEmptyProbe / CPBT fusion / footnotes plan-breaks.

## Completion report

Report:

- production and outputV2 wall times;
- exact-parity result for all five tables;
- which parallel groups ran (`independent_early_loads`,
  `independent_input_builders`, `output_writes`);
- checkpoint names and backends;
- ExecutionProfile / CheckpointMode / shuffle / MaxThreads loaded;
- changes retained or reverted.
