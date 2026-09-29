---
name: optimize-usp-load-lookthrough-allocation-input
description: Regenerates or extends the locked Development outputV2 for uspLoadLookThroughAllocationInput (Checkpoint V2, three parallel groups, extra plan-break seams, window PFIC EffPercentage in write_helpers). Use when implementing, generating, benchmarking, or diagnosing this SP. Do not add pfic_income_attributes.py or revert PFIC % to a double-group self-join.
---

# Optimize uspLoadLookThroughAllocationInput

Use this skill only for:

`AllocationV2/usp_load_lookthrough_allocation_input`

When the user asks to **generate**, **optimize in Development mode**, or
**recreate outputV2**, reproduce the locked candidate below. Do not
redesign packaging. Do not add files that are not in the allowed tree.
Portable two-mode rules live in the parent
[optimize-spark-sp](../SKILL.md) skill. Read
[SP_OPTIMIZATION_GUIDE.md](SP_OPTIMIZATION_GUIDE.md) for the DAG.

The **SP orchestrator**
(`outputV2/load_lookthrough_allocation_input.py`) resolves
`ExecutionProfile` from `Common_V2.core.execution_profiles` at run
start. Do not resolve profiles from `Common_V2.core.__init__`. Default
**low**. Do not set AQE. Locked defaults (`CheckpointMode=4`, shuffle
`32`, `MaxThreads=4`) match profile `low`.

Public entry: `run_load_lookthrough_allocation_input`.

Never improve benchmark results by weakening business logic, validation,
output persistence, or exact result comparison. Do not copy FEP CPBT /
yearly rewrites or footnotes `cost_snapshot` / `entity_levels` /
`alloc_passN` into this SP. No `business/`.

## Locked candidate (regenerate this)

Exact-parity A/B, 2026-09-29, RunID `18266`, EntityID `4755`,
ClientID `15348`, TaxPeriodID `1`, catalog `qa7`, schema
`iPC_2025_QA7_15348`, ProfilePlan **off**, ExecutionProfile **low**,
original then updated:

| Metric | Before window PFIC % | Locked updated |
|---|---|---|
| Notebook wall | 23.007s | **18.380s** |
| Reported | 22.9s | **18.3s** |
| `output_writes` wall (critical = PFIC task) | 10.864s | **5.995s** |
| PFIC append task | 10.861s | **5.991s** |
| LookThrough append task | ~2.3s | 2.430s |
| SchK append task | ~2.6s | 1.583s |
| Reconciled rows | 26 | 26 |
| Fingerprints | — | **PASS** (all five tables) |

Treat **18.380s / exact parity / window PFIC %** as the Development bar.
Do not replace this shape with experiments that failed (see Known bad).

Production `output/` is unchanged. Do not lock a production wall from
this run; the bar is the updated candidate plus PASS hashes.

## Allowed outputV2 tree

```text
Source/AllocationV2/usp_load_lookthrough_allocation_input/
├── output/                                 # PRODUCTION — do not edit
│   ├── load_lookthrough_allocation_input.py
│   ├── lt_*.py
│   └── lt_helpers.py
└── outputV2/
    ├── __init__.py                         # lazy export only
    ├── load_lookthrough_allocation_input.py
    ├── parent.py
    ├── parallel_helpers.py
    ├── write_helpers.py                    # output_writes + window PFIC %
    ├── plan_profiler.py
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/
        └── benchmark_load_lookthrough_allocation_input.py
```

**Do not create:** `pfic_income_attributes.py`, `business/`, `updated/`,
`output/*_updated.py`, copies of `lt_*.py` inside `outputV2/`. Business
services stay in production `output/` and are imported via `parent.py`.
Window `EffPercentage` lives in `write_helpers.py` so Databricks Source
trees that already have that file pick it up. `__init__.py` must **not**
eager-import the orchestrator (that pulled a missing submodule and
broke `output_reconcile` import).

`parent.py` must be:

```python
_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"

def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")
```

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
    off unless `ProfilePlan` is on. No `ExecutionOrder` widget.

## Target configuration (locked)

- `ExecutionProfile=low` (orchestrator-resolved at run start)
- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- ParallelGroups `all`:
  - `independent_early_loads`
  - `independent_input_builders`
  - `output_writes`
- production checkpoints: **keep** (`alloc_input_post_unions`,
  `alloc_input_post_pfic`)
- extra Checkpoint V2 seams (reused frames; after local backend
  `toDF(*columns)`): `lower_tier_funds`, `reclass_k1`, `fx_rates`,
  `lower_tier_amount`, `pfic_mapped`, `alloc_input_box_jkl`,
  `alloc_input_pre_write`
- PFIC `%` in `write_helpers.build_pfic_income_attributes`: window
  `sum(Amount)` over EntityID/LineID/TrackingKey; drop null join keys
  to match production inner join. Do **not** group `recalc_grouped`
  twice and join.
- speculative broadcasts / extra caches: **off** unless measured
- FEP / footnotes-only flags: **do not apply**

## Notebook widgets (frozen)

Exactly the parent-skill 10 widgets. Identity override from the QA job:

| # | Name | Default |
|---|---|---|
| 1 | `source_path` | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| 2 | `EntityID` | `4755` |
| 3 | `ClientID` | `15348` |
| 4 | `TaxPeriodID` | `1` |
| 5 | `RunID` | `18266` |
| 6 | `CatalogName` | `qa7` |
| 7 | `SchemaName` | `iPC_2025_QA7_15348` |
| 8 | `ExecutionProfile` | `low` |
| 9 | `number_of_runs` | `1` |
| 10 | `ProfilePlan` | `off` |

Job `SchemaName` is null; the client schema widget is still required.
`removeAll()` in its own cell. No MaxThreads / shuffle / CheckpointMode /
ExecutionOrder / VolumePath / ResultType widgets. Hardcode ResultType
`deltalake` and VolumePath `/Volumes/qa7/datavolume/databrickdata`.
Pass `ExecutionProfile` and `ProfilePlan` only to updated. A/B order is
**original then updated**. Inspect live columns before `WHERE RunID` /
`UpperTierRunID`. Last display cell uses explicit Spark schemas so
`skip_reason=None` does not fail inference.

## Implementation workflow

### 1. Establish the production baseline

Read `output/load_lookthrough_allocation_input.py` and the `lt_*`
services. Record DAG, checkpoints, temp views, and every write.

### 2. Build or repair outputV2 isolation

Leave production `output/` unchanged unless the user asked for
Production mode. Required modules are the allowed tree above.

### 3. Apply proven parallel phases

- **`independent_early_loads`**: `load_workflows`,
  `load_lower_tier_funds`, `load_reclass_k1_data`. Isolated `cfg`.
- **`independent_input_builders`**: after FX exists,
  `build_k1_input`, `build_adjustments_input`, `build_lt_flowup_k1`.
- **`output_writes`**: after K3 SUCCESS, parallel append the three
  distinct tables. Writer constructed in-task. LookThrough `.count()`
  log stays **inside** that task. Wave time is `max(task)`.

### 4. Apply Checkpoint V2

Use only `Common_V2.core.checkpoint_V2`. Initialize once before pools.
Keep production seams plus extra reused-frame seams listed above.

### 5. Validate

Syntax-check. Databricks A/B on RunID `18266`: snapshot five tables,
restore AllocationRun and purge generated rows before each variant,
compare every table, restore in `finally`. Reject if any table differs.

## Known bad experiments

Do not promote these without a new isolated experiment:

- `pfic_income_attributes.py` as a separate module (Databricks Source
  import of `output_reconcile` failed when `__init__` loaded the
  orchestrator);
- PFIC `%` as `recalc_grouped` twice + join `total_amounts` (locked
  window is faster and PASS);
- dropping `alloc_input_post_unions` or `alloc_input_post_pfic`;
- parallelizing `build_fx_rates`;
- parallelizing the PFIC chain or the union/finalization pipeline;
- running `output_writes` before K3 SUCCESS;
- parallelizing AllocationRun / AllocationRunErrors with result tables;
- broadcasting unproven-large lookups;
- FEP SkipYearlyEmptyProbe / CPBT fusion / footnotes plan-breaks;
- eager `__init__.py` import of the orchestrator.

## Completion report

Report production and outputV2 walls when both are in the log, exact
parity for all five tables, parallel groups, checkpoint names, profile
line (`ExecutionProfile` / CheckpointMode / shuffle / MaxThreads), and
whether the window PFIC `%` was kept.
