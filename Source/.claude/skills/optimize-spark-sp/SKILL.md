---
name: optimize-spark-sp
description: Optimizes Databricks PySpark AllocationV2 stored procedures in two modes. Production (mode 1) applies inline changes in output/ with a run notebook only—no plan profiler, no updated/, no business/. Development (mode 2) creates outputV2 with a side-by-side A/B notebook, optional plan profiler slim shim, and per-table validation hashes. Both modes use Checkpoint V2 and the same business logic. Use when optimizing an AllocationV2 SP, adding parallel thread phasing, or preparing an SP for production output or outputV2.
---

# Optimize Spark Stored Procedure

Read [implementation-reference.md](implementation-reference.md) before
editing. Read [benchmark-reference.md](benchmark-reference.md) before
creating a notebook.

The **pipeline logic is the same in both modes**: Checkpoint V2, parallel
thread phasing, isolated `cfg`, RunID pruning, bounded broadcasts, and
preserved join/filter/write semantics. Only packaging and notebooks differ.

## Execution profile at orchestrator start

The **SP orchestrator** (not `Common_V2.core.__init__`, not the notebook)
imports the tier map and applies it once at the start of `run_*`, after
`cfg` exists and **before** checkpoint init and before worker threads
copy `cfg`. Default tier is **`low`**.

```python
from Common_V2.core.execution_profiles import resolve_execution_profile

# low    shuffle 32  CheckpointMode 4  MaxThreads 4
# medium shuffle 48  CheckpointMode 2  MaxThreads 4
# big    shuffle 64  CheckpointMode 1  MaxThreads 4
profile = resolve_execution_profile(
    ExecutionProfile or execution_profile or "low"
)
```

Then apply `profile["shuffle_partitions"]`, `profile["checkpoint_mode"]`,
and `profile["max_threads"]`. Explicit `SqlShufflePartitions`,
`CheckpointMode`, and `MaxThreads` override the profile. Do not set AQE.
Do not `from Common_V2.core import resolve_execution_profile`.

## Choose the mode

| User says | Mode |
|---|---|
| production, inline, main `output/`, ship it | **1 Production** |
| development, outputV2, side by side, A/B, profiler | **2 Development** |
| (unspecified) | **2 Development** until they ask to land in production |

Never mix modes in one change set. Never add `updated/`, `business/`, or
`tests/` folders in either mode. Development is always a **flat**
`outputV2/` (every `.py` in that folder, plus `notebook/`). Production
is always **inline** in `output/` (same optimizations, no `outputV2/`).

## SP-specific skills

Use the parent skill for packaging, Checkpoint V2, phasing, profiles,
and notebooks. Use the child skill when the work is that SP:

| SP | Skill |
|---|---|
| `usp_get_final_effective_percentage` | [usp-get-final-effective-percentage/SKILL.md](usp-get-final-effective-percentage/SKILL.md) — Production must keep concurrent `mode_prep`, `mode_prep_boundaries`, post-miss/final-cost, `effective_inputs` (`de_post_minq_fused` / `cost_pct_min_q_fused`), `output_build`; CheckpointMode 4 alone is not enough (~71s vs locked ~54s) |
| `usp_load_footnotes_allocation_to_output` | [usp-load-footnotes-allocation-to-output/SKILL.md](usp-load-footnotes-allocation-to-output/SKILL.md) |
| `usp_load_lookthrough_allocation_input` | [usp-load-lookthrough-allocation-input/SKILL.md](usp-load-lookthrough-allocation-input/SKILL.md) |
| `usp_load_lookthrough_footnote_effective_allocation_pct` | [usp-load-lookthrough-footnote-effective-allocation-pct/SKILL.md](usp-load-lookthrough-footnote-effective-allocation-pct/SKILL.md) |
| `usp_load_lookthrough_cost_alloc` | [usp-load-lookthrough-cost-alloc/SKILL.md](usp-load-lookthrough-cost-alloc/SKILL.md) |
| `usp_load_allocation_input` | [usp-load-allocation-input/SKILL.md](usp-load-allocation-input/SKILL.md) |
| `usp_add_allocation_summary` | [usp-add-allocation-summary/SKILL.md](usp-add-allocation-summary/SKILL.md) |
| `usp_add_lookthrough_alloc_detail_step01` | [usp-add-lookthrough-alloc-detail-step01/SKILL.md](usp-add-lookthrough-alloc-detail-step01/SKILL.md) |
| `usp_apply_investment_level_rounding` | [usp-apply-investment-level-rounding/SKILL.md](usp-apply-investment-level-rounding/SKILL.md) |
| `usp_load_k3_allocation_summary` | [usp-load-k3-allocation-summary/SKILL.md](usp-load-k3-allocation-summary/SKILL.md) |
| `usp_sm_load_lookthrough_cost_allocation_to_output` | [usp-sm-load-lookthrough-cost-allocation-to-output/SKILL.md](usp-sm-load-lookthrough-cost-allocation-to-output/SKILL.md) |
| `usp_sm_load_lookthrough_effective_allocation_pct` | [usp-sm-load-lookthrough-effective-allocation-pct/SKILL.md](usp-sm-load-lookthrough-effective-allocation-pct/SKILL.md) |

The child skill owns SP-only checkpoints, extra tables, and locked
timings. Do not copy those into other SPs. For
`usp_load_allocation_input`, Development generate must reproduce the
locked `outputV2` in that child skill (no `form_flowup_collect.py`,
collect FormFlowups as one task, keep `pfic_raw`, AllocationInput Delta
then parallel `output_writes`). For look-through allocation input,
Development generate must reproduce the locked `outputV2` in that child
skill (no `pfic_income_attributes.py`, window PFIC `%` in
`write_helpers.py`, extra Checkpoint V2 seams, lazy `__init__.py`,
frozen 10 widgets with RunID `18266`). For look-through cost alloc, Development
generate must keep extra Checkpoint V2 seams
(`temp_alloc_input`, `fep`, `alloc_pass1`–`alloc_pass4`, `alloc_output`)
and sequential Output-then-Input writes.

## Mode 1 — Production

Inline edits in the live package. Ready to run as production.

```text
Source/AllocationV2/<sp_name>/output/
├── __init__.py
├── orchestrator.py              # modified in place (or the SP entry module)
├── parallel_helpers.py          # only if phasing is added
└── notebook/
    └── run_<sp_name>.py         # runs THIS orchestrator only
```

**Do not create:** `outputV2/`, `updated/`, `business/`, `tests/`,
`_pre_opt/`, `plan_profiler.py`, `output_reconcile.py`, or any profiler
shim. Apply the same optimizations **in place** in `output/` (orchestrator
and existing helper modules). Do not nest extra packages.

**Notebook:** single-variant run of `AllocationV2.<sp>.output.<entry>`.
Use the frozen identity widgets plus `ExecutionProfile` only (no A/B
widgets). Wall time and row counts only. The live orchestrator resolves
the profile. **No** original-vs-updated compare, **no** ProfilePlan
widget, **no** table-hash A/B.

## Mode 2 — Development

Leave production `output/` unchanged. Candidate lives in a **flat**
`outputV2/` (no nested `business/`, `tests/`, `updated/`, or
`_pre_opt/`). Import production helpers from the sibling `output/`
package (`parent.py`). Copy a production helper into `outputV2/` only
when that file itself holds gated optimizations (FEP:
`cost_pct_loader.py`, `state_allocation.py`, `pfic_footnotes.py`,
`effective_calc.py`, `entity_hierarchy.py` sit in the `outputV2/` root
and are bound onto the isolated production orchestrator). Keep every
locked optimization; only the folder nesting changes.

```text
Source/AllocationV2/<sp_name>/
├── output/                      # PRODUCTION — do not edit
└── outputV2/                    # FLAT — all candidate .py here
    ├── __init__.py
    ├── orchestrator.py          # candidate (same public API)
    ├── parent.py                # import unchanged prod helpers
    ├── parallel_helpers.py
    ├── plan_profiler.py         # slim shim; no-op unless ProfilePlan on
    ├── output_reconcile.py      # per-table fingerprints / hashes
    ├── write_helpers.py         # optional, same folder
    ├── cost_pct_loader.py       # FEP only: optimized copy in this folder
    └── notebook/
        └── benchmark_<sp_name>.py
```

**Notebook:** side by side. Original =
`AllocationV2.<sp>.output.<entry>`. Updated =
`AllocationV2.<sp>.outputV2.<entry>`. Use **only** the frozen widget
set below. Purge/restore RunID between variants. Compare every output
table: row count, schema, numeric sums, order-independent hash
(`xxhash64` min/max plus `sum(_h.cast("decimal(38,0)"))`). Never
`F.sum("_h")` — ANSI Spark overflowed BIGINT on FEP
(`[ARITHMETIC_OVERFLOW] long overflow`, SQLSTATE 22003). Do **not**
set `spark.sql.ansi.enabled=false` to hide it. `ProfilePlan` widget
default **off**.

Snapshot, purge, restore, and hash **only after reading the live table
columns**. Never `WHERE RunID` unless `RunID` is in that table’s schema.
Declare each compared table with its real key (`RunID`, `UpperTierRunID`,
or ClientID+EntityID+TaxPeriodID). Skip missing tables. Do not put
side-effect tables that lack the declared key on the RunID snapshot list.

The Databricks notebook `source_path` must be the same Source tree that
contains the `outputV2/` just generated. Evict `AllocationV2.<sp>` and
`Common_V2` from `sys.modules` after putting that path first. Do not mix a
new notebook with a stale workspace copy of `output_reconcile.py`.

**Mode 2 display (locked):** never
`spark.createDataFrame(runtime_rows)` / `parity_rows` without a schema.
`original_reported` / `updated_reported` are often `None`, and
`skip_reason` can be empty — Spark then fails with
`[CANNOT_DETERMINE_TYPE]`. Use explicit schemas:

```python
runtime_schema = (
    "pass int, original_wall double, original_reported double, "
    "updated_wall double, updated_reported double, "
    "original_rows long, updated_rows long"
)
parity_schema = (
    "pass int, table string, exact_match string, "
    "orig_rows long, upd_rows long, "
    "orig_hash_sum decimal(38,0), upd_hash_sum decimal(38,0)"
)
display(spark.createDataFrame(runtime_rows, schema=runtime_schema))
display(spark.createDataFrame(parity_rows, schema=parity_schema))
```

**ProfilePlan last cell (Mode 2):** after the parity displays, add a cell
that runs **only** `if profile_plan:`. Import shared
`AllocationV2.plan_profiler` in `load_runner` **before** the SP module.
The last cell reads `AllocationV2.plan_profiler.LAST_PLAN_PROFILE`
first (fallback: SP shim). Display BUILDER / CHECKPOINT / ACTION.
Do **not** call `df.explain` or `measure_plan` in that cell.

CHECKPOINT (keep/measure/remove) is the plan-size source of truth
(incoming plan at each Checkpoint V2 seam). BUILDER only records
outputs that are still fat Catalyst plans (skip 1-node post-checkpoint
scans). ACTION stays empty unless `profile_action` wraps actions.

`measure_plan` uses Catalyst `_explain_string` / `_jdf` only when the
widget is on. Never count the Connect `_plan` proto as node counts
(that reports `nodes=1`). If explain/analyze fails
(`UNRESOLVED_COLUMN`), skip **that** frame only — never fail the SP.
Do not measure **inputs** to `track_plan` (localCheckpoint strips join
aliases like `D.UnderlyingEntityID`; explaining those inputs fails the
Databricks cell). Do **not** wrap FEP
`compute_effective_percentage_dated` / `non_dated`, `apply_plugging`,
`apply_type_id_update`, `compute_form199a_effective_percentage`, or
`compute_missing_entities`. When the widget is off, call
`disable_plan_profile()`. Pass `profile_plan=True` to updated only
when the widget is on.

Confirm a live import with `[plan-profile] v6-catalyst-explain` then
`armed=True`. Shim `track_plan` fallback must be identity (`return fn`).

## Databricks deploy (A/B)

Copy **all** of these under the notebook `source_path`:

1. `Source/AllocationV2/plan_profiler/` (shared implementation)
2. `Source/AllocationV2/<sp>/output/` (original + helpers
   `outputV2` imports — footnotes needs `join_optimizations.py`)
3. `Source/AllocationV2/<sp>/outputV2/`

Copying only `outputV2/` causes `ModuleNotFoundError` on
`output.join_optimizations` and similar production helpers.

## Production `output/` (locked inline)

Each of the 12 SPs already has Mode 1 inline in `output/`: Checkpoint
V2, `resolve_execution_profile`, locked seams/waves, production run
notebook, **no** `plan_profiler` / `profile_plan`. That tree is
`sdt_d` production plus the locked optimization overlay. Do not
replace it with vanilla `sdt_d` and do not add profiler code.
`outputV2` may differ only by packaging (absolute `output.*` imports,
shim, `track_plan` wraps, start/finish). Business logic must match.

## Frozen notebook widgets (do not add extras)

This set is locked. Next generate must match it. Keep
`# dbutils.widgets.removeAll()` commented in its **own** cell so
widget values persist across Run all; uncomment only to reset
defaults. Then create widgets.

**Mode 2 A/B** — exactly these 10 widgets, in this order, with these
defaults (FEP Development run):

| # | Name | Label | Default |
|---|---|---|---|
| 1 | `source_path` | Source root | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| 2 | `EntityID` | EntityID | `4137` |
| 3 | `ClientID` | ClientID | `15348` |
| 4 | `TaxPeriodID` | TaxPeriodID | `1` |
| 5 | `RunID` | RunID | `17376` |
| 6 | `CatalogName` | Catalog | `QA7` |
| 7 | `SchemaName` | Schema | `iPC_2025_QA7_15348` |
| 8 | `ExecutionProfile` | Execution profile | `low` (`low` / `medium` / `big`) |
| 9 | `number_of_runs` | A/B passes | `1` |
| 10 | `ProfilePlan` | Plan profile | `off` (`off` / `on`) |

**Mode 1** — widgets 1–8 only (no `number_of_runs`, no `ProfilePlan`).

**Do not create widgets for:** `ExecutionOrder`, `MaxThreads`,
`SqlShufflePartitions`, `CheckpointMode`, `PlanCheckpointThreshold`,
`ParallelGroups`, `VolumePath`, `ResultType`, `LineType`, `RankForRule`,
or any other SP-local flag. A/B order is always **original then
updated**. Shuffle, checkpoint mode, and MaxThreads come from
`Common_V2.core.execution_profiles` via the orchestrator.
Hardcode SP-only values in the notebook (cost-alloc: LineType
`K1 with Cost`, RankForRule `0`, ResultType `deltalake`, VolumePath
`/Volumes/qa7/datavolume/databrickdata`).

An SP-specific skill may override **EntityID / RunID / CatalogName /
SchemaName only** when that SP cannot run on the FEP identity. It still
must not add widgets.

Pass `ExecutionProfile` and `ProfilePlan` only to the updated variant.
The notebook does not call `resolve_execution_profile`.

## Required inputs

- SP directory and entry function
- output tables and RunID column
- run parameters and Databricks `source_path`
- **mode 1 or 2**

## Non-negotiable invariants

1. Same business logic in both modes. Do not “optimize” by changing filters,
   joins, schemas, or writes.
2. Checkpoint **only** via `Common_V2.core.checkpoint_V2` (`checkpoint_V2`
   aliased `checkpoint`, `initialize_checkpoint_V2`, `resolve_checkpoint_mode`).
   No local `checkpoint.py`. No `Common_V2.core.checkpoint`.
   `checkpoint_mode=None` / `CheckpointMode=None` then resolve. Initialize
   once before worker threads copy `cfg`.
   Modes: 1=Delta, 2=odd local/even Delta, 3=odd local/even Volume, 4=all
   localCheckpoint. Inherit `DEFAULT_CHECKPOINT_MODE` unless overridden.
   **Production inline:** if builders call module-level `checkpoint` and you
   rebind that name to a `_checkpoint` wrapper (for `toDF` after local
   backends / hierarchy `hier_level_*`), the wrapper must call
   **`checkpoint_V2(...)` by that imported name**, never `checkpoint(...)`.
   Rebinding `checkpoint = _checkpoint` while `_checkpoint` looks up
   `checkpoint` at runtime is infinite recursion (`maximum recursion depth
   exceeded`). Development `outputV2` may patch `output.orchestrator.checkpoint`
   because `_checkpoint` lives in a **different** module. That pattern is
   unsafe when inlined into the same production module.
3. The **SP orchestrator** resolves `ExecutionProfile` (`low`/`medium`/`big`)
   at invocation start. Import
   `Common_V2.core.execution_profiles.resolve_execution_profile` directly.
   Do not resolve or re-export profiles from `Common_V2.core.__init__`.
   Explicit `CheckpointMode`, `SqlShufflePartitions`, and `MaxThreads`
   override the profile. Do not set AQE from a profile.
4. Parallelize only independent work, in **phases**. Cap workers **1..4**.
   Observe every future. Wave time is `max(task)`, not the sum.
   Parallelize **distinct-table writes** in an `output_writes` phase whenever
   tables (or proven-safe partitions) do not depend on each other. Keep
   same-table writes, gating validation, and AllocationRun status updates
   sequential. Build each Spark writer inside its task.
5. No `output/*_updated.py`. No SP-root `notebooks/`.
6. Mode 1: no plan profiler code at all. Mode 2: slim profiler only
   (`AllocationV2.plan_profiler` + shim). Production `output/` must
   stay profiler-free.
   Notebook widget `ProfilePlan` default **off**. No `ExecutionOrder`
   widget.
7. Mode 2: never report success until every table hash matches.
8. Duck-type DataFrames. Do not rename public helper symbols.
9. Do not drop checkpoints on the hot path. Do not coalesce/repartition
   solely to shrink a Delta checkpoint.
10. Preserve production checkpoint seams by default.

## Common optimizations (both modes)

| Optimization | Notes |
|---|---|
| Parallel thread phasing | Independent loads/builders/writes |
| Bounded ThreadPoolExecutor | Max 4; log START/DONE |
| Isolated `cfg` | Parallel tasks that mutate config |
| Checkpoint V2 | Existing seams |
| RunID / ClientID / TaxPeriodID prune | When columns exist |
| Bounded broadcast | Proven-small lookups only |
| DataFrame reuse / persist | Persist only for multi-action; unpersist in `finally` |
| Concurrent distinct-table writes | Required `output_writes` phase when tables are independent; isolated `cfg`; writer constructed in-task |
| Execution profile `low`/`medium`/`big` | Orchestrator resolves shuffle, checkpoint mode, MaxThreads=4 |

Do not copy FEP CPBT/footnote/state rewrites into other SPs.
Do not put AQE in the profile; it stays at the cluster default (`true`).

## Known bad (Checkpoint V2 rebind)

Do **not** inline this Production pattern:

```python
from Common_V2.core.checkpoint_V2 import checkpoint_V2 as checkpoint

def _checkpoint(spark, df, name, cfg):
    result = checkpoint(spark, df, name, cfg)  # looks up module.checkpoint
    ...

mod.checkpoint = _checkpoint  # now checkpoint is _checkpoint → RecursionError
```

Use:

```python
from Common_V2.core.checkpoint_V2 import checkpoint_V2

checkpoint = checkpoint_V2  # builders may call this name

def _checkpoint(spark, df, name, cfg):
    result = checkpoint_V2(spark, df, name, cfg)
    ...

mod.checkpoint = _checkpoint  # safe: wrapper never names the rebound alias
```

Patch a **sibling** module’s `checkpoint` (Development `outputV2` → `output`)
only when the wrapper is defined in the patching module. After local
Checkpoint V2, `toDF(*columns)` stays required.

**Plan profiler:** Mode 2 only. See implementation reference.

## Known bad (A/B hash + display)

Do **not** regenerate these Mode 2 patterns. They fail after a passing
SP run:

```python
F.sum("_h").alias("hash_sum")  # ARITHMETIC_OVERFLOW on xxhash64 BIGINT
spark.conf.set("spark.sql.ansi.enabled", "false")  # hide overflow
display(spark.createDataFrame(runtime_rows))  # CANNOT_DETERMINE_TYPE
display(spark.createDataFrame(parity_rows))
```

Use `F.sum(F.col("_h").cast("decimal(38,0)"))` and the locked display
schemas above. Hash min/max stay on the raw `_h` BIGINT.

## Workflow

1. Map the DAG (stages, actions, `cfg`, checkpoints, writes).
2. Apply phasing + Checkpoint V2 + portable read optimizations in the
   mode’s target package. The orchestrator resolves the execution profile
   at start. Parallelize independent distinct-table writes. Logic stays
   equivalent.
3. Mode 1: add `output/notebook/run_<sp>.py`. Mode 2: add
   `outputV2/notebook/benchmark_<sp>.py` plus reconcile hashes.
4. Syntax-check. Mode 2: require per-table hash parity before claiming a win.

## Completion summary

- mode (1 Production or 2 Development)
- files created or changed
- phases and why they are independent
- checkpoint V2 usage and which execution profile the orchestrator applied
- Mode 2: per-table hash result; profiler on/off

## Examples

User: “Production mode on `usp_example`.”

→ Edit `output/`, add `output/notebook/run_usp_example.py`. No profiler,
no `outputV2`.

User: “Development mode on `usp_example`.”

→ Unchanged `output/`. Create `outputV2/` with slim profiler and A/B
notebook that hashes each table.
