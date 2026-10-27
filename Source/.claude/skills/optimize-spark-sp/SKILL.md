---
name: optimize-spark-sp
description: Optimizes Databricks PySpark AllocationV2 stored procedures in two modes. Production (mode 1) applies inline changes in output/ with a run notebook only—no plan profiler, no updated/, no business/. Development (mode 2) creates outputV2 with a side-by-side A/B notebook, optional plan profiler slim shim, and per-table validation hashes. Both modes use the shared Common_V2.core checkpoint/parallel/write helpers and the same business logic. Use when optimizing an AllocationV2 SP, adding parallel thread phasing, or preparing an SP for production output or outputV2.
---

# Optimize Spark Stored Procedure

Read [implementation-reference.md](implementation-reference.md) before
editing. Read [benchmark-reference.md](benchmark-reference.md) before
creating a notebook.

The **pipeline logic is the same in both modes**: profile-driven checkpoints,
parallel thread phasing, isolated `cfg`, RunID pruning, bounded broadcasts,
and preserved join/filter/write semantics. Only packaging and notebooks differ.

## Shared Common_V2.core modules (never per-SP copies)

| Module | Owns |
|---|---|
| `Common_V2.core.checkpoint` | `checkpoint`, `drop_checkpoints`, `DELTA_CHECKPOINT_MODE` (1), `LOCAL_CHECKPOINT_MODE` (2), `DEFAULT_CHECKPOINT_MODE` (= Delta) |
| `Common_V2.core.execution_profiles` | `resolve_execution_profile`, `EXECUTION_PROFILES` (`small` / `large`), `DEFAULT_EXECUTION_PROFILE` |
| `Common_V2.core.parallel_helpers` | `normalize_workers`, `parse_enabled_groups(..., known_groups=)`, `isolated_cfg`, `run_parallel`, `run_phase` |
| `Common_V2.core.write_helpers` | `run_isolated_tasks`, `flush_collected_results`, `write_tables_parallel`, `merge_parquet_results`, `align_to_table`, `save_table_via_storer`, `merge_save_values` |

There is no `checkpoint_V2.py` and no SP-folder `parallel_helpers.py` /
`write_helpers.py`. Each SP declares its own `KNOWN_GROUPS` frozenset in the
orchestrator and passes it as `known_groups=`. SP-specific write wrappers
(table lists, PFIC math) live in that SP's existing service module
(allocation input `ai_finalization_service`,
look-through allocation input `lt_finalization_service`) and call the
common write helpers. `checkpoint.py` is the original `sdt_d` file plus one
local mode: `checkpoint()` uses the `checkpoint_mode` argument, else
`cfg["checkpoint_mode"]`; anything other than 4 is the original Delta path,
so every SP outside these 7 keeps its behavior unchanged. Local mode runs
`localCheckpoint(eager=True)` + `toDF(*columns)` (Delta fallback on error).

## Execution profile at orchestrator start

The **SP orchestrator** (not `Common_V2.core.__init__`, not the notebook)
imports the tier map and applies it once at the start of `run_*`, after
`cfg` exists and **before** checkpoint init and before worker threads
copy `cfg`. The default tier is `DEFAULT_EXECUTION_PROFILE` in
`execution_profiles.py` (currently **`small`**); SPs never hardcode it.

```python
from Common_V2.core.execution_profiles import resolve_execution_profile

# small  shuffle 32  CheckpointMode 2 (localCheckpoint)  MaxThreads 4
# large  shuffle 64  CheckpointMode 1 (Delta)            MaxThreads 4
# blank / "default" -> DEFAULT_EXECUTION_PROFILE
profile = resolve_execution_profile(
    _blank(ExecutionProfile) or _blank(execution_profile),
    checkpoint_mode=(
        CheckpointMode if CheckpointMode is not None else checkpoint_mode
    ),
)
profile_name = profile["name"]
mode = profile["checkpoint_mode"]
cfg["checkpoint_mode"] = mode
cfg.setdefault("_checkpoint_tables", [])  # before worker threads copy cfg
```

Then apply `profile["shuffle_partitions"]` and `profile["max_threads"]`.
Explicit `SqlShufflePartitions` and `MaxThreads` override the profile;
`CheckpointMode` is applied by `resolve_execution_profile` (2 = local, anything
else = Delta; unknown profile names → `large`; it never raises). Do not set AQE.
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

Use the parent skill for packaging, checkpoints, phasing, profiles,
and notebooks. Use the child skill when the work is that SP:

| SP | Skill |
|---|---|
| `usp_get_final_effective_percentage` | [usp-get-final-effective-percentage/SKILL.md](usp-get-final-effective-percentage/SKILL.md) — Production must keep concurrent `mode_prep`, `mode_prep_boundaries`, post-miss/final-cost, `effective_inputs` (`de_post_minq_fused` / `cost_pct_min_q_fused`), `output_build`; CheckpointMode 2 alone is not enough (~71s vs locked ~54s) |
| `usp_load_footnotes_allocation_to_output` | [usp-load-footnotes-allocation-to-output/SKILL.md](usp-load-footnotes-allocation-to-output/SKILL.md) |
| `usp_load_lookthrough_allocation_input` | [usp-load-lookthrough-allocation-input/SKILL.md](usp-load-lookthrough-allocation-input/SKILL.md) |
| `usp_load_lookthrough_footnote_effective_allocation_pct` | [usp-load-lookthrough-footnote-effective-allocation-pct/SKILL.md](usp-load-lookthrough-footnote-effective-allocation-pct/SKILL.md) |
| `usp_load_lookthrough_cost_alloc` | [usp-load-lookthrough-cost-alloc/SKILL.md](usp-load-lookthrough-cost-alloc/SKILL.md) |
| `usp_load_allocation_input` | [usp-load-allocation-input/SKILL.md](usp-load-allocation-input/SKILL.md) |
| `usp_add_allocation_summary` | [usp-add-allocation-summary/SKILL.md](usp-add-allocation-summary/SKILL.md) |

The child skill owns SP-only checkpoints, extra tables, and locked
timings. Do not copy those into other SPs. For
`usp_load_allocation_input`, Development generate must reproduce the
locked `outputV2` in that child skill (no `form_flowup_collect.py`,
collect FormFlowups as one task, keep `pfic_raw`, AllocationInput Delta
then parallel `output_writes`). For look-through allocation input,
Development generate must reproduce the locked `outputV2` in that child
skill (no `pfic_income_attributes.py`, window PFIC `%` in
`lt_finalization_service.py`, extra checkpoint seams, lazy `__init__.py`,
frozen 10 widgets with RunID `18266`). For look-through cost alloc, Development
generate must keep extra checkpoint seams
(`temp_alloc_input`, `fep`, `alloc_pass1`–`alloc_pass4`, `alloc_output`)
and sequential Output-then-Input writes.

## Mode 1 — Production

Inline edits in the live package. Ready to run as production.

```text
Source/AllocationV2/<sp_name>/output/
├── __init__.py
├── orchestrator.py              # modified in place (or the SP entry module)
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
`_pre_opt/`). **`outputV2` is self-contained.** Copy every helper the
orchestrator needs into `outputV2/` (keep `services/` only when the SP
already uses that subpackage). Import
`AllocationV2.<sp>.outputV2.<module>` (or relative `.module`). **Never**
import `AllocationV2.<sp>.output.*` from `outputV2`.

`__init__.py` is the **simple lazy export only** (no `_OutputV2Finder`,
no `sys.meta_path` install). That finder broke Databricks
`import-dir --overwrite` (`RESOURCE_ALREADY_EXISTS` when a file is
replaced by a notebook). Do **not** add `# Databricks notebook source`
to helper `.py` files — only the `notebook/benchmark_*.py` files are
notebooks. Keep every locked optimization.

```text
Source/AllocationV2/<sp_name>/
├── output/                      # PRODUCTION — do not edit
└── outputV2/                    # FLAT — all candidate .py here
    ├── __init__.py
    ├── orchestrator.py          # candidate (same public API)
    ├── parent.py                # unused / self package; do not import output/
    ├── plan_profiler.py         # slim shim; no-op unless ProfilePlan on
    ├── output_reconcile.py      # per-table fingerprints / hashes
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
first (fallback: SP shim). Use **one** `display()` of
`build_combined_plan_profile_display` (column `kind` =
checkpoint / builder / action). Databricks keeps one table widget
per cell; three `display()` calls hide CHECKPOINT behind an empty
BUILDER table. Do **not** call `df.explain` or `measure_plan` in
that cell.

CHECKPOINT (keep/measure/remove) is the plan-size source of truth
(incoming plan at each checkpoint seam). BUILDER only records
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
2. `Source/AllocationV2/<sp>/output/` — **old unoptimized baseline**.
   Current ipac `output/` is already optimized production. For A/B
   original, copy or checkout
   `/Users/mukesh.singh/spark/sdt_d/Source/AllocationV2/<sp>/output/`
   into this folder. Do not treat ipac `output/` as “old.”
3. `Source/AllocationV2/<sp>/outputV2/` — optimized Development package.
   **Self-contained:** every helper it needs lives under `outputV2/`
   (or `outputV2/services/`). `outputV2` must not import
   `AllocationV2.<sp>.output.*`. Public entry names stay identical
   to `sdt_d`. The A/B notebook `ORIGINAL` line is the only
   remaining pointer at `output/`.

## Production `output/` (locked inline)

Each of the 7 SPs already has Mode 1 inline in `output/`: Checkpoint
V2, `resolve_execution_profile`, locked seams/waves, production run
notebook, **no** `plan_profiler` / `profile_plan`. That tree is
`sdt_d` production plus the locked optimization overlay. Do not
replace it with vanilla `sdt_d` and do not add profiler code.
`outputV2` is a full copy of the optimized package (helpers included)
plus shim / `track_plan` / start-finish. It does not import `output/`.
Business logic must match production `output/`.

**Production parity check (locked 2026-10-18):** after normalizing
import paths and removing profiler code (`_start_dev_plan_profile`,
`_finish_dev_plan_profile`, `track_plan` wraps, `profile_plan`),
`output/` and `outputV2/` must be identical for all 7 SPs, including
`services/` subpackages. Allowed differences are only import style
(production keeps relative `try/except ImportError` fallbacks;
`outputV2` uses absolute `...outputV2.*` imports) and the profiler. To
promote a Development change, port the normalized diff into `output/`
and keep `output/` free of `plan_profiler`, `output_reconcile`, and
`profile_plan`.

Production run notebooks (`output/notebook/run_*.py`) call only the
`output` entry, take widgets 1–8, and do no data comparison. Keep
`# dbutils.widgets.removeAll()` commented so values passed by
`run_sp_test/run_sp_in_prod_mode` are not discarded.

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
| 8 | `ExecutionProfile` | Execution profile | `default` (`default` / `small` / `large`) |
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

## ProfilePlan propagation contract (Mode 2)

`profile_plan` must be defined at every scope that reads it. A notebook,
entry function, and helper do not share local variables.

1. The entry function declares `profile_plan: bool = False` and accepts
   legacy `ProfilePlan` through `**kwargs`:

```python
profile_plan = bool(
    profile_plan or kwargs.get("ProfilePlan") or kwargs.get("profile_plan")
)
```

2. If `_apply_execution_profile(...)` starts profiling or writes
   `cfg["profile_plan"]`, its signature must declare
   `profile_plan=False`, and the entry function must pass
   `profile_plan` explicitly at the call site.
3. Never reference `profile_plan` in a helper unless it is a parameter.
   This exact omission broke allocation summary with
   `NameError: name 'profile_plan' is not defined`.
4. The A/B notebook must tolerate a stale/missing widget:

```python
try:
    profile_plan = dbutils.widgets.get("ProfilePlan").strip().lower() == "on"
except Exception:
    profile_plan = False
```

Use `if globals().get("profile_plan"):` before adding updated-only
profiler kwargs. ProfilePlan off must never alter or fail the SP.

## Required inputs

- SP directory and entry function
- output tables and RunID column
- run parameters and Databricks `source_path`
- **mode 1 or 2**

## Non-negotiable invariants

1. Same business logic in both modes. Do not “optimize” by changing filters,
   joins, schemas, or writes.
2. Checkpoint **only** via `Common_V2.core.checkpoint` imported as
   `checkpoint as common_checkpoint` plus `drop_checkpoints`. No SP-local
   `checkpoint.py`. No `checkpoint_V2` module or names. Keep `checkpoint.py`
   the original `sdt_d` API (backward compatible for every other SP) plus
   the local mode only — no extra resolver layers there.
   Mode = `profile["checkpoint_mode"]` from `resolve_execution_profile`, stored
   in `cfg["checkpoint_mode"]` with `cfg.setdefault("_checkpoint_tables", [])`
   once before worker threads copy `cfg`.
   Modes: **1 = Delta (default)**, **2 = localCheckpoint(eager)** with Delta
   fallback; `checkpoint()` itself resets qualifiers with `toDF(*columns)`
   after local. Profile `small` → 2; `large` → 1. Development `outputV2`
   wrappers call `track_checkpoint_plan(name, df, cfg)` (from the SP's
   `.plan_profiler`) before `common_checkpoint`; production does not. Legacy direct
   `checkpoint(...)` calls in SP services that were Delta before (e.g.
   allocation input `base_flowup`, `reclass_data`) pass
   `checkpoint_mode=DELTA_CHECKPOINT_MODE` so they stay Delta.
   **Production inline:** if builders call module-level `checkpoint` and you
   rebind that name to a `_checkpoint` wrapper (hierarchy `hier_level_*`,
   dev plan tracking), the wrapper must call
   **`common_checkpoint(...)` by that imported name**, never `checkpoint(...)`.
   Rebinding `checkpoint = _checkpoint` while `_checkpoint` looks up
   `checkpoint` at runtime is infinite recursion (`maximum recursion depth
   exceeded`). Development `outputV2` may patch `output.orchestrator.checkpoint`
   because `_checkpoint` lives in a **different** module. That pattern is
   unsafe when inlined into the same production module.
3. The **SP orchestrator** resolves `ExecutionProfile` (`small`/`large`)
   at invocation start. Import
   `Common_V2.core.execution_profiles.resolve_execution_profile` directly.
   Do not resolve or re-export profiles from `Common_V2.core.__init__`.
   Explicit `CheckpointMode`, `SqlShufflePartitions`, and `MaxThreads`
   override the profile. No hardcoded default tier in SPs or notebooks. Do not set AQE from a profile.
4. Parallelize only independent work, in **phases**, through
   `Common_V2.core.parallel_helpers` / `write_helpers`. Cap workers **1..4**.
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
| Profile checkpoint (`Common_V2.core.checkpoint`) | Existing seams |
| RunID / ClientID / TaxPeriodID prune | When columns exist |
| Bounded broadcast | Proven-small lookups only |
| DataFrame reuse / persist | Persist only for multi-action; unpersist in `finally` |
| Concurrent distinct-table writes | Required `output_writes` phase when tables are independent; isolated `cfg`; writer constructed in-task |
| Execution profile `small`/`large` (default in `execution_profiles.py`) | Orchestrator resolves shuffle, checkpoint mode, MaxThreads=4 |

Do not copy FEP CPBT/footnote/state rewrites into other SPs.
Do not put AQE in the profile; it stays at the cluster default (`true`).

## Known bad (checkpoint rebind)

Do **not** inline this Production pattern:

```python
from Common_V2.core.checkpoint import checkpoint

def _checkpoint(spark, df, name, cfg):
    result = checkpoint(spark, df, name, cfg)  # looks up module.checkpoint
    ...

mod.checkpoint = _checkpoint  # now checkpoint is _checkpoint → RecursionError
```

Use:

```python
from Common_V2.core.checkpoint import checkpoint as common_checkpoint

checkpoint = common_checkpoint  # builders may call this name

def _checkpoint(spark, df, name, cfg):
    result = common_checkpoint(spark, df, name, cfg)
    ...

mod.checkpoint = _checkpoint  # safe: wrapper never names the rebound alias
```

Patch a **sibling** module’s `checkpoint` (Development `outputV2` → `output`)
only when the wrapper is defined in the patching module. After a local
checkpoint, `toDF(*columns)` stays required.

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
2. Apply phasing + profile checkpoints + portable read optimizations in the
   mode’s target package. The orchestrator resolves the execution profile
   at start. Parallelize independent distinct-table writes. Logic stays
   equivalent.
3. Mode 1: add `output/notebook/run_<sp>.py`. Mode 2: add
   `outputV2/notebook/benchmark_<sp>.py` plus reconcile hashes.
4. Syntax-check. Mode 2: require per-table hash parity before claiming a win.
5. Before deployment, statically verify every use of `profile_plan` is
   either in the entry-function scope or in a helper that declares it as
   a parameter. Verify the helper call passes it explicitly.

To time all 7 SPs as a suite, use
`Source/AllocationV2/run_sp_test/`. First cell is the locked SP list.
Suite widgets are only `source_path`, `ExecutionProfile`, and
`run_in_parallel`. Both notebooks always call the per-SP notebooks
(`outputV2/notebook/benchmark_*.py` in dev, `output/notebook/run_*.py`
in prod) and pass only those two path/profile values so each SP keeps
its own Entity / Client / Run / catalog defaults. `run_in_parallel=3`
runs 7 SPs three at a time.

## Completion summary

- mode (1 Production or 2 Development)
- files created or changed
- phases and why they are independent
- checkpoint mode used and which execution profile the orchestrator applied
- Mode 2: per-table hash result; profiler on/off

## Examples

User: “Production mode on `usp_example`.”

→ Edit `output/`, add `output/notebook/run_usp_example.py`. No profiler,
no `outputV2`.

User: “Development mode on `usp_example`.”

→ Unchanged `output/`. Create `outputV2/` with slim profiler and A/B
notebook that hashes each table.
