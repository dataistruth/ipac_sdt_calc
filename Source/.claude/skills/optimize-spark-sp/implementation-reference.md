# Implementation reference

Pipeline logic is identical in both modes. Packaging and profiler differ.

## Layout

**Mode 1 Production** — edit live `output/` in place. Same optimizations
as Development. No `outputV2/`, no `updated/`, no `business/`, no
`tests/`, no `_pre_opt/`, no `plan_profiler.py`.

```text
Source/AllocationV2/<sp>/output/
├── orchestrator.py            # or the SP entry module (inline edits)
└── notebook/run_<sp>.py
```

**Mode 2 Development** — do not edit `output/`. Flat `outputV2/` only
(every candidate `.py` in that directory). No nested `business/`,
`tests/`, `updated/`, or `_pre_opt/`.

```text
Source/AllocationV2/<sp>/outputV2/
├── orchestrator.py
├── parent.py
├── plan_profiler.py           # slim shim only
├── output_reconcile.py
└── notebook/benchmark_<sp>.py
```

Thread pools, isolated `cfg`, and write fan-out come from
`Common_V2.core.parallel_helpers` and `Common_V2.core.write_helpers` in
both modes. Never add an SP-folder `parallel_helpers.py` or
`write_helpers.py`; SP-specific write wrappers go in the SP's existing
service module.

FEP Development also places optimized helper **copies** in that same
folder (`cost_pct_loader.py`, `state_allocation.py`, `pfic_footnotes.py`,
`effective_calc.py`, `entity_hierarchy.py`). Do not put them under
`outputV2/business/`.

Forbidden in both modes:

```text
Source/AllocationV2/<sp>/output/*_updated.py
Source/AllocationV2/<sp>/notebooks/
Source/AllocationV2/<sp>/output/business/
Source/AllocationV2/<sp>/outputV2/business/
Source/AllocationV2/<sp>/outputV2/tests/
Source/AllocationV2/<sp>/output/updated/
Source/AllocationV2/<sp>/outputV2/updated/
```

Shared profiler package (Development import only):
`Source/AllocationV2/plan_profiler/`.

Mode 2 `outputV2/` is self-contained for SP code (shared
`Common_V2.core` modules are imported, never copied). Copy every SP helper the
orchestrator needs into `outputV2/` (keep `services/` as a
subpackage when the SP already uses one). Import as
`AllocationV2.<sp>.outputV2.<module>` or relative `.module`.
**Never** import `AllocationV2.<sp>.output.*` from `outputV2`.
`__init__.py` stays lazy exports only — do not add `_OutputV2Finder`.
Do not prefix helpers with `# Databricks notebook source`.
The A/B notebook `ORIGINAL` constant is the only allowed
reference to `output/` (the `sdt_d` checkout). Current ipac
`output/` is already optimized — do not use it as original.
Keep the public entry name exactly as in `sdt_d`.

```python
import importlib

_OUTPUT_PACKAGE = __package__  # this outputV2 package

def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")
```

Mode 1 has no `parent.py` and no `_pre_opt`. Inline edits live in
`output/` itself.

## Entry parameters

Keep the original public entry name.

Both modes:

```python
max_threads: int = 4,
MaxThreads: int | None = None,
parallel_groups: str = "all",
ParallelGroups: str | None = None,
checkpoint_mode: int | None = None,
CheckpointMode: int | None = None,
execution_profile: str | None = None,
ExecutionProfile: str | None = None,
```

Mode 2 only (do not add these in Production):

```python
profile_plan: bool = False,
plan_checkpoint_threshold: int = 30,
```

```python
def normalize_workers(max_threads=4, MaxThreads=None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 4))
```

Parse groups: `all` enables every named phase; `none` forces sequential;
otherwise a comma-separated allow-list.

## Execution profiles

Keep the two-tier map (`small`, `large`) and `DEFAULT_EXECUTION_PROFILE` in
`Common_V2/core/execution_profiles.py`. Import it
directly from that module; do not re-export or resolve profiles from
`Common_V2.core.__init__`, and do not put profiles in an SP `__init__.py`.

The **SP orchestrator** performs this resolution once at invocation start,
before checkpoint initialization and before parallel tasks copy `cfg`.
Blank / `"default"` resolves to `DEFAULT_EXECUTION_PROFILE` (currently
**`small`**); SPs and notebooks never hardcode a tier. The
notebook passes the frozen `ExecutionProfile` and `ProfilePlan` widgets
through; it does not call `resolve_execution_profile` and must not add
MaxThreads / shuffle / CheckpointMode / ExecutionOrder widgets. `Common_V2.core.__init__` stays
passive. Function args for those overrides may still exist; they stay
`None` so the tier applies.

```python
from Common_V2.core.execution_profiles import resolve_execution_profile
from Common_V2.core.parallel_helpers import normalize_workers, parse_enabled_groups

KNOWN_GROUPS = frozenset({"independent_builders", "output_writes"})  # per SP

# shuffle_partitions: small=32, large=64
# checkpoint_mode:    small=2 (local), large=1 (Delta)
# max_threads:        4 on both
profile = resolve_execution_profile(
    _blank(ExecutionProfile) or _blank(execution_profile),
    checkpoint_mode=CheckpointMode if CheckpointMode is not None else checkpoint_mode,
)
profile_name = profile["name"]
if not explicit_shuffle:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(profile["shuffle_partitions"]),
    )
workers = normalize_workers(
    max_threads=MaxThreads if MaxThreads is not None else profile["max_threads"],
    MaxThreads=MaxThreads,
)
mode = profile["checkpoint_mode"]
cfg["checkpoint_mode"] = mode
cfg.setdefault("_checkpoint_tables", [])
enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups, known_groups=KNOWN_GROUPS)
```

Explicit `CheckpointMode`, `SqlShufflePartitions`, and `MaxThreads` override
the profile. Do **not** set `spark.sql.adaptive.enabled` from a profile.
AQE is on by default on Databricks.

## Parallel thread phasing

Phasing is the portable FEP scheduling pattern. It is not SP-specific
business logic.

### Rules

1. Draw the DataFrame/`cfg` dependency DAG first.
2. A **phase** (wave) is a set of tasks that share only outputs of **prior**
   phases. Tasks in the same phase must not read each other's results or
   mutate the same `cfg` keys.
3. Run phases **sequentially**. Run tasks **inside** a phase concurrently.
4. Cap the pool at `min(4, task_count, max_threads)`.
5. Submit all tasks, wait for **every** future, then raise on the main
   thread if any failed. Do not cancel siblings silently without observing
   them.
6. Wave wall time is `max(task elapsed)`, not the sum.
7. Do not start the next phase until the current phase returns.
8. Construct Spark writers **inside** each task. Do not share a writer.
9. Log `[parallel] START/DONE` with phase, task, thread, status, elapsed.

### Phase catalog (adapt names to the SP)

| Phase | Independent when |
|---|---|
| `common_dimensions` | Dimension/config loads do not need each other |
| `common_inputs` | Fact/reference reads do not need each other |
| `independent_builders` | Builders consume only prior-phase frames |
| `independent_validations` | Non-gating warnings; no early-abort coupling |
| `output_writes` | Distinct tables (or proven-safe distinct partitions) |

Always add `output_writes` when two or more result tables append
independently after gating validation. Example: LookThroughAllocationInput,
SchKTaxableIncome, and PFICtoK1IncomeAttributePercentages. Do not fold a
post-write `.count()` onto another table’s critical path; keep it inside
that table’s task if the production log requires it.

Keep sequential: gating `isEmpty`/`first` that abort the run, builders that
consume a sibling's DataFrame, same-table writes, temp views that reference
each other, unordered writes to the same `cfg` key, AllocationRun /
error-table updates that depend on validation outcome.

### Coordinator

```python
from concurrent.futures import ThreadPoolExecutor
import threading
import time

def run_phase(name, tasks, max_threads, enabled_groups):
    """tasks: list[(task_name, fn, args, kwargs)] in stable merge order."""
    if name not in enabled_groups or max_threads <= 1 or len(tasks) <= 1:
        return [(task_name, fn(*args, **kwargs)) for task_name, fn, args, kwargs in tasks]

    workers = max(1, min(max_threads, len(tasks)))
    results = {}
    failures = []
    started = time.time()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"sp-{name}") as pool:
        futures = {
            pool.submit(fn, *args, **kwargs): task_name
            for task_name, fn, args, kwargs in tasks
        }
        for future, task_name in futures.items():
            try:
                results[task_name] = future.result()
            except Exception as exc:
                failures.append((task_name, exc))
    if failures:
        detail = [(n, f"{type(e).__name__}: {e}") for n, e in failures]
        error = RuntimeError(f"Parallel group {name!r} failed: {detail}")
        raise error from failures[0][1]
    print(
        f"[parallel] {name}: tasks={len(tasks)} workers={workers} "
        f"wall={time.time() - started:.2f}s critical=max-task"
    )
    return [(task_name, results[task_name]) for task_name, *_ in tasks]
```

Initialize the pool **once per SP invocation** when several phases run;
otherwise a per-phase `with ThreadPoolExecutor` is acceptable. Shutdown in
`finally`.

### Isolated cfg and deterministic merge

```python
def isolated_cfg(cfg):
    local = dict(cfg)
    local["_parquet_results"] = {}
    return local
```

For deeper forks (lists/dicts that parallel tasks mutate), shallow-copy
containers except thread-safe checkpoint coordination lists that **must**
stay shared. Merge branch artifacts in production task order after the
phase. If two tasks can write the same key, keep them sequential or
reproduce production union/order exactly.

Pass Spark sessions as the process session; do not create extra sessions
per thread.

### What not to copy from FEP, footnotes, look-through, or allocation input

Do not port FEP CPBT `_mode` fusion, footnote PFIC batching, state-pass
collapse, or hierarchy WHILE rewrites unless this SP has the same SQL
contract.

Do not port footnotes `cost_snapshot` / `entity_levels` /
`alloc_pass1`–`alloc_pass4` or two-table AllocationInput deduction
reconcile unless this SP has the same S1–S13 contract.

Do not port look-through allocation input `independent_early_loads` /
`independent_input_builders` / `output_writes` table list, or keep-FX-
sequential / keep-PFIC-chain-sequential rules, unless this SP has the
same contract.

Do not port load-allocation-input `independent_input_builders` (forms /
K1 / PFIC snapshot), `output_collect`, or per-table `output_writes`
unless this SP uses the same `_parquet_results` flush.

Port only phasing, isolation, profile checkpoints, profiling, pruning,
bounded broadcasts, and distinct-table `output_writes` when the tables
are actually independent.

## Safe sequential fallback

If `workers == 1` or the group is disabled, run tasks in listed order with
the same function bodies. Results must match the parallel merge order.

## RunID pruning

```python
def read_current_run(read_table, spark, table_name, cfg):
    df = read_table(spark, table_name, cfg)
    if "RunID" in df.columns:
        df = df.filter(df.RunID == int(cfg["run_id"]))
    if "ClientID" in df.columns:
        df = df.filter(df.ClientID == int(cfg["client_id"]))
    if "TaxPeriodID" in df.columns:
        df = df.filter(df.TaxPeriodID == int(cfg["tax_period_id"]))
    return df
```

Lower-tier run pruning:

```python
import pyspark.sql.functions as F

run_ids = (
    spark.table(f"_lower_tier_funds_{cfg['run_id']}")
    .select(F.col("RunID").cast("long").alias("RunID"))
    .distinct()
)
pruned = fact.join(F.broadcast(run_ids), "RunID", "left_semi")
```

Filter before wide projections and before joining other facts.

## Broadcast rules

Good candidates: one-row config, client/period enums, RunID key sets,
narrow bounded lookups. Do not infer “small” from a name.

```python
small_lookup = F.broadcast(
    read_table(spark, "Lookup", cfg)
    .filter(F.col("ClientID") == cfg["client_id"])
    .select("Key", "Value")
)
```

## Cache and reuse

```python
cfg.setdefault("_lookup_df", build_lookup())

expensive = build_expensive().persist()
try:
    expensive.count()
    consume_a(expensive)
    consume_b(expensive)
finally:
    expensive.unpersist()
```

Do not cache a single-consumer DataFrame.

## Plan profiler (Mode 2 only)

Do not add profiler modules, imports, wrappers, or parameters in Mode 1.
Production orchestrators must not reference `plan_profiler` or
`profile_plan`.

In Mode 2, add a slim `outputV2/plan_profiler.py` that re-exports the
shared `AllocationV2.plan_profiler` when present, else no-ops with the
same signatures. Default `profile_plan=False`. The A/B notebook may turn
it on with widget `ProfilePlan`. `measure_plan` and Catalyst `_explain_string` run
only in that on-path. If explain/analyze fails (for example
`UNRESOLVED_COLUMN`), skip **that frame only** — never fail the SP.
Do not explain `track_plan` inputs. Do not wrap FEP effective-calc /
Form 199A / type-id / missing-entity helpers (localCheckpoint drops
alias `D`). Never use Connect `_plan` proto for node counts.
When the widget is off, call
`disable_plan_profile()` so leftover sinks cannot tax the
updated A/B run. The last notebook cell displays stored
BUILDER / CHECKPOINT / ACTION tables only when the widget is on.

Record sinks must be process-wide (module list + lock), not
ContextVar-only. Checkpoints run inside
`ThreadPoolExecutor` workers that do not inherit ContextVars; otherwise
CHECKPOINT keep/remove tables stay empty. Optionally wrap Mode 2
builder imports with `track_plan` (output only). Finish must
merge `cfg["_plan_profile"]` / `cfg["_checkpoint_plan_profile"]` /
`cfg["_action_plan_profile"]` into shared
`AllocationV2.plan_profiler.LAST_PLAN_PROFILE`.

Shared package: `AllocationV2.plan_profiler`. The slim shim must re-export:

```python
finish_checkpoint_plan_profile
finish_action_profile
finish_plan_profile
measure_plan
plan_profile_report
profile_action
start_action_profile
start_checkpoint_plan_profile
start_plan_profile
track_action_plan
track_checkpoint_plan
track_plan
```

Provide no-op fallbacks with the same signatures, including
`plan_profile_report(source, threshold=None, label="")`.
`track_plan` fallback must be an identity decorator (`return fn`),
never a function that returns `None`. `profile_action` fallback must
call `action()`. Wrapping builders with a None-returning fallback
makes the updated A/B run raise `TypeError: 'NoneType' object is not callable`.

Wrap builders used by the orchestrator with `track_plan`. For one
invocation:

```python
plan_token, builder_records = start_plan_profile()
cp_token, checkpoint_records = start_checkpoint_plan_profile()
action_token, action_records = start_action_profile()
try:
    result = run_pipeline(...)
finally:
    finish_action_profile(action_token)
    finish_checkpoint_plan_profile(cp_token)
    finish_plan_profile(plan_token)

plan_profile_report(builder_records, plan_checkpoint_threshold, label="BUILDER")
plan_profile_report(checkpoint_records, plan_checkpoint_threshold, label="CHECKPOINT")
plan_profile_report(action_records, plan_checkpoint_threshold, label="ACTION")
```

Call `track_checkpoint_plan(name, incoming_df)` before materialization.
Do not globally monkeypatch DataFrame. Wrap `.count()`, `.isEmpty()`,
`.collect()`, `.first()`, `.take()`, `.toPandas()`, writes, and
result-storer calls with `profile_action`.

Preserve production checkpoint seams by default. Collapse a seam only when
the user asks and A/B parity plus wall time win.

## Shared checkpoint (required import)

```python
from Common_V2.core.checkpoint import (
    checkpoint as common_checkpoint,
    drop_checkpoints,
)

checkpoint = common_checkpoint  # only if builders call this name

def _checkpoint(spark, df, name, cfg):
    return common_checkpoint(spark, df, name, cfg)
```

Development (`outputV2`) only — record the truncated plan for ProfilePlan:

```python
def _checkpoint(spark, df, name, cfg):
    from .plan_profiler import track_checkpoint_plan

    track_checkpoint_plan(name, df, cfg)
    return common_checkpoint(spark, df, name, cfg)
```

When Production **inlines** a wrapper and rebinds the same module’s
`checkpoint` (hierarchy `hier_level_*`), the wrapper
must call `common_checkpoint(...)`, never `checkpoint(...)`. Python looks up
`checkpoint` at call time. After `mod.checkpoint = _checkpoint`, a wrapper
that calls `checkpoint` calls itself until `maximum recursion depth exceeded`.
Development may patch `_prod.checkpoint = _checkpoint` because
`_checkpoint` lives in `outputV2` and calls its own `common_checkpoint`
import.

Set `cfg["checkpoint_mode"] = profile["checkpoint_mode"]` and
`cfg.setdefault("_checkpoint_tables", [])` once at start, after the
orchestrator resolves the execution profile and before worker threads copy
`cfg` (shared list so `drop_checkpoints` sees tables from every thread).
`checkpoint.py` is the original `sdt_d` file plus the local branch: without
an explicit mode or `cfg["checkpoint_mode"] == 2` it runs the original Delta
path, so SPs outside this program keep their behavior.

Modes: **1 = Delta temp table** (default; unique per-call name, UC retry);
**2 = `localCheckpoint(eager=True)`** + `toDF(*columns)`, falling back to
Delta on failure. Nothing raises: any CheckpointMode other than 2 (e.g. 4, junk) resolves
to Delta, and an unknown ExecutionProfile name resolves to `large` (Delta). Legacy direct calls
that must stay Delta pass `checkpoint_mode=DELTA_CHECKPOINT_MODE`.

Do not `coalesce()`/`repartition()` to narrow checkpoint writes.

Call `drop_checkpoints(spark, cfg)` in `finally` (keep any existing
`cfg["drop_checkpoints"]` gate). Local checkpoints need no cleanup.

Log once per run:

```text
[CHECKPOINT] ExecutionProfile=... CheckpointMode=...
```

## Recommendation evidence

Record:

```text
name, builder_delta, checkpoint_nodes, depth, operator_mix,
consumer_count, fan_out, measured_action_seconds,
measured_checkpoint_seconds, recommendation, rationale
```

Rules:

1. High delta alone means inspect, not checkpoint.
2. High delta + fan-out/repeated actions is the strongest add.
3. A low-node fan-out seam may still be valuable.
4. A high-node single-consumer seam may be cheaper to recompute.
5. On Delta, include write/commit time.
6. Validate every implemented break with A/B parity and wall time.
