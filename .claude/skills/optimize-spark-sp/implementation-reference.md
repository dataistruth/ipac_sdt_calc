# Implementation reference

Pipeline logic is identical in both modes. Packaging and profiler differ.

## Layout

**Mode 1 Production** — edit live `output/`. No `outputV2/`, no `updated/`,
no `business/`, no `_pre_opt/`, no `plan_profiler.py`.

```text
Source/AllocationV2/<sp>/output/
├── orchestrator.py            # or the SP entry module
├── parallel_helpers.py        # optional
└── notebook/run_<sp>.py
```

**Mode 2 Development** — do not edit `output/`.

```text
Source/AllocationV2/<sp>/outputV2/
├── orchestrator.py
├── parent.py
├── parallel_helpers.py
├── plan_profiler.py           # slim shim only
├── output_reconcile.py
└── notebook/benchmark_<sp>.py
```

Forbidden in both modes:

```text
Source/AllocationV2/<sp>/output/*_updated.py
Source/AllocationV2/<sp>/notebooks/
Source/AllocationV2/<sp>/output/business/
Source/AllocationV2/<sp>/output/updated/
```

Shared profiler package (Development import only):
`Source/AllocationV2/plan_profiler/`.

Mode 2 imports unchanged production services from the same SP’s
`output/` package (the live production folder). Do not import a remote
repo copy.

```python
import importlib

PARENT = __package__.rsplit(".", 1)[0]  # ...<sp>.outputV2 → ...<sp>

def output_module(name: str):
    return importlib.import_module(f"{PARENT}.{name}")
```

Example: `output_module("output.underlyings")` is
`AllocationV2.<sp>.output.underlyings`.

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
execution_profile: str = "low",
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

Keep the three-tier map in `Common_V2/core/execution_profiles.py`. Import it
directly from that module; do not re-export or resolve profiles from
`Common_V2.core.__init__`, and do not put profiles in an SP `__init__.py`.

The **SP orchestrator** performs this resolution once at invocation start,
before checkpoint initialization and before parallel tasks copy `cfg`.
Default `execution_profile` / `ExecutionProfile` is **`low`**. The
notebook passes the widget through; it does not call
`resolve_execution_profile`. `Common_V2.core.__init__` stays passive.

```python
from Common_V2.core.checkpoint_V2 import (
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
from Common_V2.core.execution_profiles import resolve_execution_profile

# shuffle_partitions: low=32, medium=48, big=64
# checkpoint_mode:    low=4,  medium=2,  big=1
# max_threads:        4 on all three
profile = resolve_execution_profile(ExecutionProfile or execution_profile or "low")
if not explicit_shuffle:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(profile["shuffle_partitions"]),
    )
workers = normalize_workers(
    max_threads=MaxThreads if MaxThreads is not None else profile["max_threads"],
    MaxThreads=MaxThreads,
)
mode = resolve_checkpoint_mode(
    cfg,
    checkpoint_mode=checkpoint_mode if checkpoint_mode is not None else profile["checkpoint_mode"],
    CheckpointMode=CheckpointMode,
)
initialize_checkpoint_V2(cfg, mode)
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
K1 / PFIC snapshot) or collect-then-Delta-then-Parquet `output_writes`
unless this SP uses the same `_parquet_results` flush.

Port only phasing, isolation, checkpoint V2, profiling, pruning,
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
it on.

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

## Shared checkpoint V2 (required import)

```python
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    resolve_checkpoint_mode,
)
```

Call once at start, after the orchestrator resolves the execution profile
and before worker threads copy `cfg`. If `CheckpointMode` /
`checkpoint_mode` are unset, pass `profile["checkpoint_mode"]`. Do not
hardcode `checkpoint_mode=2` (or 4) as an orchestrator fallback except as
the profile value. `DEFAULT_CHECKPOINT_MODE` remains the inherit path when
neither an explicit mode nor a profile is supplied.

Modes: 1=all stats-off Delta; 2=odd local / even Delta; 3=odd local / even
uncompressed Volume Parquet (`volume_path` required); 4=all local.
`localCheckpoint` failure falls back to stats-off Delta.

Do not `coalesce()`/`repartition()` to narrow checkpoint writes.

Keep `drop_checkpoints_V2` imported for debug; do not call it on the hot
path unless `cfg["drop_checkpoints"]` is true, and then only in `finally`.

Log once per run:

```text
[CHECKPOINT_V2] mode=...
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
