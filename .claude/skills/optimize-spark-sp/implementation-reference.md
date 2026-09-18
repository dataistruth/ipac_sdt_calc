# Implementation reference

## Layout (do not violate)

- Shared profiler: `Source/AllocationV2/plan_profiler/`
- SP production (read-only): `Source/AllocationV2/<sp>/output/`
- SP candidate (write here only): `Source/AllocationV2/<sp>/output/updated/`

Forbidden:

```text
Source/AllocationV2/<sp>/output/*_updated.py
Source/AllocationV2/<sp>/notebooks/          # A/B notebooks go under updated/notebook/
```

## Isolated updated orchestrator

Prefer parent-package imports for unchanged production services:

```python
import importlib

PARENT = __package__.rsplit(".", 1)[0]

def output_module(name: str):
    return importlib.import_module(f"{PARENT}.{name}")
```

If the production orchestrator contains substantial orchestration logic that cannot be
cleanly imported, copy it verbatim to
`Source/AllocationV2/<sp>/output/updated/<same_filename>.py` and make the smallest
possible edits there. Never make an updated module import itself. Never write that
copy into `output/` next to production.

Keep the original public entry name and parameters. Append:

```python
max_threads: int = 4,
MaxThreads: int | None = None,
profile_plan: bool = False,
plan_checkpoint_threshold: int = 30,
```

Normalize workers:

```python
def normalize_workers(max_threads=4, MaxThreads=None) -> int:
    raw = MaxThreads if MaxThreads is not None else max_threads
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        workers = 4
    return max(1, min(workers, 8))
```

## Safe parallel task runner

Use task functions that return isolated values. Merge them after all futures complete.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def run_parallel(tasks, max_threads, label):
    workers = max(1, min(max_threads, len(tasks)))
    if workers == 1:
        return [(name, fn()) for name, fn in tasks]

    started = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks}
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()  # propagate failure

    print(f"[parallel] {label}: tasks={len(tasks)} workers={workers} "
          f"wall={time.time() - started:.2f}s")
    return [(name, results[name]) for name, _ in tasks]
```

For builders that mutate `cfg`, pass a shallow copy with an isolated output dictionary:

```python
def isolated_cfg(cfg):
    local = dict(cfg)
    local["_parquet_results"] = {}
    return local
```

Merge in deterministic task order. If two tasks can produce the same key, either keep
them sequential or explicitly reproduce the production union/order semantics.

Do not share a Spark action DataFrame writer between threads. Construct the writer
inside each task.

## RunID pruning

Current-run read:

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

Filter before projecting wide columns and before joining other facts.

## Broadcast rules

Good candidates:

- one-row configuration DataFrames
- enum/reference tables filtered to client/tax period
- RunID key sets
- narrow lookup views with bounded row counts

Do not infer “small” from a name. Verify through known data constraints, observed
counts, statistics, or an explicit bounded filter.

```python
small_lookup = F.broadcast(
    read_table(spark, "Lookup", cfg)
    .filter(F.col("ClientID") == cfg["client_id"])
    .select("Key", "Value")
)
```

## Cache and reuse

Storing an unmaterialized DataFrame in `cfg` avoids rebuilding Python expressions but
does not prevent Spark recomputation across actions. Distinguish:

```python
# Reuse the same logical DataFrame in multiple builders.
cfg.setdefault("_lookup_df", build_lookup())

# Persist only for repeated actions.
expensive = build_expensive().persist()
try:
    expensive.count()  # deliberate materialization
    consume_a(expensive)
    consume_b(expensive)
finally:
    expensive.unpersist()
```

Do not cache a single-consumer DataFrame.

## FEP-style profiler wiring

The shared package is `AllocationV2.plan_profiler`, implemented at
`Source/AllocationV2/plan_profiler/`. Each SP's local shim is
`output/updated/plan_profiler.py` and must re-export:

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

Provide no-op fallbacks with identical signatures, including
`plan_profile_report(source, threshold=None, label="")`.

Wrap builder references used by the updated orchestrator:

```python
build_input = track_plan(build_input)
build_hierarchy = track_plan(build_hierarchy)
```

For a ContextVar-style orchestrator:

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

plan_profile_report(
    builder_records, plan_checkpoint_threshold, label="BUILDER"
)
plan_profile_report(
    checkpoint_records, plan_checkpoint_threshold, label="CHECKPOINT"
)
plan_profile_report(
    action_records, plan_checkpoint_threshold, label="ACTION"
)
```

Checkpoint wrappers must call `track_checkpoint_plan(name, incoming_df)` before the
materialization. The profiler is analyze-only; it must not trigger a Spark action.

Instrument every explicit Spark action whose input DataFrame is available:

```python
is_empty = profile_action("warnings.isEmpty", warnings, warnings.isEmpty, cfg)
row_count = profile_action("output.count", output, output.count, cfg)
result = profile_action(
    "AllocationInput.saveAsTable",
    output,
    lambda: writer.saveAsTable(fqn),
    cfg,
)
```

This includes `.count()`, `.isEmpty()`, `.collect()`, `.first()`, `.take()`,
`.toPandas()`, writes, and result-storer calls. Do not monkeypatch DataFrame or
Spark globally to discover actions. Calls hidden inside unchanged production services
cannot be attributed safely; wrap the service's explicit action site only when an
updated local copy already exists.

The ACTION report prints every instrumented action's incoming plan size, elapsed
seconds, and repeated call count. It flags `add` only when nodes meet the plan
threshold or the same named action repeats; otherwise it prints `measure`.
These are candidates only. A terminal write or one-off action may not benefit from a
checkpoint even when expensive.

When profiler evidence justifies collapsing an existing break, preserve a per-run
escape hatch:

```python
DEFAULT_COLLAPSED_CHECKPOINTS = frozenset({"low_value_seam"})

def should_checkpoint(cfg, name):
    if name in set(cfg.get("_checkpoint_force", ()) or ()):
        return True
    bypass = set(DEFAULT_COLLAPSED_CHECKPOINTS)
    bypass.update(cfg.get("_checkpoint_bypass", ()) or ())
    return name not in bypass
```

Log the effective collapsed names at startup. Remove only materialization, never the
DataFrame transformation, temp-view registration, or downstream consumers. Re-run
parity in both execution orders and compare wall time before keeping the collapse.

## Shared checkpoint V2 (required import)

Do **not** add `output/updated/checkpoint.py`. Every updated orchestrator and
any copied service that materializes a lineage break imports:

```python
from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2 as checkpoint,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
    normalize_checkpoint_mode,
)
```

Production `Common_V2.core.checkpoint` stays untouched and is unused by updated
code.

At orchestrator start, before worker threads copy `cfg`:

```python
initialize_checkpoint_V2(cfg, CheckpointMode)  # default mode 2
```

Then call `checkpoint(spark, df, name, cfg)` at existing seams (and any new
break the user asked for). Sequence/backend selection lives entirely in V2.

Modes: 1=all stats-off Delta; 2=odd local / even Delta (default); 3=odd local /
even uncompressed Volume Parquet (`volume_path` required); 4=all local.
`localCheckpoint` failure falls back to stats-off Delta.

Do not call `coalesce()` or `repartition()` to narrow Delta checkpoint writes
and do not expose a checkpoint-coalesce parameter. That can serialize large
materializations and underutilize the cluster. Use the run-level
`spark.sql.shuffle.partitions` setting instead.

If a copied service still has `from Common_V2.core.checkpoint import checkpoint`,
rewrite that import to `checkpoint_V2`. Alias `pipeline_checkpoint = checkpoint`
in the orchestrator only if production used that name — do not invent a local
module for it.

Keep `drop_checkpoints_V2` in the import list for optional debug, but **do not
call it on the main run path**. Unique UUID/sequence names make the next run
collision-free. `localCheckpoint` leaves no catalog object. Delta `_tmp_v2_*`
tables and Volume `_checkpoints/` paths are hygiene only — a common end-of-day
job drops objects older than a safety window. If the user opts in
(`cfg["drop_checkpoints"] = True`), call drop in `finally` after writes, never
mid-pipeline.

Log once per run:

```text
[CHECKPOINT_V2] mode=2 (odd=local, even=delta)
```

## Recommendation evidence

Create a recommendation record containing:

```text
name
builder_delta
checkpoint_nodes
depth
operator_mix
consumer_count
fan_out
measured_action_seconds
measured_checkpoint_seconds
recommendation
rationale
```

Use these decision rules:

1. High delta alone means “inspect,” not “checkpoint.”
2. High delta + fan-out/repeated actions is the strongest candidate.
3. A low-node fan-out seam may still be valuable.
4. A high-node single-consumer seam may be cheaper to recompute.
5. On Delta backends, use measured write/commit time in the decision.
6. Validate every implemented break with A/B parity and wall time.
