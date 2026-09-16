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
finish_plan_profile
measure_plan
plan_profile_report
start_checkpoint_plan_profile
start_plan_profile
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
try:
    result = run_pipeline(...)
finally:
    finish_checkpoint_plan_profile(cp_token)
    finish_plan_profile(plan_token)

plan_profile_report(
    builder_records, plan_checkpoint_threshold, label="BUILDER"
)
plan_profile_report(
    checkpoint_records, plan_checkpoint_threshold, label="CHECKPOINT"
)
```

Checkpoint wrappers must call `track_checkpoint_plan(name, incoming_df)` before the
materialization. The profiler is analyze-only; it must not trigger a Spark action.

## Checkpoint module (required API)

`output/updated/checkpoint.py` is required whenever the SP materializes lineage
breaks. Export at least:

```python
DEFAULT_CHECKPOINT_BACKEND = "delta"
normalize_checkpoint_backend
normalize_local_denylist
checkpoint
pipeline_checkpoint   # required alias of checkpoint
drop_checkpoints
log_checkpoint_plan
should_checkpoint
_use_production_checkpoint  # False unless caller opts into Common_V2
```

Default backend is **delta**, not local. `local` is optional and opt-in only.

The orchestrator and notebook must import only names this module exports. Export
every public name they use in one shot — do not discover them by Databricks
`ImportError`. If the SP historically called `pipeline_checkpoint`, keep that
name as:

```python
def pipeline_checkpoint(spark, df, name, cfg):
    return checkpoint(spark, df, name, cfg)
```

`_use_production_checkpoint(cfg)` must default to **False**. Updated checkpoints
write stats-off Delta; they do not wrap `Common_V2.core.checkpoint` unless the
caller explicitly sets `checkpoint_use_production=True`.

Required Delta write contract:

```python
STATS_KEY = "spark.databricks.delta.stats.collect"

existed, previous = True, spark.conf.get(STATS_KEY)  # guard missing conf
try:
    spark.conf.set(STATS_KEY, "false")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.dataSkippingNumIndexedCols", "0")
        .saveAsTable(fqn)
    )
finally:
    # restore previous conf or unset if it did not exist
    ...
```

`drop_checkpoints(spark, cfg)` must drop only tables listed in
`cfg["_checkpoint_tables"]` for this invocation.

Always export `normalize_local_denylist`. `localCheckpoint` cannot re-resolve
self-joins (`UNRESOLVED_COLUMN`). Names matching the denylist stay on Delta
even when the run backend is `local`. Accept a comma/space string or iterable,
with `mode="extend"` (union defaults) or `mode="replace"` (use only extras;
empty extras fall back to defaults). Store the result on
`cfg["_local_delta_denylist"]`.

Do not wrap `Common_V2.core.checkpoint.checkpoint` without also exporting the
public names the orchestrator and notebook import:

```python
from .checkpoint import (
    checkpoint,
    drop_checkpoints,
    normalize_checkpoint_backend,
    normalize_local_denylist,
    pipeline_checkpoint,
)
```

Log the setting once per run:

```text
[checkpoint] backend=delta, column_stats=off
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
