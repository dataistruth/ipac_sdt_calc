# Databricks notebook reference

Two notebooks, one per mode. Do not mix them. Do not create
`Source/AllocationV2/<sp>/notebooks/` or a package-root `benchmark_*.py`.

Default `source_path`:

```text
/Workspace/Users/<user>/iPACSCore_SDT_Databricks/Source
```

## Mode 1 — Production run notebook

```text
Source/AllocationV2/<sp_name>/output/notebook/run_<sp_name>.py
```

Runs **only** the live modified orchestrator:

`AllocationV2.<sp>.output.<entry>`

No original/updated pair. No `_pre_opt`. No ProfilePlan. No table-hash A/B.

Widgets: **frozen Mode 1 set only** (parent skill). Identity +
`ExecutionProfile`. No MaxThreads / shuffle / CheckpointMode /
VolumePath / ResultType / ParallelGroups widgets.

Flow: evict `AllocationV2.<sp>.output` and `Common_V2` from `sys.modules`,
put `source_path` first on `sys.path`, import checkpoint V2, optionally
purge this RunID, time one `run_*` call, print wall time, reported time,
and per-table row counts.

```text
[run] production-inline wall=... reported=... rows=...
```

## Mode 2 — Development A/B notebook

```text
Source/AllocationV2/<sp_name>/outputV2/notebook/benchmark_<sp_name>.py
```

| Variant | Module |
|---|---|
| Original | `AllocationV2.<sp>.output.<prod_entry>` |
| Updated | `AllocationV2.<sp>.outputV2.<entry>` |

Widgets: **frozen Mode 2 set only** (parent skill). Exactly 10 widgets:
`source_path`, `EntityID` `4137`, `ClientID` `15348`, `TaxPeriodID` `1`,
`RunID` `17376`, `CatalogName` `QA7`, `SchemaName` `iPC_2025_QA7_15348`,
`ExecutionProfile` `low`, `number_of_runs` `1`, `ExecutionOrder`
`alternate`. `removeAll()` in its own cell.

Do **not** add MaxThreads, shuffle, CheckpointMode, ProfilePlan,
threshold, ParallelGroups, VolumePath, ResultType, or LineType widgets.
Hardcode SP-only constants in the notebook. Pass `ExecutionProfile`
**only** to the updated variant. The orchestrator resolves the tier from
`Common_V2.core.execution_profiles`; the notebook does not call
`resolve_execution_profile`.

### Fresh import

```python
import importlib
import sys

def clear_modules(package):
    shared = "AllocationV2.plan_profiler"
    common = "Common_V2"
    for name in list(sys.modules):
        if (
            name == package
            or name.startswith(package + ".")
            or name == shared
            or name.startswith(shared + ".")
            or name == common
            or name.startswith(common + ".")
        ):
            del sys.modules[name]
    importlib.invalidate_caches()
```

Evict both `AllocationV2.<sp>.output` and `...outputV2` plus `Common_V2`.
Import `Common_V2.core.checkpoint_V2` after putting `source_path` first.
Do not use `os.path.isfile` on `/Workspace/...` paths.

Validate `outputV2/` has entry module, `plan_profiler.py`,
`output_reconcile.py`, and this notebook.

### Fair execution

For each pass:

1. Choose order from `ExecutionOrder`.
2. Purge only this RunID from compared tables **that have a RunID
   column**. Read `spark.table(fqn).columns` first. Skip (do not fail)
   tables without that column.
3. Start the wall timer immediately before the entry function.
4. Capture reported timing separately from notebook wall time.
5. Capture **per-table hashes** immediately after success.
6. Run the other variant with the same Spark settings and parameters.
7. Fail on the first mismatched table.

### Per-table validation hash

`output_reconcile.py` must inspect live schemas before `CREATE TABLE …
WHERE <key>`. A guessed `RunID` on `PFICUpdateAlert` (AlertID / ClientID /
EntityID / TaxPeriodID only) fails snapshot creation.

For each compared table and its **declared** key partition compare:

- row count
- schema (names and data types)
- null counts for key columns when relevant
- sums of numeric business amounts
- order-independent row fingerprint

```python
import pyspark.sql.functions as F

ordered = sorted(df.columns)
row_hash = F.xxhash64(
    *[
        F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
        for c in ordered
    ]
)
fingerprint = (
    df.select(row_hash.alias("_h"))
    .agg(
        F.count("*").alias("rows"),
        F.sum("_h").alias("hash_sum"),
        F.min("_h").alias("hash_min"),
        F.max("_h").alias("hash_max"),
    )
    .first()
)
```

Display one row per table: `table`, `exact_match`, original hash fields,
updated hash fields, mismatch detail. For floats, compare the exact
fingerprint and rounded aggregates. Do not hide a mismatch behind an
undocumented tolerance.

### Required reports (Mode 2)

```text
[benchmark] pass N execution order: original -> updated
[benchmark] original: wall=... reported=... rows=...
[benchmark] updated: wall=... reported=... rows=...
[reconcile] PASS N: all output fingerprints match
[reconcile] table=<name> rows=... hash_sum=... match=PASS|FAIL
[parallel] <phase>: tasks=... workers=... wall=...s
```

When `ProfilePlan` is on, also emit BUILDER / CHECKPOINT / ACTION reports.

Display DataFrames for runtime, per-table parity hashes, and optional
profiler rankings.

### Acceptance (Mode 2)

1. every required output table hash matches,
2. both execution orders pass when two runs are practical,
3. updated wall improves beyond ordinary variance,
4. failures name the first mismatching table or failed parallel task.

Mode 1 has no hash-acceptance gate; it is a production run harness only.
Promote Development → Production only after Mode 2 hashes pass.
