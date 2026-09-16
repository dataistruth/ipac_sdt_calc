# Databricks A/B benchmark reference

Create a Databricks source notebook at:

```text
Source/AllocationV2/<sp_name>/output/updated/notebook/benchmark_<sp_name>.py
```

Do not create `Source/AllocationV2/<sp_name>/notebooks/` or
`Source/AllocationV2/<sp_name>/output/benchmark_*.py`.

Default `source_path` widget:

```text
/Workspace/Users/<user>/iPACSCore_SDT_Databricks/Source
```

After a local edit, sync `Source/AllocationV2/<sp>/output/updated/` (and
`Source/AllocationV2/plan_profiler/` if the profiler changed) to that workspace
`Source/` tree. Do not sync optimized files into production `output/`.

## Required widgets

Use `dbutils.widgets.removeAll()` before defining:

```text
source_path
number_of_runs
ExecutionOrder             # alternate | original_first | updated_first
EntityID
ClientID
TaxPeriodID
RunID
CatalogName
SchemaName
ResultType
VolumePath                 # only when the SP supports it
MaxThreads                 # default 4
ProfilePlan                # off | on; default on
PlanCheckpointThreshold    # default 30
SqlShufflePartitions       # blank means unchanged
```

Add SP-specific widgets only when they map to real entry-function parameters.

## Fresh import

Workspace sync does not reload imported Python modules. Before each variant:

```python
import importlib
import sys

def clear_modules(package):
    shared = "AllocationV2.plan_profiler"
    for name in list(sys.modules):
        if (
            name == package
            or name.startswith(package + ".")
            or name == shared
            or name.startswith(shared + ".")
        ):
            del sys.modules[name]
    importlib.invalidate_caches()
```

Validate that the updated package contains at least:

```text
Source/AllocationV2/<sp>/output/updated/__init__.py
Source/AllocationV2/<sp>/output/updated/<entry_module>.py
Source/AllocationV2/<sp>/output/updated/plan_profiler.py
Source/AllocationV2/<sp>/output/updated/output_reconcile.py
Source/AllocationV2/<sp>/output/updated/notebook/benchmark_<sp>.py
```

Original module: `AllocationV2.<sp>.output.<prod_entry>`
Updated module: `AllocationV2.<sp>.output.updated.<entry>`

## Fair execution

For each pass:

1. Choose order from `ExecutionOrder`; alternate order by pass when set to `alternate`.
2. Purge only this RunID from every compared output table.
3. Start wall timer immediately before calling the entry function.
4. Capture returned reported timing independently from notebook wall time.
5. Capture output metrics immediately after successful completion.
6. Run the other variant with the same Spark settings and parameters.
7. Compare fingerprints and print mismatches.

Do not run another process for the same RunID during the benchmark.

Apply `SqlShufflePartitions` to both variants. Pass `MaxThreads`, `ProfilePlan`, and
`PlanCheckpointThreshold` only to the updated variant.

## Output reconciliation

For each output table and RunID partition compare:

- row count
- schema (names and data types)
- null counts for key columns when relevant
- sums of numeric business amounts
- order-independent row fingerprint

A robust row fingerprint:

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

For floating-point amounts, compare both the exact row fingerprint and rounded business
aggregates. Do not hide a mismatch behind a tolerance without documenting it.

## Required notebook reports

Print:

```text
[benchmark] pass N execution order: original -> updated
[benchmark] original: wall=... reported=... rows=...
[benchmark] updated: wall=... reported=... rows=...
[reconcile] PASS N: all output fingerprints match
```

The updated run must also emit:

```text
===== BUILDER-LEVEL PLAN PROFILE (where the plan grows) =====
[PLAN REPORT BUILDER] ...

===== CHECKPOINT-LEVEL PLAN PROFILE (plan truncated at each checkpoint) =====
[PLAN REPORT CHECKPOINT] ...
```

Display summary DataFrames for:

- pass, variant, order, wall time, reported time and status
- original vs updated delta and percent improvement
- per-table parity metrics
- ranked builder profile
- ranked checkpoint profile
- checkpoint recommendations with evidence

## Acceptance criteria

An optimization is accepted only when:

1. every required output table reconciles,
2. both execution orders pass when two runs are practical,
3. the updated wall time improves beyond ordinary run-to-run variance,
4. the profiler/recommendation output is present in notebook output and driver logs,
5. failures identify the first mismatching table or failed parallel task.
