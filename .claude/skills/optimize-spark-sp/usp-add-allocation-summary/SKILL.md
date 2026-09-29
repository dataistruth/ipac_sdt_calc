---
name: optimize-usp-add-allocation-summary
description: Development outputV2 for uspAddAllocationSummary. Use when generating, benchmarking, or diagnosing this SP.
---

# Optimize uspAddAllocationSummary

Use this skill only for `AllocationV2/usp_add_allocation_summary`.

Public entry: `run_add_allocation_summary`. Production `output/` is unchanged. No `business/`.

The orchestrator resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles` at start (default **low**). Cap
`MaxThreads` 1..4. Do not set AQE from the profile.

## Allowed outputV2 tree

```text
Source/AllocationV2/usp_add_allocation_summary/
├── output/                         # PRODUCTION — do not edit
└── outputV2/
    ├── __init__.py                 # lazy __getattr__
    ├── add_allocation_summary.py
    ├── parent.py
    ├── parallel_helpers.py
    ├── plan_profiler.py
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/benchmark_add_allocation_summary.py
```

## Checkpoints

Checkpoint V2: allocation_output_summary and other multi-consumer `tables` frames. Same-table appends stay sequential.

## Notebook widgets (frozen)

Exactly the parent-skill 10 widgets. Identity:

| # | Name | Default |
|---|---|---|
| 2 | EntityID | 4137 |
| 5 | RunID | 17376 |
| 6 | CatalogName | QA7 |
| 7 | SchemaName | iPC_2025_QA7_15348 |

`removeAll()` in its own cell. Hardcode ResultType `deltalake` and
VolumePath `/Volumes/qa7/datavolume/databrickdata`. Pass
`ExecutionProfile` and `ProfilePlan` only to updated. A/B order is
**original then updated**. Inspect live columns before `WHERE RunID`.
Last display cell uses explicit Spark schemas so `skip_reason=""` does
not fail inference.

## Phases

1. Sequential config + working tables + partner PFIC
2. `independent_builders` (PFIC reclass, custom footnote txns)
3. `output_writes` distinct tables, isolated cfg, merge file-info lists

## Known bad

- Copying the huge production module into outputV2
- Parallelizing two `_store_result_table` calls to the same target
- Sharing one cfg across parallel writers (`_result_file_infos` mutates)
- Eager `__init__.py` import of the orchestrator
- Mix Production/Development in one change set

Do not leave `__pycache__` or `.pyc` files in the repository.
