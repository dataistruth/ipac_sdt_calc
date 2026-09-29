---
name: optimize-usp-add-lookthrough-alloc-detail-step01
description: Development outputV2 for uspAddLookThroughAllocationDetail_Step_01. Use when generating, benchmarking, or diagnosing this SP.
---

# Optimize uspAddLookThroughAllocationDetail_Step_01

Use this skill only for `AllocationV2/usp_add_lookthrough_alloc_detail_step01`.

Public entry: `run_add_lookthrough_allocation_detail_step01`. Production `output/` is unchanged. No `business/`.

The orchestrator resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles` at start (default **low**). Cap
`MaxThreads` 1..4. Do not set AQE from the profile.

## Allowed outputV2 tree

```text
Source/AllocationV2/usp_add_lookthrough_alloc_detail_step01/
├── output/                         # PRODUCTION — do not edit
└── outputV2/
    ├── __init__.py
    ├── add_lookthrough_allocation_detail_step01.py
    ├── parent.py
    ├── parallel_helpers.py
    ├── plan_profiler.py
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/benchmark_add_lookthrough_allocation_detail_step01.py
```

## Checkpoints

Checkpoint V2 `base_lt_out` (LookThroughAllocationOutput filter). After local backend, `toDF(*columns)`.

## Notebook widgets (frozen)

Exactly the parent-skill 10 widgets. Identity:

| # | Name | Default |
|---|---|---|
| 2 | EntityID | 4755 |
| 5 | RunID | 18266 |
| 6 | CatalogName | qa7 |
| 7 | SchemaName | iPC_2025_QA7_15348 |

`removeAll()` in its own cell. Hardcode ResultType `deltalake` and
VolumePath `/Volumes/qa7/datavolume/databrickdata`. Pass
`ExecutionProfile` and `ProfilePlan` only to updated. A/B order is
**original then updated**. Inspect live columns before `WHERE RunID`.
Last display cell uses explicit Spark schemas so `skip_reason=""` does
not fail inference.

## Phases

1. Config + Checkpoint V2 `base_lt_out`
2. `output_writes`: dated-transfer pair sequential in one task; other distinct tables parallel; merge `_parquet_results`
3. Sequential GenericResultStorer

## Known bad

- Parallelizing the two dated-transfer writes (same parquet key)
- Purging LookThroughAllocationOutput before variants
- Parallel GenericResultStorer save
- Eager `__init__.py`

Do not leave `__pycache__` or `.pyc` files in the repository.
