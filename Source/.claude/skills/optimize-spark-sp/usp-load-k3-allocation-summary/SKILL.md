---
name: optimize-usp-load-k3-allocation-summary
description: Development outputV2 for uspLoadK3AllocationSummary. Use when generating, benchmarking, or diagnosing this SP.
---

# Optimize uspLoadK3AllocationSummary

Use this skill only for `AllocationV2/usp_load_k3_allocation_summary`.

Public entry: `run_usp_load_k3_allocation_summary`. Production `output/`
is unchanged in Development. Flat `outputV2/` (no nested `business/` or
`tests/`). Production mode: inline in `output/`.

The orchestrator resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles` at start (default **low**). Cap
`MaxThreads` 1..4. Do not set AQE from the profile.

## Allowed outputV2 tree

```text
Source/AllocationV2/usp_load_k3_allocation_summary/
├── output/                         # PRODUCTION — do not edit
└── outputV2/
    ├── __init__.py
    ├── usp_load_k3_allocation_summary.py
    ├── parent.py
    ├── parallel_helpers.py
    ├── plan_profiler.py
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/benchmark_load_k3_allocation_summary.py
```

## Checkpoints

Checkpoint V2 on `k3_summary`; country-level also `k3_detail`, `k1_amounts`, `rounding_diff`.

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

1. `independent_early_loads`: country_sic, income_attr_import
2. `independent_inputs`: k3_detail, mapped_lines
3. Sequential flags, summary, rounding, finalize, single save

## Known bad

- Fake parallel writes of K3AllocationSummary
- Dropping country-level shared-frame checkpoints
- Resolving profiles from Common_V2.core.__init__
- Eager `__init__.py`

Do not leave `__pycache__` or `.pyc` files in the repository.
