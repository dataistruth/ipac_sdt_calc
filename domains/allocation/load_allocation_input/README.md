# Load Allocation Input — single-notebook workflow

Run the full `uspLoadAllocationInput` pipeline in **one notebook** (one Spark context). No multi-task job required.

## Notebook

`notebooks/run_load_allocation_input.py`

## Monolith layout

Copy or sync to:

```
Source/AllocationV2/uspLoadAllocationInput/
  source/
    load_allocation_input.py
    ai_*.py
  notebooks/
    run_load_allocation_input.py   ← this file
```

Open the notebook in Databricks, set widgets, Run All.

## ipac-sdt-calc layout

```
domains/allocation/load_allocation_input/
  source/
    load_allocation_input.py       ← SP entry (from monolith)
  notebooks/
    run_load_allocation_input.py
```

## Why one notebook?

- No job task handoff / cluster reuse quirks
- One Spark session for the full DAG (checkpoints stay hot)
- Easier debug: widgets + step timing table at the bottom
- Same code path as Mode 3 standalone (`run_load_allocation_input`)

## Step timings

Add `Common_V2.core.step_timer.StepTimer` to `load_allocation_input.py` (see `platform/common/core/step_timer.py`). The notebook displays the `timings` array from the result.

## Job vs notebook

| Approach | When to use |
|----------|-------------|
| **This notebook** | Dev, tuning, one-off runs, perf investigation |
| **Job task** | Scheduled production, orchestrator DAG, SLA monitoring |

You can point a single-task job at this notebook if you need scheduling without splitting phases.
