# output/ — production files stay in monolith; copy `updated/` + shim from this bundle.

| Path | Required | Purpose |
|------|----------|---------|
| `updated/load_allocation_input.py` | YES | Main pipeline entry |
| `updated/step_timer.py` | YES | Per-step timers |
| `updated/checkpoint.py` | YES | Volume / Delta checkpoints (no end-of-run cleanup) |
| `updated/parallel_config.py` | YES | Thread pool for parallel shared views |
| `updated/shared_views.py` | YES | Parallel register entry |
| `updated/shared_views_builders.py` | YES | Parallel ai_shared_views logic |
| `updated/shared_view_sql_map.py` | optional | SQL fallback only |
| `updated/__init__.py` | YES | Package marker |
| `updated/notebooks/*.py` | YES | Runner, benchmark, SQL map generator |
| `load_allocation_input_updated.py` | optional | Shim → `updated.load_allocation_input` |

## Monolith layout after copy

```text
output/
  load_allocation_input.py          # production — do not replace
  ai_*.py                           # unchanged
  updated/                          # copy entire folder
    *.py
    notebooks/
  load_allocation_input_updated.py   # shim (optional)
```

## Import paths

```python
# Recommended
from AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input import run_load_allocation_input

# Legacy shim (benchmark notebook)
from AllocationV2.usp_load_allocation_input.output.load_allocation_input_updated import run_load_allocation_input
```
