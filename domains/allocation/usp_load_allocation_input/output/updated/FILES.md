# `output/updated/` — sync this folder only

Copy or rsync **this entire directory** to monolith:

`Source/AllocationV2/usp_load_allocation_input/output/updated/`

Production `output/load_allocation_input.py` and `output/ai_*.py` stay on monolith (not replaced).

| File | Required | Purpose |
|------|----------|---------|
| `load_allocation_input.py` | YES | Main pipeline entry |
| `ai_pfic_flowup_service.py` | YES | Optimized PFIC flowup (replaces monolith for updated run) |
| `parent.py` | YES | Imports `ai_*.py` siblings from monolith `output/` |
| `step_timer.py` | YES | Per-step timers |
| `checkpoint.py` | YES | `checkpoint_use_local` / `checkpoint_backend=local` for all pipeline localCheckpoint |
| `flowup_run_filter.py` | YES | RunID partition pruning (`read_local_run_table`, `read_lower_tier_flowup`) |
| `output_reconcile.py` | YES | Benchmark output parity (row counts / bytes vs original) |
| `parallel_config.py` | YES | Thread pool for parallel shared views |
| `shared_views.py` | YES | Parallel register entry |
| `shared_views_builders.py` | YES | Parallel view builders |
| `shared_view_sql_map.py` | optional | SQL fallback |
| `load_allocation_input_updated.py` | optional | Shim inside `updated/` |
| `__init__.py` | YES | Package marker |
| `notebooks/*.py` | YES | Runner, benchmark, SQL map generator |
| `test_updated_package.py` | optional | Local import/smoke test (do not sync to prod) |
| `_build_ai_pfic_flowup.py` | optional | Dev rebuild script (do not sync) |
| `_ai_pfic_flowup_service_base.py` | optional | Dev extract (do not sync) |

## Import paths (all under `output.updated`)

```python
# Recommended
from AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input import run_load_allocation_input

# Shim (same folder)
from AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input_updated import run_load_allocation_input

# Package root
from AllocationV2.usp_load_allocation_input.output.updated import run_load_allocation_input
```

## Sync command

```bash
rsync -av output/updated/ "$MONOLITH_SOURCE/AllocationV2/usp_load_allocation_input/output/updated/"
```

Or from repo root: `./deploy.sh`
