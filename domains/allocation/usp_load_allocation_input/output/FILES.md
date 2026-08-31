# output/ — copy ALL of these to monolith Source/AllocationV2/usp_load_allocation_input/output/

| File | Required | Purpose |
|------|----------|---------|
| load_allocation_input_updated.py | YES | Main pipeline entry |
| step_timer_updated.py | YES | Per-step timers |
| checkpoint_updated.py | YES | Volume / Delta checkpoints |
| parallel_config_updated.py | YES | Thread pool helpers |
| config_loaders_updated.py | YES | Parallel load_common_config / load_config |
| config_parallel_hooks.py | YES | UC prefetch hooks |
| shared_views_updated.py | YES | Parallel register entry |
| shared_views_builders_updated.py | YES | Parallel ai_shared_views logic |
| shared_view_sql_map.py | optional | SQL fallback only |
| __init__.py | optional | Package marker |

## NOT in this folder (stay in monolith — do not replace)

- load_allocation_input.py (production)
- ai_shared_views.py
- ai_config_service.py
- all other ai_*.py

## After copy to Databricks

Reload imports in runner notebook cell.
