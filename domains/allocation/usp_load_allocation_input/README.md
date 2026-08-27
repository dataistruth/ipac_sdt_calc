# `output/` — `_updated` siblings (copy together)

```
Source/AllocationV2/usp_load_allocation_input/output/
  load_allocation_input.py
  load_allocation_input_updated.py
  step_timer_updated.py
  checkpoint_updated.py
  parallel_config_updated.py
  config_loaders_updated.py
  shared_views_updated.py
  config_parallel_hooks.py   ← edit get_shared_view_sql_map here
  ai_*.py
```

### Parallel config (default 3 workers)

Sequential steps, parallel **inside** each step:

```text
load_common_config  → prefetch DESCRIBE (3 tables) + load_common_config
load_config         → parallel if hooks / ai_config_service split tasks exist
register_shared_views → parallel if view SQL map resolved
```

```python
cfg["parallel_config_workers"] = 3   # default
cfg["parallel_config_workers"] = 1   # sequential
```

Logs:

```text
[parallel_config] config phase workers: max_workers=3
[parallel_config] load_common_config: 3 task(s), max_workers=3
[parallel_config] register_shared_views: 42 view(s), max_workers=3
```

**Important:** populate `config_parallel_hooks.get_shared_view_sql_map(cfg)` with your
`ai_shared_views.py` view SQL for parallel view registration (or add
`get_shared_view_sql_map` to `ai_shared_views.py`). Without a SQL map, views fall back
to sequential `register_shared_views`.

Optional: `load_config_tasks` in `config_parallel_hooks.py` for 3-way `load_config` split.

## Imports in `load_allocation_input_updated.py`

```python
from .step_timer_updated import StepTimer
from .checkpoint_updated import checkpoint, drop_checkpoints
from .ai_k1_service import ...
```

## `checkpoint_updated` vs `Common_V2.core.checkpoint`

| | Production `checkpoint` | `checkpoint_updated` |
|--|-------------------------|----------------------|
| Default | UC Delta `_tmp_{name}_{run_id}_{uniq}` | **auto**: uncompressed Parquet on volume if `volume_path` set |
| Lineage break | Yes | Yes |
| Cleanup | `drop_checkpoints` → DROP TABLE | DROP TABLE + remove volume dirs |
| Retries | UC transient errors | Same + volume write retry |

### Runtime volume (forced for `_updated`)

```python
VolumePath="/Volumes/qa7/datavolume/databrickdata/checkpoint"
```

Checkpoints write to:

```
/Volumes/qa7/datavolume/databrickdata/checkpoint/_checkpoints/{run_id}/{name}_{uuid}/
```

Uncompressed Parquet files. Removed by `drop_checkpoints` at end of run.

When `VolumePath` is passed, `load_allocation_input_updated` sets:

```python
cfg["checkpoint_backend"] = "volume"
```

Logs should show `[CHECKPOINT] volume write:` not `delta write:`.

### Parallel flow-up writes (default 3)

Nine flow-up tables are prepared and saved with `ThreadPoolExecutor(max_workers=3)`:

```python
cfg["parallel_write_workers"] = 3   # default in _updated
cfg["parallel_write_workers"] = 1   # sequential (production-like)
```

Log:

```text
[write] parallel flow-up table writes: max_workers=3
[store] Writing 9 tables (parallel_workers=3): ...
```

Each table uses its own `GenericResultStorer.save_results({one_table: df})`.

### Uncompressed output writes (default)

```python
cfg["write_compression"] = "uncompressed"  # default in _updated
```

Sets `spark.sql.parquet.compression.codec=uncompressed` for:

- `AllocationInput` Delta write (`compression=uncompressed`)
- 9 flow-up tables via `GenericResultStorer` (parallel or sequential)

Checkpoints already use uncompressed Parquet on volume (`checkpoint_updated.py`).

To match production compression:

```python
cfg["write_compression"] = "snappy"  # or remove / set parallel_write_workers only
```

### A/B checkpoint test

Same run params; flip only:

```python
cfg["checkpoint_backend"] = "delta"   # match production
# vs
cfg["checkpoint_backend"] = "volume"  # needs volume_path
```

## Benchmark notebook

`notebooks/benchmark_load_allocation_input.py` — widgets `sp_name`, `number_of_run`, plus run params.

Each pass runs **original** `load_allocation_input` then **updated** `load_allocation_input_updated` and records wall time.

## Runner

Only Change 2 — `load_allocation_input_updated` (not `sp_name`).
