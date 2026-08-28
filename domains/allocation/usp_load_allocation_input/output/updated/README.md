# `usp_load_allocation_input` — optimized package

**Local repo (edit here):**

```text
ipac-sdt-calc/domains/allocation/usp_load_allocation_input/output/updated/
```

Full path:

```text
/Users/mukesh.singh/spark/ipac-sdt-calc/domains/allocation/usp_load_allocation_input/output/updated/
```

Sync **only this folder** to the monolith:

```text
Source/AllocationV2/usp_load_allocation_input/output/updated/
```

## Prerequisites on monolith (do not overwrite)

```text
output/
  load_allocation_input.py    # production
  ai_*.py                     # business logic
  __init__.py
  updated/                    # ← you sync here
```

`parent.py` loads `ai_*` modules from sibling `output/` via `importlib`.

## Deploy

From `ipac-sdt-calc/domains/allocation/usp_load_allocation_input/`:

```bash
export MONOLITH_SOURCE="/path/to/Source"
rsync -av output/updated/ \
  "$MONOLITH_SOURCE/AllocationV2/usp_load_allocation_input/output/updated/"
```

Or from the deloitte bundle (same content):

```bash
export MONOLITH_SOURCE="/path/to/Source"
./deploy.sh
```

## Run (Databricks)

Notebooks: `updated/notebooks/runner_load_allocation_input.py`

```python
from AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input import run_load_allocation_input

result = run_load_allocation_input(
    spark,
    EntityID=115,
    ClientID=15348,
    TaxPeriodID=1,
    RunID=16560,
    CatalogName="QA7",
    SchemaName="IPC_2025_QA7_15348",
    VolumePath="/Volumes/qa7/datavolume/databrickdata/checkpoint",
    CheckpointLevel="default",  # minimal | default | full
    parallel_config_workers=3,
    parallel_write_workers=3,  # same value drives flow-up parallel writes
)
```

## Checkpoint levels

| Level | Steps materialized |
|-------|-------------------|
| `minimal` | `pfic_snapshot`, `alloc_input` (debug only — slow) |
| `default` | + `reclass_data`, `lower_tier_funds`, `alloc_post_k1`, `pfic_raw`, `alloc_post_7b`, `pfic_flowup`, `alloc_filtered` |
| `full` | + `alloc_tagged` (when tag workflow active); inner PFIC `base_flowup` checkpoints |

### Parallel checkpoints

Independent materializations run concurrently (same worker pool as writes):

- **After phase 7b:** `alloc_post_7b` + `pfic_flowup` in parallel (`checkpoint_post_7b_parallel`)
- Controlled by `parallel_checkpoint_workers` (defaults to `parallel_write_workers`)

Logs:

```text
[checkpoint] parallel write: 2 table(s), max_workers=4
[checkpoint] parallel ok alloc_post_7b
[checkpoint] parallel ok pfic_flowup
```

### Lineage breaks (why each step)

| Step | After | Prevents replay of |
|------|-------|-------------------|
| `reclass_data` | shared views | Heavy reclass scan in PFIC flowup |
| `lower_tier_funds` | phase 2 | Hierarchy/LTF joins in 7a/7b |
| `alloc_post_k1` | phase 5 | Form + K1 unions before PFIC |
| `pfic_snapshot` | phase 6a | Snapshot build in 6b/7a |
| `alloc_input` | phase 6c | Full alloc build before 7a |
| `pfic_raw` | phase 7a | Entire flowup build in 7b |
| `alloc_post_7b` | phase 7b | Election deletes in post-filters/writes |
| `pfic_flowup` | phase 7b | Flowup + 7a in downstream writes |
| `alloc_filtered` | post-filters | Filter chain in phase 9 |

## PFIC flowup (`ai_pfic_flowup_service.py`)

Updated runs use **`output.updated.ai_pfic_flowup_service`** (not monolith `ai_pfic_flowup_service`).

Logs are prefixed with `[updated.ai_pfic_flowup_service]`.

Optimizations: broadcast `PficForeignCorpClassificationInput`, `register_reclass_unblocked` reuse,
`reclass_wf_id > 0` gate, cached `_zero_fa_only_ids`, skip inner checkpoints when `CheckpointLevel != full`,
custom footnote txn log without status `collect()`.

`CheckpointLevel=full` sets `skip_inner_pfic_checkpoints=False` (inner `base_flowup` checkpoints enabled).
