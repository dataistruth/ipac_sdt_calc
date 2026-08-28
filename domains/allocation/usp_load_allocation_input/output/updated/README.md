# `usp_load_allocation_input` — optimized package

**Local path:** `ipac-sdt-calc/domains/allocation/usp_load_allocation_input/output/updated/`

Sync **only this folder** to:

`Source/AllocationV2/usp_load_allocation_input/output/updated/`

## Run

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
    parallel_config_workers=4,
    parallel_write_workers=4,
)
```

## Checkpoints (original-aligned, Delta temp tables)

Intermediate lineage breaks use **Delta temp tables** in UC (`_tmp_*`), same as original
`Common_V2.core.checkpoint` — not volume Parquet unless `checkpoint_backend=volume`.

| Step | When |
|------|------|
| `alloc_input` | After phase 6c |
| `base_flowup` | After phase 7a build (single materialization) |
| `pfic_flowup` | After phase 7b |
| `alloc_filtered` | After post-filters |
| `alloc_tagged` | After phase 8 (if investment tag workflow active) |

`VolumePath` is for **final flow-up / storer outputs**, not checkpoints by default.

Opt-in volume checkpoints: `checkpoint_backend="volume"` (requires `volume_path`).

## Phase 7a optimizations (broadcast / cache)

Kept in `updated.ai_pfic_flowup_service`:

- Broadcast `_pfic_line_item`, `_entity`, `_fx_avg_rate`
- Cached `_lower_tier_funds_{run_id}`
- Broadcast `PficForeignCorpClassificationInput`, cached `_zero_fa_only_ids`
- `register_reclass_unblocked`, `reclass_wf_id > 0` gate

## Other optimizations

- Parallel shared view registration (`parallel_config_workers`, default 4)
- Parallel flow-up Delta writes (`parallel_write_workers`, default 4)
- Uncompressed Parquet on volume checkpoints and writes
