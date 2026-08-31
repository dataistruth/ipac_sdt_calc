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

## Checkpoints (default: UC Volume Parquet when `VolumePath` is set)

Pipeline lineage breaks write Parquet to `{VolumePath}/_checkpoints/{RunID}/` — serverless-safe and faster than UC Delta temp tables.

| Backend | When |
|---------|------|
| `volume` (default with `VolumePath`) | Parquet on volume, `compression=uncompressed` |
| `delta` | UC temp tables — `checkpoint_backend="delta"` |

Override codec: `checkpoint_compression="snappy"` (default is `uncompressed`).

| Step | When |
|------|------|
| `pfic_snapshot` | After phase 6a |
| `alloc_input` | After phase 6c |
| `base_flowup` | Inside flowup — `post-reclass`, `post-zero` |
| `pfic_raw` | After phase 7a |
| `pfic_flowup` | After phase 7b |
| `alloc_filtered` | After post-filters |
| `alloc_tagged` | After phase 8 (if investment tag workflow active) |

`VolumePath` is used for **checkpoints** (`_checkpoints/`) and **final flow-up outputs**.

## Phase 7a optimizations (broadcast / cache)

Kept in `updated.ai_pfic_flowup_service`:

- Broadcast `_pfic_line_item`, `_entity`, `_fx_avg_rate`
- Cached `_lower_tier_funds_{run_id}`
- Broadcast `PficForeignCorpClassificationInput`, cached `_zero_fa_only_ids`
- `register_reclass_unblocked`, `reclass_wf_id > 0` gate

## Other optimizations

- Parallel shared view registration (`parallel_config_workers`, default 4)
- Parallel flow-up Delta writes (`parallel_write_workers`, default 4)
- Uncompressed Parquet on checkpoints and final outputs
