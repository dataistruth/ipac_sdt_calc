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

## Checkpoints (fast UC temp Delta, stats disabled)

Pipeline lineage breaks write throwaway `_tmp_*` UC Delta tables with **Delta
data-skipping statistics disabled** (`delta.dataSkippingNumIndexedCols=0` +
`spark.databricks.delta.stats.collect=false`) and **uncompressed Parquet**. Checkpoints
are read back once and dropped, so per-column min/max stats add write cost with no benefit.

`VolumePath` is only for **final flow-up outputs** (GenericResultStorer), not checkpoints.

| Backend | Flag | Behavior |
|---------|------|----------|
| `fast_delta` (default) | — | Temp Delta, stats off, uncompressed |
| `Common_V2` | `checkpoint_use_production=True` | Exact production parity (stats on, snappy) |

Cfg toggles: `checkpoint_disable_stats` (default `True`), `checkpoint_compression` (default `uncompressed`).

| Step | When |
|------|------|
| `pfic_snapshot` | After phase 6a |
| `alloc_input` | After phase 6c |
| `base_flowup` | Inside flowup — `post-reclass`, `post-zero` |
| `pfic_raw` | After phase 7a |
| `pfic_flowup` | After phase 7b |
| `alloc_filtered` | After post-filters |
| `alloc_tagged` | After phase 8 (if investment tag workflow active) |

## Phase 7a optimizations (broadcast / cache)

See `ai_pfic_flowup_service.py` — RunID partition pruning, parallel shared views, etc.
