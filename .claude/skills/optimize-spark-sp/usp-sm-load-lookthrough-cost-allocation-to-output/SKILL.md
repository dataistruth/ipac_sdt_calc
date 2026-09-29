---
name: optimize-usp-sm-load-lookthrough-cost-allocation-to-output
description: Development outputV2 for usp_SM_LoadLookThroughCostAllocationToOutput. Imports production orchestrator builders; Checkpoint V2 extras; sequential SM Output then Input.
---

# Optimize usp_SM_LoadLookThroughCostAllocationToOutput

Use only for
`AllocationV2/usp_sm_load_lookthrough_cost_allocation_to_output`.

Do **not** copy `output/orchestrator.py`. Import it via `parent.py` and
wrap `run_sm_load_lookthrough_cost_allocation_to_output`. Patch
production `checkpoint` to Checkpoint V2 for recursive `hier_level_*`.

Extra seams: `temp_alloc_input`, `fep`, `alloc_pass*`, `alloc_output`.
Sequential **SM_LookThroughAllocationOutput then SM_LookThroughAllocationInput**.
Never parallel those two.

Development: flat `outputV2/` (no `business/`, `tests/`). Production:
inline in `output/`.

## Notebook

Frozen 10 widgets. `removeAll()` in its own cell, then:

| # | Name | Default |
|---|---|---|
| 1 | `source_path` | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| 2 | `EntityID` | `4755` |
| 3 | `ClientID` | `15348` |
| 4 | `TaxPeriodID` | `1` |
| 5 | `RunID` | `18266` |
| 6 | `CatalogName` | `qa7` |
| 7 | `SchemaName` | `iPC_2025_QA7_15348` |
| 8 | `ExecutionProfile` | `low` |
| 9 | `number_of_runs` | `1` |
| 10 | `ProfilePlan` | `off` |

Hardcode RankForRulePickup=0, ResultType `deltalake`, VolumePath
`/Volumes/qa7/datavolume/databrickdata`. Original then updated. Restore
both SM tables; do not purge Output. Last display cell uses explicit
Spark schemas.
