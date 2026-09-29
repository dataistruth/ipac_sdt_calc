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

## Notebook

Frozen 10 widgets. LT identity 4755 / 18266 / qa7.
Hardcode RankForRulePickup=0, ResultType `deltalake`, VolumePath
`/Volumes/qa7/datavolume/databrickdata`. Restore both SM tables; do not
purge Output.
