---
name: optimize-usp-sm-load-lookthrough-effective-allocation-pct
description: Development outputV2 for usp_SM_LoadLookThroughEffectiveAllocationPercentage. Imports output.services via parent; Checkpoint V2 on multi-consumer DFs; sequential SM Output then Input.
---

# Optimize usp_SM_LoadLookThroughEffectiveAllocationPercentage

Use only for
`AllocationV2/usp_sm_load_lookthrough_effective_allocation_pct`.

Import `AllocationV2....output.services.config_service` (and siblings)
via `parent.output_module("services.config_service")`. Public entry
`run_sm_load_lt_effective_alloc_pct`.

Checkpoint V2 on multi-consumer frames (`temp_alloc_input`, mapping,
amount passes, `alloc_output`). Sequential
`write_allocation_output` then `update_allocation_input`. Never
parallel Output+Input. Flow-up Output write stays before the main write.

## Notebook

Frozen 10 widgets. Same LT identity 4755 / 18266 / qa7. Restore both SM
tables.
