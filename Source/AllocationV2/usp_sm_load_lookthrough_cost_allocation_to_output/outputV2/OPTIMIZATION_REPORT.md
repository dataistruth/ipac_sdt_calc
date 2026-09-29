# SM look-through cost allocation — Development outputV2

Production `output/orchestrator.py` is unchanged and is not copied.
`outputV2` imports builders via `parent.py` and wraps
`run_sm_load_lookthrough_cost_allocation_to_output`.

Checkpoint V2: production `cost_pct_snapshot`, recursive `hier_level_*`
(via patched `orchestrator.checkpoint`), plus extras `temp_alloc_input`,
`entity_hier_final`, `alloc_pass1`–`alloc_pass3`, `alloc_input_final`,
`fep`, `alloc_pass4`, `alloc_output`. Sequential
SM_LookThroughAllocationOutput then SM_LookThroughAllocationInput.

Phase `independent_loads`: book effective || input load || cost % ||
entity/asset-class rel.

A/B identity: EntityID 4755, RunID 18266, catalog qa7. RankForRulePickup=0
(not a widget). Restore Output and Input; do not purge Output.
