# SM look-through effective allocation % — Development outputV2

Production `output/` is unchanged. `outputV2` imports
`output.services.*` via `parent.output_module`.

Checkpoint V2: production `sm_lt_filtered` / `sm_lt_input` / `k1_amount`
(via patched `amount_service.checkpoint`) plus extras
`distinct_mappings`, `temp_alloc_input`, `alloc_pass1`–`alloc_pass3`,
`alloc_output`. Sequential SM Output append then Input update.

Phase `independent_amounts`: K1 amounts || UBTI amounts after shared
temp input. Flow-up write stays sequential before those amounts when
the flow-up gate is true.

A/B identity: EntityID 4755, RunID 18266, catalog qa7. Restore both SM
tables; do not purge Output.

Unwrapped production internals: `mapping_service` tagged-union
`localCheckpoint`, `compute_effective_percentages` eager
`localCheckpoint` (replaced by V2 `alloc_output` after residual/PE book).
