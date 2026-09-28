# Look-through cost allocation — Development outputV2

Production `output/` is unchanged. `outputV2` calls the same builders.

Extra Checkpoint V2 seams follow the footnotes pattern that saved ~12s:
`partners`, `input_raw`, `cost_percentages`, `temp_alloc_input`, `fep`,
`alloc_pass1`–`alloc_pass4`, plus production `alloc_output`. Writes are
sequential (Output then Input).

A/B notebook defaults: EntityID 4137, RunID 17376, LineType `K1 with Cost`.
