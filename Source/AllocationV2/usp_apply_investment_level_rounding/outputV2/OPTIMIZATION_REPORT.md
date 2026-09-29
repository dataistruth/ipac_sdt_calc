# Investment-level rounding — Development outputV2

Production `output/` is unchanged. `outputV2` imports the same services via
`parent.py`.

Checkpoint V2 replaces production V1 seams (`temp_alloc_output`,
`rounded_diff`) and adds multi-consumer breaks (`lookthrough_output`,
`lookthrough_input`, `partners`, `temp_alloc_input`, `k1_summary`).

Phases: `independent_early` (lookthrough || partner snapshots),
`independent_builders` (allocation input || temp output || max type),
`output_writes` (IsRounded flag || AllocationSummary tables). Sequential
rounding branch and `write_final_summaries`.

A/B notebook defaults: EntityID 4137, RunID 17376, Catalog QA7. CallFrom
is None (not a widget). ResultType `deltalake`.
