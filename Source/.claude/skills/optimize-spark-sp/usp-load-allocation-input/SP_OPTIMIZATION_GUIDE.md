# uspLoadAllocationInput — optimization guide

Companion to the SP-specific skill. When generating Development
`outputV2`, match the locked tree and APIs in `SKILL.md`. Production
`output/` is the correctness baseline.

## What this SP does

`run_load_allocation_input` loads AllocationInput from forms, K1-related
sources, PFIC, and custom footnotes, builds PFIC/form flowups, then
writes AllocationInput (Delta) and flow-up tables.

## Dependency map (do not flatten)

Config and `register_shared_views` first (`reclass_data` checkpoint in
production helpers).

Hierarchy registers `_entity_hierarchy_{run_id}` (Part V/VII). Lower-tier
funds are a shared view. Workflows feed forms and PFIC snapshot.
Validations can insert `AllocationRunErrors` and abort.

After purge no-op: form inputs, K1-related inputs, and PFIC snapshot are
independent plans (`independent_input_builders`).

Then sequential: checkpoint `pfic_snapshot`, union form+K1, checkpoint
`alloc_form_k1`, add PFIC allocation rows and custom footnote input
(`_cf_latest_txn_*`), checkpoint `alloc_input`, PFIC flowup pipeline
(inner `base_flowup` 7a-1 / 7a-2), checkpoint `pfic_raw`, XML alert,
election deletes, Part V/VII, checkpoint `pfic_flowup`, filters,
`alloc_filtered`, tags, optional `alloc_tagged`.

`alloc_form_k1` and `alloc_input` are intentionally separate. The former
cuts the form+K1 lineage before PFIC/custom-footnote unions; the latter
materializes the completed AllocationInput consumed downstream. In the
RunID 16560 observation, the old 18.015s `alloc_input` materialization
became 5.182s (`alloc_form_k1`) + 3.383s (`alloc_input`), and the following
7a-1 phase dropped from 8.5s to 5.7s. Keep both seams in future Production
inline and Development outputV2 generation. Validate total wall time with a
same-cluster A/B because write routing and cluster warmth can dominate the
end-to-end result.

Then `output_collect` (3 tasks) and sequential AllocationInput Delta
write, then `output_writes` (one task per remaining table, max 4 workers).

## Parallel groups

| Group | Tasks |
|---|---|
| `independent_input_builders` | `build_all_form_inputs`, `build_k1_and_related_inputs`, `build_pfic_snapshot` |
| `output_collect` | AllocationInput collect, PFIC flowup collect, **one** FormFlowups collect |
| `output_writes` | each flow-up table after AllocationInput commits |

Orchestrator applies `ExecutionProfile` at start, default `DEFAULT_EXECUTION_PROFILE` (**small**).

## Packaging

Development: `outputV2/` importing `output/` via `parent.py`. Write and
collect helpers live in `ai_finalization_service.py` (threading from `Common_V2.core.write_helpers`). No `form_flowup_collect.py`.
No copies of `ai_*.py` under `outputV2/`.

## A/B reconcile

- Snapshot tables that have `RunID` on the live schema.
- Do not snapshot `PFICUpdateAlert` / `PFICAlertDetails` with RunID.
- Notebook and `output_reconcile.py` must be the same `source_path` tree.

## Locked timings (RunID 16560)

Production ~67.6s reported / ~69.8s wall. Updated **48.4s** reported /
**49.2s** wall, hashes PASS, ProfilePlan off, profile small.
