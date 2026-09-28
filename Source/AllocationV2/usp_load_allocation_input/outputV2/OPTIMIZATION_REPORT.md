# Load Allocation Input outputV2

## Scope

The public entry remains
`outputV2/load_allocation_input.py::run_load_allocation_input`.
Production `output/` is unchanged. Unmodified business services are loaded
through `parent.py` from this SP's live `output/` package. Orchestration
changes are isolated in `outputV2`.

The SP orchestrator resolves `ExecutionProfile` (default `low`) at run
start from `Common_V2.core.execution_profiles`. Explicit checkpoint,
shuffle, and MaxThreads override the profile. AQE is not set.

Orchestrator seams use `Common_V2.core.checkpoint_V2`. Production helpers
may still call the legacy checkpoint helper for `reclass_data` and inner
`base_flowup`. There is no checkpoint coalesce/repartition or hot-path
cleanup on V2 seams.

## Complete dependency map

1. Common config -> SP `load_config` -> shared temp views.
2. Sequential hierarchy (temp view) + lower-tier view read + workflows.
3. Gating validations (may write AllocationRunErrors) then purge no-op.
4. Independent builders: form inputs, K1-related inputs, PFIC snapshot.
5. Checkpoint `pfic_snapshot`; PFIC allocation union; custom footnotes
   (temp view); checkpoint `alloc_input`.
6. PFIC flowup, `pfic_raw`, XML alert, election deletes, Part V/VII,
   `pfic_flowup`.
7. Filters, `alloc_filtered`, tags, optional `alloc_tagged`.
8. Parallel `output_collect` (AllocationInput, PFIC flowup, FormFlowups),
   sequential AllocationInput Delta `replaceWhere`, then parallel
   `output_writes` (one Delta write per remaining table, max 4 workers).

## Thread pools and safety

Pools cap workers at `min(MaxThreads, task_count, 4)`.

- `independent_input_builders` (3 tasks): `build_all_form_inputs`,
  `build_k1_and_related_inputs`, `build_pfic_snapshot`. Isolated `cfg`.
  No temp views in these three.
- `output_collect` (3 tasks): AllocationInput, PFIC flowup, all form
  flowups.
- `output_writes`: one Delta write per flow-up table after the sequential
  AllocationInput commit. Writer is constructed in each task.

Custom footnotes, hierarchy, shared views, and the PFIC flowup chain
remain sequential.

## Checkpoint parity (orchestrator)

| Seam | Candidate |
| --- | --- |
| `pfic_snapshot` | `checkpoint_V2` |
| `alloc_input` | `checkpoint_V2` |
| `pfic_raw` | `checkpoint_V2` |
| `pfic_flowup` | `checkpoint_V2` |
| `alloc_filtered` | `checkpoint_V2` |
| `alloc_tagged` | `checkpoint_V2` when tag workflow is on |

## Benchmark and reconciliation

`notebook/benchmark_load_allocation_input.py` snapshots every compared
table for the RunID, purges before each variant, and restores in
`finally`.

## Validation status

Locked exact-parity A/B (2026-09-28, RunID 16560, ProfilePlan off,
ExecutionProfile low): production **67.6s** reported / **69.8s** wall;
updated **48.4s** reported / **49.2s** wall; all fingerprints PASS.
Production store 17.2s vs `output_writes` 5.666s. Do not regenerate a
different packaging without a new isolated A/B that beats 48.4s with
parity.
