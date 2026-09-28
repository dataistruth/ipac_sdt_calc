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
8. Parallel collect into `_parquet_results`, then sequential Delta
   AllocationInput write and Parquet storer flush.

## Thread pools and safety

Pools cap workers at `min(MaxThreads, task_count, 4)`.

- `independent_input_builders` (3 tasks): `build_all_form_inputs`,
  `build_k1_and_related_inputs`, `build_pfic_snapshot`. Isolated `cfg`.
  No temp views in these three.
- `output_writes` (3 tasks): collect-only into isolated
  `_parquet_results`, then merge. Disk flush stays ordered.

Custom footnotes, hierarchy, shared views, PFIC flowup chain, and the
Delta-then-Parquet flush remain sequential.

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

Local validation is syntax compilation. Databricks catalog parity is not
yet accepted. Acceptance requires matching fingerprints in both orders
and improvement beyond run-to-run variance.
