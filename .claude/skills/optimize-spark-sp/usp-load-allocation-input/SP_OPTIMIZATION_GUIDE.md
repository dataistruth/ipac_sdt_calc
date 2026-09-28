# uspLoadAllocationInput — optimization guide

Companion to the SP-specific skill. Production `output/` is the
correctness baseline.

## What this SP does

`run_load_allocation_input` loads AllocationInput from forms, K1-related
sources, PFIC, and custom footnotes, builds PFIC/form flowups, then
writes AllocationInput (Delta) and flow-up tables (Parquet storer).

## Dependency map (do not flatten)

Config and `register_shared_views` first (includes `reclass_data`
checkpoint in production helpers).

Hierarchy registers `_entity_hierarchy_{run_id}` (needed by Part V/VII).
Lower-tier funds are already a shared view. Workflows feed forms and
PFIC snapshot. Validations can insert `AllocationRunErrors` and abort.

After purge no-op: form inputs, K1-related inputs, and PFIC snapshot are
independent plans.

Then sequential: checkpoint snapshot, PFIC allocation rows, custom
footnote input (registers `_cf_latest_txn_*`), checkpoint `alloc_input`,
PFIC flowup pipeline, `pfic_raw`, XML alert, election deletes, Part V/VII,
`pfic_flowup`, filters, `alloc_filtered`, tags, optional `alloc_tagged`.

Collect three disjoint `_parquet_results` groups, merge, then:

1. AllocationInput Delta `replaceWhere` RunID
2. remaining distinct flow-up tables in `output_writes` (up to 4 workers)

## Parallel groups

| Group | Tasks |
|---|---|
| `independent_input_builders` | forms, K1-related, PFIC snapshot |
| `output_collect` | collect AllocationInput / PFIC flowup / form flowups |
| `output_writes` | one disk write per distinct flow-up table |

Orchestrator applies `ExecutionProfile` at start, default `low`.

## Packaging

Development: `outputV2/` importing `output/` via `parent.py`. Collect
helpers live in `write_helpers.py`.

## A/B reconcile (do not regress on regenerate)

- List snapshot tables from writers / `_parquet_results` keys that use
  `RunID`, not from every `_collect_result` table.
- `output_reconcile.py` reads live columns before `WHERE RunID`.
- Notebook purge uses the same column check.
- `PFICUpdateAlert` / `PFICAlertDetails` are not RunID-scoped.
- Databricks must import the same generated `outputV2/` as the notebook;
  a stale workspace copy of `output_reconcile.py` is a sync failure, not
  a reason to inline snapshot SQL in the notebook.
