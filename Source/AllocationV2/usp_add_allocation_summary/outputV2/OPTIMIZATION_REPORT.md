# Add allocation summary — Development outputV2

Production `output/` is unchanged. `outputV2` wraps production writers via
`parent.output_module("add_allocation_summary")`.

## Logic vs production

Same `load_sp_config`, working tables, partner PFIC, PE-book gate, and
the same 17 `_store_result_table` targets. Same-table appends (K1, M1,
custom footnote, Form200616, Form8886) stay inside one writer task.

## Phases

- `independent_builders`: PFIC reclass vs custom-footnote transactions
- `output_writes`: distinct-table writers with isolated `{**cfg}` and
  merged `_result_file_infos`

## Checkpoints

Checkpoint V2 on multi-consumer working frames:
`allocation_output_summary`, `allocation_output`, `k1_workflow`,
`lower_tier_funds`, `at_risk_workflow`, `partner_pfic`. Local backends
reset with `toDF(*columns)`. Production still checkpoints PFIC text
inside its writer (legacy drop in `finally`).

## Validation

Local syntax only until Databricks A/B on EntityID `4137` / RunID
`17376`, catalog `QA7`. Frozen 10 widgets; ProfilePlan off.
