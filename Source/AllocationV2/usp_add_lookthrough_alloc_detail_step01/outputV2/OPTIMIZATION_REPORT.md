# Look-through allocation detail step 01 — Development outputV2

Production `output/` is unchanged. Writers are production `_write_*`.

## Logic vs production

Same LookThroughAllocationOutput filter stored as `cfg["_base_lt_out"]`.
Dated-transfer and dated-transfer-without-adj stay sequential (same
parquet key). Other distinct-table writers run in `output_writes`.
`GenericResultStorer.save_results` stays sequential.

## Checkpoints

Checkpoint V2 on `base_lt_out`. After local backend, `toDF(*columns)`.

## Validation

Identity EntityID `4755` / RunID `18266`, catalog `qa7`. Restore
`LookThroughAllocationOutput` with overwrite+refresh; do not purge it
before variants. Hash the detail tables that have RunID.
