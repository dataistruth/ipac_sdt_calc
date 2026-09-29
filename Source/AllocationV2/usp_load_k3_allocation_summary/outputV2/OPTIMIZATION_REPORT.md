# K3 allocation summary — Development outputV2

Production `output/` is unchanged. Single write:
`K3AllocationSummary`.

## Logic vs production

Same S1–S11 order after the two independent load phases. Country-level
rounding still uses production `_country_rounding` internals.

## Phases

- `independent_early_loads`: `country_sic`, `income_attr_import`
- `independent_inputs`: `k3_detail`, `mapped_lines`

No fake parallel writes.

## Checkpoints

Checkpoint V2 on `k3_summary`, and when country-level also `k3_detail`,
`k1_amounts`, `rounding_diff`. Local backends reset with `toDF`.

## Validation

FEP EntityID `4137` / RunID `17376`, catalog `QA7`. Hash
`K3AllocationSummary` after inspecting live columns.
