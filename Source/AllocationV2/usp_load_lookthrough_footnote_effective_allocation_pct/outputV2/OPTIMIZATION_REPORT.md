# Look-through footnote effective allocation % — Development outputV2

Production `output/` is unchanged. `outputV2` calls the same production
builders in the same order.

## Logic vs production

Identical gates, K1 short-circuit, yearly/partners/FEP/LT output,
cost then book then union, temp input empty exit, classify → K1 amounts
→ final % → build frames, then Output append then Input overwrite.

## Checkpoints (footnotes-style extras)

Production only materializes `lt_output`. outputV2 keeps that seam and
adds plan breaks on the other multi-consumer frames:
`distinct_mappings` (after the temp-input empty gate), `yearly_line_amounts`, `partners`, `fep`,
`temp_final_eff_pct`, `temp_alloc_input`, `single_percent`,
`k1_amount_pct`, `final_pct`, `alloc_output`, `grouped_output`.
Local V2 backends get the footnotes `toDF` qualifier reset.

## Validation

Local syntax only until a new Databricks A/B on EntityID `4755` /
RunID `18266`, catalog `qa7`, schema `iPC_2025_QA7_15348` with matching
hashes. Frozen 10 widgets; ProfilePlan off; original then updated.

A/B restore uses replaceWhere from the backup plus `refreshTable` so
updated still sees BoxJKL `LookThroughAllocationInput` rows after
original's partition overwrite.
