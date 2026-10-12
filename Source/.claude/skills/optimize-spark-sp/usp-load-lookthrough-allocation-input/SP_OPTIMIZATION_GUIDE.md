# uspLoadLookThroughAllocationInput — optimization guide

Companion to the SP-specific skill. Production `output/` is the
correctness baseline.

## What this SP does

`run_load_lookthrough_allocation_input` loads look-through allocation
input: workflows, FX, K1/adjustment/LT amounts, PFIC conversion, Box JKL
and exclusions, K3 validation, then appends result tables.

## Dependency map (do not flatten)

Independent after config: workflows, lower-tier funds, reclass K1.

FX depends on K1 workflows (temp view — sequential).

After FX: K1 input, adjustments input, and LT flow-up K1 are independent.

Then sequential: rounding, unions, LT adjustment/M1 flow-up,
`alloc_input_post_unions`, PFIC chain, `alloc_input_post_pfic`, Box JKL
through line exclusions, K3 validation.

Then independent writes: LookThroughAllocationInput, SchKTaxableIncome,
optional PFICtoK1IncomeAttributePercentages.

K3 / AllocationRun mutations stay sequential and run **before**
`output_writes`. On K3 FAIL, skip writes.

## PFIC EffPercentage (locked)

Production groups `recalc_grouped` twice and inner-joins totals.
Development `write_helpers.build_pfic_income_attributes` uses
`sum(Amount)` over EntityID/LineID/TrackingKey, then Amount/TotalAmount.
Null join keys dropped. Do not add a separate
`pfic_income_attributes.py`.

Locked updated A/B (RunID 18266, profile low, ProfilePlan off): wall
**18.380s**, `output_writes` **5.995s**, PFIC task **5.991s**,
fingerprints PASS.

## Checkpoints

Keep:

| Name | Why |
|---|---|
| `alloc_input_post_unions` | ~9 downstream consumers |
| `alloc_input_post_pfic` | ~7 downstream consumers |

Checkpoint V2 only. Do not drop for time.

## Parallel groups

| Group | Tasks |
|---|---|
| `independent_early_loads` | workflows, lower-tier funds, reclass K1 |
| `independent_input_builders` | K1 input, adjustments, LT flow-up K1 |
| `output_writes` | three distinct appends |

Orchestrator applies `ExecutionProfile` at start, default `low`.

## Reconciliation

Snapshot and hash all five tables listed in the skill. Restore in
`finally`.

## Packaging

Development: `outputV2/` importing `output/` via `parent.py`. Write
logic for the parallel phase lives in `write_helpers.py`, not in
production `lt_finalization_service.py`.

Production mode: same logic inline in `output/` only when the user
asks; no profiler.
