# Look-through footnote effective allocation % — Development outputV2

Production `output/` is unchanged. Candidate lives in `outputV2/` and
imports production builders via `parent.py`.

## What changed

- Orchestrator resolves `ExecutionProfile` (default **low**) at start:
  shuffle 32, CheckpointMode 4, MaxThreads 4. No AQE override.
- Production checkpoint seam `lt_output` now uses Checkpoint V2.
- Phase `independent_builders`: cost %, book %, temp allocation input
  (lazy plans; materialize on the main thread).
- Sequential writes matching production: LookThroughAllocationOutput
  append, then LookThroughAllocationInput RunID overwrite. Parallel
  writes caused a LookThroughAllocationOutput count mismatch.
- Sequential: config/skip gates, mapping expand (isEmpty), distinct
  mappings, K1 gate, yearly/partners/FEP/lt_output (Spark actions),
  single/multi classify, K1 amounts, final %, build output frames.

## Compared tables

- `LookThroughAllocationOutput` (`RunID`) — snapshot and restore; do
  not purge before a variant (SP reads this table in §7).
- `LookThroughAllocationInput` (`RunID`)

## Notebook defaults (from FEP Development run)

EntityID `4137`, ClientID `15348`, TaxPeriodID `1`, RunID `17376`,
catalog `QA7`, schema `iPC_2025_QA7_15348`, ExecutionProfile `low`,
ProfilePlan `off`, shuffle blank.

## Validation status

Local syntax check only. Databricks A/B hashes are not accepted yet.
Production baseline on this procedure is ~96s.
