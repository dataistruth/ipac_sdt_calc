# Allocation Input optimization candidate

Production source: `sdt_d/Source/AllocationV2/usp_load_allocation_input/output/`.
Production files are unchanged. All candidate changes are isolated here.

## Runtime contract

- Shared checkpoints: `Common_V2.core.checkpoint_V2`
- Default `CheckpointMode=2`: odd local, even stats-off Delta
- No checkpoint cleanup on the SP hot path; common EOD cleanup owns temporary
  Delta tables and Volume paths
- Shared parallel default/cap: four threads
- `ProfilePlan=off` by default; enable only for diagnostic runs
- No `persist()`/`cache()` on serverless; bounded lookup broadcasts only

## Parallel stages

- Shared-view DataFrame construction and `reclass_data` checkpoint
- Independent non-gating validation warning checks; error-table commits remain
  serialized
- Writes to nine distinct flow-up output tables

## Read optimization

- Current RunID plus ClientID/TaxPeriodID filters are pushed into reads when
  those columns exist
- Lower-tier facts are pruned by a broadcast left-semi join to allowed run IDs
- Proven-small lookup views are broadcast-hinted

## Checkpoint evidence

The speculative `form_inputs` checkpoint was removed: it cost 5.9 seconds and
saved only 3.1 seconds at `alloc_input` (net regression approximately 2.8s).
Checkpoint recommendations remain diagnostic; node count alone never causes a
new materialization.

## Acceptance

Run `notebook/benchmark_load_allocation_input.py` in both execution orders.
Accept only when all ten output table fingerprints match and timing improvement
exceeds serverless run-to-run variance. Use `ProfilePlan=off` for timing and
turn it on separately for BUILDER/CHECKPOINT/ACTION diagnostics.
