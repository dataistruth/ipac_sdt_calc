# uspLoadFootnotesAllocationToOutput — optimization guide

Companion to the SP-specific skill. Production `output/` is the
correctness baseline.

## What this SP does

`run_load_footnotes_allocation_to_output` follows production sections
S1–S13. It generates footnote allocation rows, inserts them into
`AllocationOutput`, and deducts those amounts from the current RunID
partition of `AllocationInput`.

Empty allocation input or failed preconditions must still SKIPPED with
the production contract.

## Why production was ~144s

Two checkpoints replayed large upstream plans:

| Seam | Typical cost | Why it was expensive |
|---|---|---|
| `all_underlyings` | ~70.6s | eight-level hierarchy union plus cost union replay |
| `alloc_input` | ~25.2s | five allocation passes stacking left-anti lineage |

Walls in the 118–160s band are the same class of run.

## Proven extra seams

Keep the four production seams. Add:

1. `cost_snapshot` — four-way cost union + distinct, then derive
   underlying types from the materialized snapshot.
2. `entity_levels` — after the eight-level `all_levels` loop inside
   `build_entity_hierarchy`.
3. `alloc_pass1` … `alloc_pass4` — after each pass’s left-anti in
   `build_allocation_input`.

All seams go through `checkpoint_V2`. Inherit
`DEFAULT_CHECKPOINT_MODE` unless the orchestrator applies profile
`low` (`checkpoint_mode=4`) or the notebook sets `CheckpointMode`.

## Proven concurrency

Independent at submit time:

- S3: book effective, temp allocation input, zero-exclude lines, final
  effective pct.
- S5: cost percentage data (same pool as S3).
- S13: `write_allocation_output` and `apply_deduction` (distinct
  tables, isolated `cfg`).

Must stay sequential: S4 quarter updates; S6 hierarchy through S12
effective allocation.

Cap workers 1..4. Observe every future.

## Proven broadcasts

Quarter-update keys, Part-V lines, zero-exclude lines, custom footnote
line types, entity-scoped partners. Nothing else.

## Reconciliation

Snapshot AllocationInput (RunID) and generated footnote
AllocationOutput before the first variant. Restore both before every
variant. Compare both tables. Restore production state in `finally`.
Keep backup tables if restore fails.

## Packaging

Development (default until the user asks Production): sibling
`outputV2/` importing `..output` helpers. No `updated/`. No
`business/`. No FEP `outputV3` layout.

Production mode: same logic inline in `output/` with
`output/notebook/run_*.py` only — no profiler, no reconcile module.

## Historical walls

- Original ~144.5s–161.5s vs updated ~45s–51.5s with both-table PASS.
- ~32–35s is an unlogged recollection (likely mode 4 / warm cluster).
  Relock only with a printed A/B.
