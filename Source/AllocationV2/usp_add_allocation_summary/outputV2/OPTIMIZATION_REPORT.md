# uspAddAllocationSummary outputV2

## Scope and invariants

The production implementation remains read-only in `sdt_d`. This package imports
that implementation at runtime and replaces only orchestration. The public
`run_add_allocation_summary` entry point, business builders, filters, joins,
schemas, write modes, and return behavior are preserved.

The existing `pfic_alloc_text` checkpoint remains at the same seam. Its backend is
resolved by `resolve_checkpoint_mode`; the shared Common V2 constant is the only
default. The hot path does not bypass, coalesce, repartition, or drop checkpoints.

## Dependency and concurrency map

- Config and working tables run first. `AllocationInputWorkflow`, `LowerTierFunds`,
  `AllocationOutput`, `AllocationOutputSummary`, and reclass inputs retain their
  production RunID pushdowns.
- One bounded pool contains 13 independent writers to distinct target tables.
  It is capped at four workers and each task receives an isolated result accumulator.
- K1 and M1 main/periodic writes remain sequential inside their respective tasks
  because each pair targets one table.
- PFIC reclass, PFIC summary, the `pfic_alloc_text` checkpoint, and PFIC text write
  remain one serial chain.
- Custom-footnote planning and its two same-table writes remain serial.
- Form200616 direct write, same-table read, and LTF append remain serial.
- Form8886 direct/reclass writes remain sequential inside one task.
- Result-file metadata is merged in declaration order after each pool.

Production RunID pruning and existing bounded broadcasts are preserved. The
run-pruned, three-column `LowerTierFunds` relation and filtered partner set are
broadcast; no unbounded fact is broadcast. No cache was added because local
measurements do not show a repeated-action benefit.

## Profiling

`ProfilePlan=on` starts builder, checkpoint, and action sinks for one invocation.
The candidate profiles working-table, PFIC, custom-footnote, and partner builders,
every result-store action, and the preserved V2 checkpoint. Reports use the shared
`AllocationV2.plan_profiler` implementation and print BUILDER, CHECKPOINT, and
ACTION recommendations.

Initial checkpoint classification:

- `pfic_alloc_text`: **keep pending measurement**. It protects a real
  read-own-writes/materialization boundary immediately before
  `PFICFootnoteAllocationText`. The benchmark must confirm its plan size and
  materialization cost before any future seam change.
- No additional checkpoint is proposed. The remaining summary outputs are terminal,
  predominantly single-consumer writes; node count alone would not justify a break.

## Full-table A/B benchmark

Run
`outputV2/notebook/benchmark_add_allocation_summary.py` on Databricks. It:

1. snapshots the selected RunID in all 17 output tables;
2. alternates production and outputV2 order over two passes by default;
3. purges only that RunID before each variant;
4. captures wall/reported time and checkpoint/profile records;
5. compares schema, key null counts, all numeric aggregates, row count, and an
   order-independent row fingerprint for every table;
6. stops at the first mismatch; and
7. restores and verifies the pre-benchmark table state.

Acceptance requires every table to match in both execution orders and updated wall
time to improve beyond normal run variance. Runtime timings and plan rankings must
be recorded from the notebook; they cannot be established by local syntax checks.
