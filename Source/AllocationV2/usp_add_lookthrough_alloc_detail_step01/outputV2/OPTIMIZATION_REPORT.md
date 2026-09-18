# uspAddLookThroughAllocationDetail Step 01 — outputV2

## Scope and invariants

- Production source remains unchanged.
- Public entry remains `run_add_lookthrough_allocation_detail_step01`.
- Business filters, projections, aggregations, output names, empty-output
  suppression, and `GenericResultStorer` behavior come from the unchanged
  production module.
- Entry checkpoint overrides default to `None`. Resolution honors explicit
  PascalCase, explicit snake_case, cfg PascalCase, cfg snake_case, then the
  single shared `checkpoint_V2.DEFAULT_CHECKPOINT_MODE`; initialization occurs
  once before worker cfg copies.
- There is no checkpoint profile, lean/bypass policy, coalesce/repartition,
  or hot-path checkpoint cleanup.

## Dependency map

`load_common_config` and `_load_config` are gating and remain sequential.
`LookThroughAllocationOutput`, filtered by RunID, ClientID, and TaxPeriodID
when present, becomes the shared `base_lt_out`. A broadcast one-row
RunID/ClientID relation applies a bounded left-semi prune. The twelve
production writer sections consume that base
or independent side inputs and only collect lazy DataFrames into invocation
local result dictionaries. Their results are merged in production order before
one result-storer call.

The two dated-transfer sections both append
`K1LookThroughDatedTransferAllocationDetail`. They execute sequentially inside
one pool task and are merged in their original order. Every other section task
has an isolated shallow cfg copy and isolated `_parquet_results`.

## Thread pools

One `ThreadPoolExecutor` runs up to four workers over eleven independent tasks:
ten single-writer tasks and one ordered two-writer dated-transfer task. The
checkpointed base DataFrame and scalar config are read-only shared state.
Exceptions propagate to the caller; results merge deterministically.

No output-table writes occur in worker threads. The final
`GenericResultStorer` call remains singular and ordered.

## Checkpoints

`base_lt_out` is the sole checkpoint seam. It is retained from the proven
updated overlay because it fans out to all twelve section writers. It uses
`Common_V2.core.checkpoint_V2`; the shared default currently selects local for
this first checkpoint, with the shared implementation's stats-off Delta
fallback.

This SP's production conversion has no other checkpoint seam. No cleanup runs
on the request hot path; unique V2 objects are left for the shared sweeper.

Recommendation: **keep and measure** `base_lt_out`. Fan-out is high (12), but
the input plan is a filtered table scan, so benchmark checkpoint time against
avoided repeated scans. The notebook emits builder, checkpoint, and action
profiles plus an evidence table containing nodes, depth, operators, consumers,
fan-out, and measured checkpoint time.

## Other optimizations

- RunID, ClientID, and available TaxPeriodID predicates are pushed into the
  shared base read.
- The broadcast relation contains exactly one invocation key and is therefore
  provably bounded; no unbounded fact or speculative lookup is broadcast.
- No cache/persist was added; the materialized V2 checkpoint provides reuse.

## A/B acceptance

`notebook/benchmark_add_lookthrough_alloc_detail_step01.py`:

1. snapshots RunID rows from all eleven written tables;
2. resets those rows before each production or outputV2 variant;
3. alternates execution order by default;
4. compares schema, row count, null counts, numeric sums, and an
   order-independent row fingerprint for every table;
5. restores the exact original rows and drops benchmark snapshots in `finally`;
6. displays timings, per-table parity, builder/checkpoint/action profiles,
   checkpoint activity, pool activity, and checkpoint recommendations.

The notebook must be run on Databricks with representative inputs. No runtime
performance or parity claim is made by local syntax validation.
