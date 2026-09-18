# K3 Allocation Summary outputV2

This candidate is isolated under `outputV2`. The production package in
`sdt_d/Source/AllocationV2/usp_load_k3_allocation_summary/output` was used only
as the baseline and was not modified.

## Entry and dependency map

The public entry remains `run_usp_load_k3_allocation_summary`.

- S1 loads SP configuration and resolves the income-attribute transaction.
- S2 `country_sic` and S3 `income_attr_import` are independent after S1.
- S4a `k3_detail` and S5a `mapped_lines` are independent of each other after S1.
- `rounding_flags` depends on `k3_detail` and `income_attr_import`.
- mapped-line existence depends on `mapped_lines`; `k1_amounts` then depends on
  `country_sic`, `mapped_lines`, and that result.
- S6 depends on all prepared inputs. S7 consumes S6 and `k1_amounts`.
- S8 or S9 consumes S7. S8 is a fixed-point rank loop whose rank N reads the
  checkpointed summary produced by rank N-1.
- S10 consumes the selected rounding branch. S11 stores
  `K3AllocationSummary`.

## Applied parallelism

One bounded `early-prep` pool contains the four proven-independent builders:
`country_sic`, `income_attr_import`, `k3_detail`, and `mapped_lines`. It is
capped by the effective `MaxThreads` value, task count, and the global maximum
of four.

The builders in each pool only construct independent DataFrame plans from
read-only configuration. Futures return isolated values and failures are
re-raised on the caller thread. Pool results are merged in declaration order.
No pool spans mapped-line existence, S6-S10, writes, checkpoint ordering, or
the country-rank loop. The normalized global maximum is 4.

## Checkpoint policy

`resolve_checkpoint_mode` applies explicit `CheckpointMode`, explicit
`checkpoint_mode`, config values in the same order, and finally the shared
default. `initialize_checkpoint_V2` runs once with that result before the pool.
No `CheckpointProfile`, coalesce/repartition control, runtime seam bypass, or
hot-path cleanup exists.

All production seams are retained:

- `k3_summary`
- country branch: `k3_detail`, `k1_amounts`, `rounding_diff`
- country implementation: `k3sp_total_by_country`,
  `k3sp_rank_by_line`, and `k3sp_rank{r}_summary`

The S8 implementation reuses the production function code with an isolated
globals dictionary that substitutes `checkpoint_V2`. It does not mutate or
monkeypatch the production module, and the per-rank loop stays sequential.

## Pruning, broadcasts, and caching

Existing production RunID pruning is preserved on
`LookThroughAllocationOutput`, `K3LookThroughCompleteAllocationDetail`,
`K3MappedAllocableLinesDetail`, and `K1AllocationSummary`. Existing bounded
lookup broadcasts are preserved. The unbounded production broadcast hint on
`MAP_DerivedLines` is removed; the join and output are unchanged, while the
single filtered `Offset` attribute lookup remains broadcast. No speculative
cache was added: checkpointed fan-out and rank-loop state already define the
intended materialization points.

## Profiling

Profiling is opt-in through `ProfilePlan` / `profile_plan`; the threshold
defaults to 30. Production builders are wrapped with the shared profiler.
Checkpoint inputs are recorded by `checkpoint_V2`. Explicit mapped-line
existence and final result storage actions are profiled. Driver output emits
separate BUILDER, CHECKPOINT, and ACTION reports. Per-run records are also
available in `LAST_RUN_DIAGNOSTICS`.

The max-country-rank `.first()` remains inside the unchanged production S8 code
object and is not separately action-wrapped; its containing S8 builder and both
adjacent checkpoint plans are still measured.

## Checkpoint recommendations

- Keep `k3_summary`: it fans out to S7, either rounding branch, and S10, with
  joins/windows downstream.
- Keep `k3_detail`, `k1_amounts`, and `rounding_diff` on the country path:
  each is reused across the country workflow and protects the fixed-point loop
  from recomputing shared upstream lineage.
- Keep `k3sp_total_by_country` and `k3sp_rank_by_line`: both are reused by the
  per-rank loop and post-loop residual plug.
- Keep every `k3sp_rank{r}_summary`: it materializes the mutation boundary
  required before rank `r+1` recomputes tied partners.
- Add no new seam until Databricks action and checkpoint timings demonstrate
  recomputation cost greater than materialization cost.

These are structural recommendations. No runtime speedup is claimed before the
A/B notebook runs on representative Databricks data.

## Mutation-safe A/B acceptance

Run `notebook/benchmark_usp_load_k3_allocation_summary.py`. It supports both
execution orders and alternates by default. For Delta output, it snapshots the
selected RunID, purges before each variant, and restores the exact baseline in
`finally`. If the output table did not exist, a benchmark-created table is
removed during restoration. Parquet output is fingerprinted directly from the
returned `FilePathInfo`.

Parity requires matching schema, row count, `Amount` sum, and order-independent
row fingerprint for `K3AllocationSummary`. Accept the optimization only after
both orders pass and wall-time improvement exceeds normal run variance.
