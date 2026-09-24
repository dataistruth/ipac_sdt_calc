# outputV3 architecture

`outputV3` is an isolated wrapper around unchanged production business
modules. It executes the production orchestrator in a private namespace and
forks only its `run_modes` control flow in `pipeline.py`. Checkpointing, timing,
bounded scheduling, and result-table storage are infrastructure seams. Every
DataFrame transformation, validation, and output builder remains a production
business function.

## Named stage contracts

The executable contracts are in `stages.py`:

1. `common_reads`
2. `with_lt_branch`
3. `no_lt_branch`
4. `mode_prep`
5. `fused_cpbt`
6. `fused_effective`
7. `output_build`
8. `output_write`

Profiles always contain all eight names and include operation-level timings.
The contracts identify production functions and whether parallel execution is
allowed.

## Parallelism

`MaxThreads`/`max_threads` is normalized to 1..4. The bounded executor runs:

- proven independent common read groups;
- with-LT and no-LT chains after their shared dependencies exist;
- valid mode 1/2/3 Pass A preparation;
- per-mode final output assembly;
- writes to distinct output tables.

`ParallelGroups` can be `all`, `none`, or a comma-separated subset of
`common_dimensions`, `common_inputs`, `lookthrough_metadata`,
`lt_nolt_branches`, `mode_prep`, `output_build`, and `output_writes`.
The benchmark records the effective set so each group can be disabled if the
Databricks A/B run shows cluster contention instead of an improvement.

Each branch receives a shallow cfg fork. DataFrames and immutable values are
shared, mutable containers are copied, and only checkpoint state/activity/table
and path collections are shared. The collections are list-compatible locked
append stores, while Common_V2's checkpoint sequence state retains its own
lock. Mode, `_current_mode`, empty-input flags,
`_part_v_quarters_df`, and computed branch artifacts remain private.

All futures are observed before an exception is re-raised on the main thread.
Fused CPBT, missing-entity/minimum-quarter handling, fused effective
calculation, plugging, and type updates remain sequential and call unchanged
production functions.

The sole branch artifact needed later is `_part_v_quarters_df`. Its producers
are schema-checked and merged using production mode order (mode 2 wins over
mode 1 when both produce the same run-scoped relation). Merge decisions are
captured in the profile. Other mode flags and outputs are never merged.

Mode 4 retains the complete production control flow. Its optional 704c path
can perform catalog metadata writes, and it has no independent sibling branch
that would justify parallel execution.

## Named checkpoint policy

`checkpoint_policy.py` selects a backend from the semantic checkpoint name,
never odd/even call position:

- durable Delta: common reused relations, LT/no-LT branch joins, reused fused
  CPBT outputs, transfer seams, and alias-sensitive/reused effective seams;
- local checkpoint: safe cheap lineage breaks only.

The implementation uses `Common_V2.core.checkpoint_V2` mode 1 as the Delta
primitive and mode 4 as the local primitive. Every decision records name,
stage, requested policy backend, actual backend, reason, and elapsed time.
Delta names remain sequence- and UUID-qualified. Cleanup is not performed in
the hot path.

An actual local checkpoint is returned through `toDF(*columns)`, preserving
the qualifier reset supplied by a fresh Delta table relation.

## Failure, retry, and cleanup behavior

Every task in a parallel group is observed before the coordinator raises the
first failure on the main thread. A failed branch prevents the fused stages
and output writes from starting; a failed output write fails the whole call.
There is no partial-success response.

Durable named Delta seams can survive executor loss during the current Spark
application. Local seams cannot: losing their executor blocks requires the
run to restart. outputV3 does not automatically resume from an earlier Delta
seam, because the public SP contract is a complete RunID recalculation.

Checkpoint tables have sequence- and UUID-qualified names, so a retry cannot
collide with a prior attempt. A failed run drops its own durable checkpoint
artifacts because no result DataFrame is returned. Successful-run artifacts
are deliberately not dropped in the hot path because returned DataFrames can
still reference them. An age-based
checkpoint hygiene job must remove stale `_tmp_v2_*` objects only after its
safety window; deploying that operational job is a prerequisite for scheduled
use. The benchmark always snapshots and restores the three business output
partitions in `finally`.

## Imports and public API

The package imports only Python standard-library modules, production modules,
and `Common_V2`. It does not import `outputV2` or `output/updated`. The copied
control flow is confined to `pipeline.py`; no production helper module is
copied.

Public APIs:

- `run_final_effective_percentages`
- `run_mode`
- `run_modes`
- `get_last_run_profile`
