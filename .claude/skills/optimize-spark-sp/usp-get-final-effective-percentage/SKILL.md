---
name: optimize-usp-final-effective-percentage
description: Optimizes the Spark implementation of uspGetFinalEffectivePercentage by creating or maintaining outputV3 from the production output baseline, applying the proven concurrency, checkpoint, CPBT, mode-preparation, effective-calculation, output-write, profiling, and exact-parity changes. Use when implementing, benchmarking, diagnosing, or extending performance work for this specific SP.
---

# Optimize uspGetFinalEffectivePercentage

Use this skill only for:

`AllocationV2/usp_get_final_effective_percentage`

The production implementation is the correctness baseline. The optimized
implementation is `outputV3`. Never improve benchmark results by weakening
business logic, validation, output persistence, or exact result comparison.

Read [SP_OPTIMIZATION_GUIDE.md](SP_OPTIMIZATION_GUIDE.md) before making a
structural change or interpreting a benchmark.

Presentation:

- [uspGetFinalEffectivePercentage_Optimization_Review.pptx](uspGetFinalEffectivePercentage_Optimization_Review.pptx)
- Regenerate it with `scripts/generate_optimization_ppt.py` using
  `python-pptx`.

## Paths

- Production baseline:
  `Source/AllocationV2/usp_get_final_effective_percentage/output/`
- Optimized implementation:
  `Source/AllocationV2/usp_get_final_effective_percentage/outputV3/`
- Shared checkpoint implementation:
  `Source/Common_V2/core/checkpoint_V2.py`
- Benchmark:
  `Source/AllocationV2/usp_get_final_effective_percentage/outputV3/notebook/benchmark_final_effective_percentage.py`
- Structure tests:
  `Source/AllocationV2/usp_get_final_effective_percentage/outputV3/tests/test_structure.py`

## Non-negotiable contracts

1. Treat production `output` behavior as authoritative.
2. Preserve all three result tables:
   - `FinalEffectivePercentages`
   - `FNFinalEffectivePercentages`
   - `SM_FinalEffectivePercentages`
3. Require exact fingerprints, schemas, row counts, and values.
4. Preserve RunID-scoped writes and production error handling.
5. Keep mode 4 on production control flow.
6. Keep all optimizations outputV3-local or guarded by an
   `_output_v3_*` flag that defaults to `False` in production.
7. Never use a runtime improvement from a failed or non-parity run.
8. Do not leave `__pycache__` or `.pyc` files in the repository.

## Target configuration

Use these promoted defaults unless a new exact-parity benchmark disproves
them:

- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- `ParallelGroups=all`
- CPBT input break: `both`
- missing-entity identity: enabled
- parallel dated/non-dated effective calculation: enabled
- broadcast `entity_partners`: enabled
- materialized effective inputs: enabled

## Implementation workflow

### 1. Establish the production baseline

Read the production orchestrator and every helper touched by the proposed
change. Record:

- control-flow order;
- actions such as `isEmpty`, `first`, `collect`, writes, and checkpoints;
- mutable `cfg` keys;
- aliases required by later joins;
- output schemas and write destinations.

Run production first. Capture wall time, reported time, row counts, schemas,
and output fingerprints.

### 2. Build or repair outputV3 isolation

If outputV3 is absent, create an isolated wrapper around the production
orchestrator. Copy orchestration control flow only. Reuse production business
helpers.

Required modules:

- `orchestrator.py`: public API, bounded scheduler, timing, configuration
- `pipeline.py`: optimized mode 1/2/3 control flow
- `cfg_isolation.py`: safe branch-local configuration
- `checkpoint_policy.py`: named checkpoints and reporting
- `stages.py`: stage contracts
- `parent.py`: isolated production loading
- `plan_profiler.py`: optional plan metadata
- `output_reconcile.py`: write coordination

### 3. Apply proven common-stage changes

- Run independent dimension and input builders concurrently.
- Materialize `cost_pct_m123`.
- Do not materialize the single-consumer `underlyings_common` relation.
- Materialize `uc_ordered_common`.
- Run with-LT and no-LT chains concurrently.
- Keep branch configuration isolated.

### 4. Apply proven mode-preparation changes

- Run modes 1, 2, and 3 concurrently.
- Keep `all_und_final_m2` and `fn_input_lines_m2`.
- Materialize `state_lines_m3` before its dated/non-dated fan-out.
- Run dated, non-dated, and transfer pre-CPBT checkpoints concurrently.
- Batch six footnote line-ID lookups into one Spark action.
- Share and materialize the PFIC allocation-input base across passes 2–6 when
  the loaded helper supports `checkpoint_fn`.
- Inspect helper signatures before passing newly added optional keywords so a
  warm Databricks interpreter cannot fail on an older loaded helper.

### 5. Apply proven CPBT changes

- Fuse modes 1/2/3 with `_mode` isolation.
- Materialize both dated and non-dated fused inputs.
- Keep `tcp_post_et_m0`, `all_ent_m0`, `parent_ord_m0`,
  `all_ent_pre_tag_m0`, and `tcp_post_tag_m0`.
- Generate cost-allocation TypeID variants with row expansion instead of
  four scans and redundant distincts.
- Narrow left-anti right sides to matching keys only.
- Prefilter transfer parent-match inputs by empty tracking key and
  `TrackingKeyMatch`.
- Drop `TrackingKeyMatch` after its final use.
- Optionally materialize `all_ent_post_tag_m0` only through its outputV3 flag.
- Materialize temp and transfer outputs concurrently.
- Materialize post-missing dated, post-missing non-dated, and final cost
  concurrently.
- Broadcast `entity_partners` for final-cost construction.

### 6. Apply proven effective-calculation changes

- Materialize post-minimum-quarter dated entities and minimum-quarter data
  concurrently before effective calculation.
- Run dated and non-dated effective calculations concurrently.
- Preserve dated helper barriers:
  - `eff_pct_dated_post_transfer`
  - `pickup_s4_m0`
  - `eff_dated_s5_m0`
  - `pickup_order_dated_pre_yearly`
  - `eff_dated_s6_m0`
- Use one pickup anti-join plus controlled row expansion to preserve the
  production duplicate-row multiset.
- Materialize plugged dated and non-dated outputs concurrently.

### 7. Apply output changes

- Build all valid per-mode outputs concurrently.
- Write the three distinct target tables concurrently.
- Observe every future before propagating a failure.
- Preserve production save logging and return contracts.

### 8. Validate

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  AllocationV2.usp_get_final_effective_percentage.outputV3.tests.test_structure
git diff --check
```

Then run the Databricks benchmark:

1. production;
2. outputV3;
3. exact comparison of all three tables;
4. checkpoint and wave timing review.

Reject the candidate if any table differs.

## Benchmark interpretation

Use wall-clock critical paths, not summed task duration.

Key evidence:

- overlapping tasks save only the shorter branch;
- helper construction time can hide eager checkpoint actions;
- deferred checkpoints move cost rather than remove it;
- stage totals may sum concurrent work and exceed wall time;
- output logs alone do not prove exact parity.

The latest measured outputV3 wall time is `57.736s`, down from `67.980s`
before the final optimization waves. The same-run production result was
`169.1s`. Treat these as historical evidence, not permanent thresholds.

## Known bad experiments

Do not promote these without a new isolated experiment:

- checkpoint mode 5 deferred materialization;
- eight shuffle partitions;
- broad lazy/action-lean checkpoint removal;
- broad fused mode-preparation lineage;
- candidate-claim CPBT rewrites;
- removing parent, tag, transfer, or dated fan-out barriers;
- stacked experiments that prevent attribution.

## Deployment checks

If a new outputV3 flag appears enabled but its checkpoint name is absent:

1. confirm the shared production helper file was deployed;
2. purge both `output` and `outputV3` modules;
3. inspect the loaded helper signature;
4. rerun from a clean benchmark pass.

Absence of `fn_alloc_pfic_m2` or `all_ent_post_tag_m0` indicates that the
corresponding shared-helper optimization was not active.

## Completion report

Report:

- production and outputV3 wall times;
- absolute and percentage improvement;
- exact-parity result for each table;
- critical checkpoint and parallel-wave timings;
- flags/configuration actually loaded;
- changes retained or reverted;
- remaining gap to the current target.
