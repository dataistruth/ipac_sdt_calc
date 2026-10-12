---
name: optimize-usp-final-effective-percentage
description: Optimizes uspGetFinalEffectivePercentage in two packagings — Development flat outputV2 A/B, Production inline in output/ — keeping concurrency, Checkpoint V2, CPBT, mode-preparation, effective-calculation, and three-table write optimizations with exact parity. Use when implementing, benchmarking, or diagnosing this SP.
---

# Optimize uspGetFinalEffectivePercentage

Use this skill only for:

`AllocationV2/usp_get_final_effective_percentage`

The production `output/` implementation is the correctness baseline.
Portable two-mode rules live in the parent
[optimize-spark-sp](../SKILL.md) skill. The **SP orchestrator** resolves
`ExecutionProfile` from `Common_V2.core.execution_profiles`;
`Common_V2.core.__init__` does not. Promoted defaults
(`CheckpointMode=4`, shuffle 32, `MaxThreads=4`) match profile `low`.

**Packaging (keep every optimization in this skill):**

| Mode | Where the optimizations live |
|---|---|
| **2 Development** | Flat `outputV2/`. Isolated orchestrator + pipeline. Optimized helper **copies** in the **same folder** (`cost_pct_loader.py`, `state_allocation.py`, `pfic_footnotes.py`, `effective_calc.py`, `entity_hierarchy.py`). Bind them onto the isolated production orchestrator. No `business/`, `tests/`, or `updated/`. A/B notebook + hashes. |
| **1 Production** | **Inline** the same optimizations in `output/` (orchestrator and existing helper modules). No `outputV2/`, no profiler, no reconcile. |

Historical log names `outputV3` / `outputV4` mean this locked candidate;
generate **outputV2** (Development) or edit **output/** (Production).

Never improve benchmark results by weakening business logic, validation,
output persistence, or exact result comparison.

Read [SP_OPTIMIZATION_GUIDE.md](SP_OPTIMIZATION_GUIDE.md) before making a
structural change or interpreting a benchmark.

Presentation:

- [uspGetFinalEffectivePercentage_Optimization_Review.pptx](uspGetFinalEffectivePercentage_Optimization_Review.pptx)
- Regenerate it with `scripts/generate_optimization_ppt.py` using
  `python-pptx`.

## Paths

- Production baseline / Production-mode target:
  `Source/AllocationV2/usp_get_final_effective_percentage/output/`
- Development candidate (flat):
  `Source/AllocationV2/usp_get_final_effective_percentage/outputV2/`
- Shared checkpoint implementation:
  `Source/Common_V2/core/checkpoint_V2.py`
- Benchmark:
  `Source/AllocationV2/usp_get_final_effective_percentage/outputV2/notebook/benchmark_final_effective_percentage.py`

## Non-negotiable contracts

1. Treat production `output` behavior as authoritative.
2. Preserve all three result tables:
   - `FinalEffectivePercentages`
   - `FNFinalEffectivePercentages`
   - `SM_FinalEffectivePercentages`
3. Require exact fingerprints, schemas, row counts, and values.
4. Preserve RunID-scoped writes and production error handling.
5. Keep mode 4 on production control flow.
6. Keep **all** optimizations listed in this skill. Development: they live
   in flat `outputV2/` (orchestrator/pipeline plus helper copies in that
   folder), gated by `_output_v3_*` flags whose production-module defaults
   stay off. Production mode: apply the same opts **inline** in `output/`.
7. Never use a runtime improvement from a failed or non-parity run.
8. Do not leave `__pycache__` or `.pyc` files in the repository.
9. Do not nest `outputV2/business/` or `outputV2/tests/`.

## Target configuration

Use these promoted defaults unless a new exact-parity benchmark disproves
them. Keep them in `_PROMOTED_EXPERIMENT_DEFAULTS` and the benchmark
`PROMOTED_OUTPUT_V3_KWARGS` block.

- `CheckpointMode=4`
- `SqlShufflePartitions=32`
- `MaxThreads=4`
- `ParallelGroups=all`
- CPBT input break: `both`
- missing-entity identity: enabled
- parallel dated/non-dated effective calculation: enabled
- broadcast `entity_partners`: enabled
- materialized effective inputs: enabled
- parallel CPBT post-tag: enabled
- compact all-entities / narrow anti-keys / transfer prefilter / drop
  tracking-match: enabled
- collapse state passes and batch state workflow lookup: enabled
- batch footnote line IDs: enabled
- footnote shared lineage: **off**
- footnote checkpoint partitions: `4`
- broadcast CPBT remaining entities: enabled
- hierarchy materialize: enabled
- parallel CPBT validate + min-quarter: enabled
- skip yearly empty probe: **off** (must stay off; it regresses
  `tcp_post_et_m0`)

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

### 2. Build or repair isolation (Development) or inline (Production)

**Development:** isolated wrapper in **flat** `outputV2/`. Copy
orchestration control flow. Import unchanged production helpers via
`parent.py`. Place optimized helper copies in the `outputV2/` root and
bind them in `orchestrator.py`.

Required modules (all in `outputV2/`, not in a subpackage):

- `orchestrator.py`: public API, bounded scheduler, timing, configuration
- `pipeline.py`: optimized mode 1/2/3 control flow
- `cfg_isolation.py`: safe branch-local configuration
- `checkpoint_policy.py`: named checkpoints and reporting
- `stages.py`: stage contracts
- `parent.py`: isolated production loading
- `plan_profiler.py`: optional plan metadata
- `output_reconcile.py`: snapshot / restore / fingerprints
- `cost_pct_loader.py`, `state_allocation.py`, `pfic_footnotes.py`,
  `effective_calc.py`, `entity_hierarchy.py`: optimized copies

**Production:** apply those same changes **inline** in
`output/orchestrator.py` and the live helper modules. No `parent.py`,
no profiler, no A/B notebook.

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

Syntax-check new or moved `outputV2` modules. Then run the Databricks
benchmark: production, then outputV2, exact comparison of all three
tables, checkpoint and wave timing review.

Reject the candidate if any table differs.

## Benchmark interpretation

Use wall-clock critical paths, not summed task duration.

Key evidence:

- overlapping tasks save only the shorter branch;
- helper construction time can hide eager checkpoint actions;
- deferred checkpoints move cost rather than remove it;
- stage totals may sum concurrent work and exceed wall time;
- output logs alone do not prove exact parity.

The locked reference run is 2026-09-28, RunID `17376`, EntityID `4137`,
modes `[1, 2, 3]`, CheckpointMode `4`, shuffle `32`, MaxThreads `4`,
experiment `baseline`:

- production `run_modes`: `155.7s`
- Development outputV2 wall: `51.808s` (`103.9s` / `66.7%` faster)
- gap to the 50s target: `1.808s`
- `yearly_lines.isEmpty` present (`SkipYearlyEmptyProbe` off)
- `tcp_post_et_m0`: `2.287s` (healthy; the 16s regression is not this run)
- CPBT helper: `12.866s`
- writes: all three tables

Critical checkpoints from that run:

- `tcp_post_tag_m0`: `4.273s`
- `fn_input_lines_m2`: `3.820s`
- `txfr_post_tag_m0`: `3.142s`
- `all_ent_pre_tag_m0`: `2.760s`
- `nde_pre_cpbt_m2`: `2.723s`
- `tcp_post_et_m0`: `2.287s`

Do not treat a slower later run as the new baseline. Earlier historical
walls (`57.736s`, `67.980s`, `68.688s`) are superseded by this lock.

## Known bad experiments

Do not promote these without a new isolated experiment:

- checkpoint mode 5 deferred materialization;
- eight shuffle partitions;
- broad lazy/action-lean checkpoint removal;
- broad fused mode-preparation lineage;
- candidate-claim CPBT rewrites;
- removing parent, tag, transfer, or dated fan-out barriers;
- `SkipYearlyEmptyProbe=True` (dropped `tcp_with_yearly_common`,
  `tcp_post_et_m0` ~2.5s to ~16s, wall ~68.7s);
- stacked experiments that prevent attribution.

## Deployment checks

If a new `_output_v3_*` flag appears enabled but its checkpoint name is absent:

1. confirm the helper copy in flat `outputV2/` (or inline `output/` in
   Production mode) was deployed;
2. purge both `output` and `outputV2` modules;
3. inspect the loaded helper signature;
4. rerun from a clean benchmark pass.

Absence of `fn_alloc_pfic_m2` or `all_ent_post_tag_m0` indicates that the
corresponding shared-helper optimization was not active.

## Notebook widgets

Frozen parent-skill Mode 2 set (10 widgets). FEP identity 4137 / 17376,
QA7 / `iPC_2025_QA7_15348`. Widget 10 is `ProfilePlan` (`off`/`on`).
No ExecutionOrder / MaxThreads / shuffle / CheckpointMode /
MissingEntityIdentity widgets. Hardcode missing-entity identity **on**.
A/B order is original then updated. Pass `ExecutionProfile` and
`ProfilePlan` only to outputV2.

## Completion report

Report:

- production and Development outputV2 wall times;
- absolute and percentage improvement;
- exact-parity result for each table;
- critical checkpoint and parallel-wave timings;
- flags/configuration actually loaded;
- changes retained or reverted;
- remaining gap to the current target.
