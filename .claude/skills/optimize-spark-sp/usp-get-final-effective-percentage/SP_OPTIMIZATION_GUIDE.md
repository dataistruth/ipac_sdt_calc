# uspGetFinalEffectivePercentage: Architecture and Optimization Guide

This document explains the current Spark implementation of
`uspGetFinalEffectivePercentage`, the performance behavior observed in
Databricks, and the outputV3 changes used to reduce runtime while preserving
the production stored-procedure contract.

The document is intentionally divided into ten page-sized chapters. Pages
1–5 introduce the SP and its current execution model. Pages 6–10 explain the
optimization work, benchmark history, validation model, and next steps.

---

## Page 1 — Purpose, scope, and business contract

`uspGetFinalEffectivePercentage` calculates final ownership or allocation
percentages for multiple allocation modes. Its PySpark implementation mirrors
a large SQL Server stored procedure with temporary tables, ordered matching
rules, transfer adjustments, dated and non-dated calculations, footnote
handling, state allocations, validations, and final persistence.

The implementation lives under:

`Source/AllocationV2/usp_get_final_effective_percentage`

The `output` package is the production baseline. The `outputV3` package is an
optimized control-flow layer. Production remains the authority for business
semantics. outputV3 may schedule independent work concurrently, materialize
lineage at different safe boundaries, reuse common intermediates, or use
equivalent relational expressions, but it must not change the resulting
tables.

The SP produces three primary tables for modes 1–3:

1. `FinalEffectivePercentages`
2. `FNFinalEffectivePercentages`
3. `SM_FinalEffectivePercentages`

The tables represent different business paths:

- standard or look-through effective percentages;
- footnote-related effective percentages;
- state-mode effective percentages.

The exact meaning of every row is determined by entity, partner, quarter,
allocation type, tracking key, tag, line type, transfer behavior, and mode.
Many of those columns are nullable or use empty-string sentinel values.
Consequently, apparently simple join rewrites can alter SQL-compatible null or
duplicate behavior. Exact parity must therefore compare complete outputs, not
only row counts.

The procedure supports modes 1, 2, 3, and 4. outputV3 optimizes the fused
mode-1/2/3 path. Mode 4 retains the production path because its 704(c) flow can
mutate catalog metadata and does not have independent sibling branches that
justify parallel execution.

The public API remains:

- `run_final_effective_percentages`
- `run_mode`
- `run_modes`
- `get_last_run_profile`

The optimized implementation also preserves the production save contract,
RunID filtering, error handling, allocation-log updates, and output schemas.
A run is successful only when computation, persistence, and exact comparison
all succeed.

The optimization target has been sub-50-second wall time for the measured
RunID and cluster. This is a performance objective, not a reason to weaken
correctness. The latest recorded outputV3 execution was `57.736s`, compared
with `169.1s` for production in the same benchmark pass.

<div style="page-break-after: always;"></div>

## Page 2 — Production pipeline and stage topology

The production implementation is a multi-stage dataflow. It creates many
DataFrames lazily, then materializes them through checkpoints, emptiness
probes, validations, and output writes. Understanding which operation is a
builder and which operation is a Spark action is essential.

The first major stage loads common configuration and dimensions. Important
relations include:

- cost-percentage snapshots;
- entity partners;
- cost underlying types;
- entity hierarchy;
- asset-class relationships;
- combined and ordered underlyings;
- allocation rules;
- line items;
- book-effective data;
- quarter and yearly reference data;
- look-through allocation inputs;
- footnote lines.

The second stage builds two common chains:

- the with-look-through chain used by mode 1;
- the without-look-through chain used by modes 2 and 3.

Each chain builds all-underlyings, input lines, amount-based allocations,
dated entities, non-dated entities, and entity-underlying relationships. The
without-look-through chain can correctly produce zero entity-underlying rows
when its input lines are empty.

Mode preparation then adds mode-specific data:

- mode 1 primarily uses the look-through chain;
- mode 2 adds footnote underlyings, footnote input lines, and footnote dated
  or non-dated entities;
- mode 3 adds state allocation input, state entities, and state amounts.

After preparation, the mode-specific frames are tagged with `_mode` and
unioned. The `_mode` column is a critical isolation mechanism. Every fused
join, grouping, anti-join, ranking operation, and output filter must include
the mode where cross-mode leakage is possible.

The fused cost-percentage-by-type stage, abbreviated CPBT, applies priority
matching:

1. underlying-only matches;
2. entity-total matches;
3. asset-class matches;
4. parent hierarchy matches with `TrackingKeyMatch`;
5. parent hierarchy matches without `TrackingKeyMatch`;
6. tag matches;
7. empty tracking-key and tag fallback matches.

It produces an updated temporary cost-percentage relation and adjusted
transfer percentages. Missing-entity handling and minimum-quarter calculation
follow.

The effective stage computes dated and non-dated percentages. Dated
calculation is more expensive because it handles transfer dates, pickup order,
excluded-from-transfer rows, missing partners, and yearly prorata fallback.
Plugging and type-ID updates are applied after both branches.

Finally, each mode builds its output, and the three result tables are written.
The production procedure logs status and updates allocation-run metadata.

outputV3 maps this topology into eight named stages:

1. `common_reads`
2. `with_lt_branch`
3. `no_lt_branch`
4. `mode_prep`
5. `fused_cpbt`
6. `fused_effective`
7. `output_build`
8. `output_write`

These names are contracts used by timing, checkpoint reporting, and benchmark
analysis.

<div style="page-break-after: always;"></div>

## Page 3 — Spark execution behavior and the original bottlenecks

Spark DataFrames are lazy. A helper returning in milliseconds may still
construct a plan that costs several seconds when a later checkpoint or write
executes it. For this SP, elapsed helper time, checkpoint time, and wall-clock
critical path must be interpreted together.

The production SQL conversion initially preserved many temporary-table
boundaries as durable Delta checkpoints. This protected correctness and
controlled plan growth, but it also created many small Spark jobs. On the
measured data set, scheduler overhead, repeated plan evaluation, shuffles, and
storage actions can cost more than the row-level transformations.

The main original bottlenecks were:

- repeated common reads for each mode;
- serial with-LT and no-LT chains;
- serial mode 1, 2, and 3 preparation;
- CPBT executed separately or with repeated upstream lineage;
- dated and non-dated effective calculations executed serially;
- serial output construction and persistence;
- overly durable or poorly placed lineage barriers;
- repeated actions such as `isEmpty` and `first`;
- large plans duplicated through unions and anti-joins.

The SP also contains intentional fan-out points. Removing a checkpoint at a
fan-out can make a downstream action replay an entire multi-join lineage two,
three, or four times. This is why checkpoint count alone is not a useful
optimization metric. A two-second checkpoint can save ten seconds of
recomputation.

The measured workload uses a relatively small final result—historically 79
rows across the three outputs—but it reaches that result through complex
matching. Small output size does not imply a cheap plan. Parent ranking,
distinct operations, transfer matching, and footnote expansion can process
substantially larger intermediate relations.

The CPBT helper illustrates the distinction between helper and deferred
cost. In a representative run:

- the CPBT helper took about `12.4s`;
- `tcp_by_type_fused` took about `0.6s`;
- `txfr_adj_fused` took about `3.5s`.

The transfer output is slower because transfer lineage intentionally skips
some mid-helper barriers and is materialized at the final transfer boundary.

Mode 2 is dominated by footnote work. `build_footnote_input_lines` contains
six passes over allocation input, repeated PFIC line-item joins, repeated
book-effective joins, and a distinct operation per pass. Mode 3 is dominated
by constructing and materializing shared state lines.

Before the effective-input optimization, dated effective calculation took
about `11.7s`. The same minimum-quarter and filtered-dated plans were consumed
through multiple joins and by both parallel effective branches. Materializing
those inputs once reduced the dated helper to `2.792s`, with an additional
`1.221s` for the concurrent input barriers.

<div style="page-break-after: always;"></div>

## Page 4 — Correctness hazards and state management

The most important optimization constraint is exact semantic parity.
Several features make this SP unusually sensitive to rewrites.

### Mode isolation

Modes 1, 2, and 3 are fused only after each mode-specific relation is tagged
with `_mode`. A join missing `_mode` can allow a row produced for one mode to
satisfy another mode’s match. Rankings must partition by mode, and anti-joins
must include mode in their keys.

### Empty strings and nulls

Tracking keys and tags often use empty strings as business values. SQL-style
expressions use `coalesce`, sentinel `-1`, and conditional matching. Replacing
those expressions with generic equality or dropping null-preserving behavior
can change results.

### Duplicate-row semantics

Some production expressions intentionally or historically preserve
duplicates. For example, the dated pickup-order rewrite emits non-pickup-3
rows once and certain unmatched pickup-3 rows twice. The outputV3
single-anti-join implementation must reproduce that multiset using controlled
row expansion. A simple deduplication would not be equivalent.

### Aliases and qualifiers

Production helpers contain expressions such as `D.InvestmentID` and
`P.Quarter`. A checkpoint can reset logical-plan qualifiers. outputV3 returns
local checkpoints through `toDF(*columns)` to match the qualifier reset of a
fresh table relation and avoid ambiguous or stale aliases.

### Mutable configuration

The production `cfg` dictionary stores both immutable configuration and
run-time artifacts. Parallel branches cannot safely mutate one shared
dictionary. outputV3 creates branch-local shallow forks:

- DataFrames and immutable values are shared;
- ordinary mutable containers are copied;
- checkpoint coordination and activity collections are shared through
  thread-safe wrappers;
- mode, current mode, input-empty flags, and branch artifacts stay private.

The `_part_v_quarters_df` artifact is merged after mode preparation using
production mode ordering and schema validation.

### Writes and failures

Parallel output writes target distinct tables, but all futures are observed.
One failed write fails the complete SP call. A failed branch prevents fused
stages from starting. Durable temporary objects for failed runs are cleaned
up, while successful artifacts cannot be dropped prematurely because returned
DataFrames may still reference them.

### Loaded-module compatibility

Databricks interpreters can retain older helper functions. A newer outputV3
pipeline once passed `checkpoint_fn` to an older loaded
`build_footnote_input_lines`, causing:

`TypeError: unexpected keyword argument 'checkpoint_fn'`

The pipeline now inspects the helper signature before passing optional
keywords. This prevents failure, but it also means the optimization is skipped
when the old helper is loaded. Benchmark logs must therefore confirm expected
checkpoint names, not merely configuration flags.

<div style="page-break-after: always;"></div>

## Page 5 — Benchmark, profiling, and acceptance model

The benchmark notebook runs production and outputV3 side by side. It snapshots
the existing output partitions, executes both variants, captures their
results, compares them, and restores data in a `finally` path.

The benchmark records:

- wall-clock time;
- procedure-reported time;
- result row counts;
- complete output fingerprints;
- requested and effective Spark configuration;
- stage timings;
- helper, task, action, checkpoint, and write events;
- checkpoint materialization mode;
- parallel waves and their critical duration;
- optional logical-plan node count, depth, and partition count.

The comparison must cover all three output tables. A cost-percentage sum
validation passing inside the pipeline is not equivalent to exact output
parity. Likewise, 79 rows on both sides do not prove that the rows contain the
same values.

Wall time is the primary performance metric. Summed task or stage duration can
double-count concurrent work. If dated takes 12 seconds and non-dated takes 3
seconds in parallel, the effective wave is approximately 12 seconds, not 15.

The benchmark’s critical-action list is particularly useful. In the
`57.736s` run, important checkpoint durations included:

- `state_lines_m3`: `4.170s`
- `fn_input_lines_m2`: `3.982s`
- `tcp_post_tag_m0`: `3.812s`
- `txfr_adj_fused`: `3.496s`
- `nde_pre_cpbt_m2`: `2.604s`
- `tcp_post_et_m0`: `2.296s`
- `all_ent_pre_tag_m0`: `2.253s`
- `uc_ordered_common`: `2.243s`
- `input_lines_lt`: `2.163s`
- `cost_pct_m123`: `2.071s`

The same run’s major parallel-wave critical paths were:

- LT/no-LT branches: `5.230s`
- mode preparation: `10.981s`
- mode-2 boundaries: `2.604s`
- CPBT inputs: `0.591s`
- CPBT outputs: `3.498s`
- effective inputs: `1.221s`
- dated/non-dated effective: `3.170s`
- plugging boundaries: `0.745s`
- output construction: `1.151s`
- output writes: `2.141s`

An optimization is promoted only if:

1. the run succeeds;
2. all three exact comparisons pass;
3. the improvement is repeatable;
4. the gain is visible on the critical path;
5. no cost was merely shifted into a later unmeasured action;
6. production behavior remains unchanged when outputV3 flags are absent.

One-at-a-time experiments are preferred. Stacking unrelated changes may
produce a faster result without identifying which change helped or which one
introduced risk.

<div style="page-break-after: always;"></div>

## Page 6 — outputV3 isolation, scheduling, and common-stage changes

outputV3 executes the production orchestrator source in a private module
namespace and substitutes an optimized `run_modes` control flow for modes
1–3. This minimizes business-code duplication while allowing explicit
scheduling and checkpoints.

A bounded `ThreadPoolExecutor` uses at most four workers. Parallel groups are
named and configurable. The promoted set includes:

- `common_dimensions`
- `common_inputs`
- `lookthrough_metadata`
- `lt_nolt_branches`
- `mode_prep`
- `mode_prep_boundaries`
- `cpbt_boundaries`
- `effective_inputs`
- `fused_effective`
- `effective_boundaries`
- `output_build`
- `output_writes`

Independent common dimensions begin together. Cost snapshot construction,
entity partners, and asset-class relationships do not need to wait for one
another. Common input readers and look-through metadata readers follow the
same pattern.

The cost snapshot is materialized as `cost_pct_m123`. This relation is reused
widely and is worth the approximately two-second eager checkpoint. The entity
hierarchy remains a serial dependency because it consumes cost underlying
types.

The earlier implementation materialized `underlyings_common`, then shortly
afterward materialized `uc_ordered_common`. `underlyings_common` had only one
consumer, so removing that intermediate action retained the final safe
boundary while avoiding a redundant job.

The with-LT and no-LT chains execute concurrently after their shared
dependencies are ready. In the latest run, their combined wave took `5.230s`;
running them serially would have cost the sum of both branches.

Checkpoint mode experiments established that mode 4—eager local checkpoint
for every named seam—performed better than deferred mode 5. Deferred
materialization did not remove work; it moved it into later actions where
larger plans were replayed. Eight shuffle partitions also underutilized the
cluster. The promoted baseline is:

- checkpoint mode 4;
- 32 shuffle partitions;
- AQE enabled;
- 128 MB advisory partition size.

The orchestration layer emits live START and DONE events through the production
logger. This matters in Databricks, where stdout from imported modules or
worker threads can be omitted. Events include operation kind, stage, thread,
status, and elapsed time.

These common-stage changes are structural rather than business-specific. They
preserve relation contents and alter only when independent work begins or
where an equivalent lineage is materialized.

<div style="page-break-after: always;"></div>

## Page 7 — Mode 2, Mode 3, and preparation optimizations

Mode preparation originally ran serially. outputV3 prepares modes 1, 2, and 3
concurrently using isolated configuration forks.

Mode 1 is usually the shortest preparation branch. Mode 2 performs footnote
processing, and mode 3 performs state processing. Their internal work must be
optimized without exposing long upstream lineage to CPBT.

### Mode 3

Both state entity branches consume the same multi-pass state-allocation input.
Materializing `state_lines_m3` prevents dated and non-dated branches from
rebuilding that input independently. Although the checkpoint itself measured
`4.170s` in the latest run, earlier tests without the seam increased total
mode-3 preparation. It is a fan-out barrier and should remain.

### Mode 2

Mode 2 retains:

- `all_und_final_m2`;
- `fn_input_lines_m2`;
- dated, non-dated, and transfer pre-CPBT boundaries.

The footnote input helper performs six semantic passes. Five share the same
allocation-input to PFIC-line-item join. The optimized helper can build a
single uniquely named projection containing all allocation columns plus PFIC
line description, materialize it as `fn_alloc_pfic_m2`, and reuse it across
passes 2–6. Each pass keeps its own `distinct`, because replacing all of them
with one final distinct could alter duplicate behavior across passes.

Six scalar footnote line-ID lookups are also batched into one Spark action.
Each branch retains `limit(1)`, preserving the original `first()` behavior.

The outputV3 pipeline checks whether the loaded helper accepts
`checkpoint_fn`. If it does, the shared PFIC seam is enabled. If not, the
helper is called using the old signature so the run remains compatible.

The latest `57.736s` log did not show `fn_alloc_pfic_m2`, while
`fn_input_lines_m2` still cost `3.982s`. This is evidence that the shared
helper optimization was not loaded in that Databricks interpreter. The source
file and module cache must be synchronized before attributing any gain to this
change.

### Preparation boundaries

After mode-specific transformation, dated, non-dated, and transfer relations
are independent. outputV3 materializes them concurrently. In the latest mode-2
wave, the critical boundary was `nde_pre_cpbt_m2` at `2.604s`; the wave costs
the maximum task duration instead of the sum.

The total mode-preparation wave was `10.981s`. This remains one of the largest
opportunities after dated effective was reduced.

<div style="page-break-after: always;"></div>

## Page 8 — CPBT fusion and lineage optimization

CPBT is executed once on mode-tagged unions instead of independently rebuilding
equivalent matching structures for each mode. `_mode` is included in every
required join, anti-join, window partition, and output projection.

Both dated and non-dated CPBT inputs are materialized concurrently. This keeps
mode-preparation lineage out of the already complex CPBT plan.

The helper retains five proven internal barriers:

- `tcp_post_et_m0`
- `all_ent_m0`
- `parent_ord_m0`
- `all_ent_pre_tag_m0`
- `tcp_post_tag_m0`

`parent_ord_m0` protects a dense-rank hierarchy result used by four matching
paths. `tcp_post_tag_m0` protects the final temporary cost relation before
nothing-match processing and downstream consumers.

Several exact-equivalent outputV3-gated reductions were added:

1. Dated and non-dated entity keys are unioned once. An array/explode
   expression emits the original TypeID and, when different, the cost
   allocation TypeID. One final distinct replaces four scans and redundant
   per-branch distincts.
2. Anti-joins project only `DealId`, `TypeId`, `TrackingKey`, `Tag`, and
   `_mode` from the right side. Extra partner, quarter, percentage, and 704(c)
   columns cannot affect anti-join existence.
3. Transfer parent-match inputs are prefiltered using predicates already
   required by the joins: empty tracking key and empty/non-empty
   `TrackingKeyMatch`.
4. `TrackingKeyMatch` is dropped after its last cleanup use, narrowing later
   tag and fallback lineage.
5. A post-tag all-entities seam can materialize the shared anti-join before
   temp and transfer nothing-match branches.

After the helper, temp and transfer outputs materialize concurrently. Missing
dated, missing non-dated, and final cost also materialize concurrently.
`entity_partners` is broadcast into final-cost construction.

The latest run still measured:

- CPBT helper: `12.421s`
- transfer output: `3.498s`
- temp output: approximately `0.656s`

It also did not show `all_ent_post_tag_m0`. This demonstrates that the shared
`cost_pct_loader.py` changes were not active in that run. The configuration
alone is insufficient evidence; checkpoint names and helper timings confirm
loaded behavior.

Broad checkpoint removal is not a substitute. Earlier action-lean and lazy
experiments exposed deep multi-consumer plans and produced runtimes between
approximately 174 and 254 seconds. CPBT optimization must narrow actual work
while retaining fan-out boundaries.

<div style="page-break-after: always;"></div>

## Page 9 — Effective calculation, output construction, and measured gains

The most successful recent change was materializing effective inputs after
minimum-quarter calculation.

`compute_minimum_quarter` returns:

- a minimum-quarter relation;
- a cost-percentage minimum-quarter relation;
- filtered dated entities.

Previously, those lazy plans were consumed repeatedly inside dated effective
calculation and across the dated/non-dated parallel branches. outputV3 now
materializes:

- `de_post_minq_fused`;
- `cost_pct_min_q_fused`.

They run concurrently in the `effective_inputs` group. In the latest
benchmark, that wave cost `1.221s`.

The dated and non-dated calculations then run concurrently. Dated retains its
internal barriers because each protects repeated downstream consumers. The
dated pickup rewrite was changed to execute one anti-join for pickup-3 rows
and use an explode operation to reproduce the production-required duplicate
multiplicity.

Measured improvement:

- before effective-input materialization, dated helper: about `11.713s`;
- after materialization, dated helper: `2.792s`;
- outer dated task after optimization: `3.170s`;
- non-dated task: `1.275s`;
- effective-input wave: `1.221s`.

The combined effective critical path fell from roughly 12 seconds to roughly
4.4 seconds including input barriers. This produced most of the improvement
from `64.412s` to `57.736s`.

Plugging outputs are independent after `apply_plugging`, so dated and
non-dated plugged relations materialize concurrently. Per-mode final outputs
also build concurrently.

The three result writes target different tables and execute concurrently. The
latest output-write wave took `2.141s`, even though summed write-related stage
time was higher. This difference illustrates why wave critical time is the
correct wall-clock measure.

Runtime progression during the optimization effort included:

- production: approximately 154–169 seconds, depending on pass conditions;
- early outputV3: approximately 74–75 seconds;
- mode-5/shuffle-8 regression: `86.55s`;
- restored mode-4/shuffle-32 path: `71.185s`;
- shared-state and CPBT/effective changes: `67.980s`;
- boundary parallelization: `64.412s`;
- effective-input materialization and computation reductions: `57.736s`.

From `67.980s` to `57.736s`, the total improvement is `10.244s`. Relative to
the latest same-run production time of `169.1s`, outputV3 was `111.364s`
faster, or approximately 65.9 percent faster.

<div style="page-break-after: always;"></div>

## Page 10 — Deployment, remaining work, and operating procedure

The remaining gap to the 50-second objective is `7.736s` based on the latest
run. The next work should be evidence-driven.

### First: activate already implemented shared-helper changes

Before creating another algorithm, deploy and reload:

- `output/pfic_footnotes.py`;
- `output/cost_pct_loader.py`;
- `output/effective_calc.py`;
- all outputV3 modules.

The benchmark deletes loaded `output`, `outputV3`, and `Common_V2` modules
before importing, but that cannot load source that was not synchronized to the
Databricks workspace or cluster.

Confirm activation through checkpoint names:

- `fn_alloc_pfic_m2` for shared PFIC input;
- `all_ent_post_tag_m0` for post-tag CPBT entity reuse;
- `de_post_minq_fused` and `cost_pct_min_q_fused` for effective inputs.

The latest run showed only the effective-input pair. Therefore, the measured
`57.736s` does not yet include all implemented shared-helper optimizations.

### Second: measure the remaining critical path

After a clean exact-parity run, inspect:

- `state_lines_m3`;
- `fn_input_lines_m2`;
- `tcp_post_tag_m0`;
- `txfr_adj_fused`;
- `nde_pre_cpbt_m2`;
- the with-LT branch;
- output save overhead outside the write wave.

Do not select a target from summed stage time. Use checkpoint timing and
parallel-wave critical paths.

### Third: retain proven safeguards

Do not remove:

- the mode-3 state seam;
- mode-2 final-underlying and input-line seams;
- CPBT parent and tag fan-out seams;
- dated post-transfer, pickup, step-5, pre-yearly, or step-6 seams;
- final fused temp, transfer, dated, or non-dated boundaries.

Do not restore:

- deferred checkpoint mode 5;
- shuffle partition count 8;
- broad lazy checkpointing;
- candidate-claim ranking;
- unmeasured stacked experiments.

### Operating checklist

For every candidate:

1. synchronize source;
2. clear loaded modules;
3. run production;
4. run outputV3;
5. verify all three exact fingerprints;
6. inspect checkpoint names to prove flags were active;
7. compare wall time and critical waves;
8. retain or revert the candidate;
9. run structure tests and `git diff --check`;
10. remove generated Python cache files.

### Final design principle

This SP is fastest when expensive shared lineage is materialized once, truly
independent work overlaps, and single-consumer plans remain lazy until an
existing boundary. It is slow when checkpoints are removed mechanically or
when deferred work is mistaken for eliminated work.

The optimization strategy is therefore:

- preserve the production relational contract;
- isolate modes with `_mode`;
- materialize fan-out, not every builder;
- parallelize only dependency-independent actions;
- narrow joins and remove repeated computation;
- prove loaded behavior through logs;
- require exact parity before accepting speed.

That procedure is now encoded in the accompanying SP-specific skill and can be
reapplied whenever the production baseline changes.
