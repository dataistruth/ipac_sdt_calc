---
name: optimize-spark-sp
description: Creates an isolated Source/AllocationV2/<sp>/output/updated package for Databricks PySpark stored procedures. Never edits production output/*.py. Adds FEP-style plan profiling, checkpoint recommendations, ThreadPoolExecutor parallelism with max_threads=4, RunID pruning, bounded broadcasts, reuse-aware caching, and an old-vs-new parity notebook under output/updated/notebook/. Use when optimizing an AllocationV2 SP or asking to profile and benchmark a Spark orchestrator.
---

# Optimize Spark Stored Procedure

Create a reviewable, parity-tested optimization candidate. Production files under
`output/` stay unchanged. Every optimization lives in `output/updated/`.

Read [implementation-reference.md](implementation-reference.md) before editing.
Read [benchmark-reference.md](benchmark-reference.md) before creating the notebook.

## Repo layout

Work in this repo as a source-compatible AllocationV2 tree:

```text
ipac-sdt-calc/
└── Source/
    └── AllocationV2/
        ├── plan_profiler/                 # shared profiler (sync to workspace Source/)
        └── <sp_name>/
            └── output/
                ├── __init__.py            # package marker only
                ├── orchestrator.py        # PRODUCTION — never edit here
                ├── ai_*.py                # PRODUCTION — never edit here
                └── updated/               # ALL candidate changes
                    ├── __init__.py
                    ├── orchestrator.py    # copy of prod, then edit
                    ├── plan_profiler.py
                    ├── parent.py
                    ├── ...
                    └── notebook/
                        └── benchmark_<sp_name>.py
```

Python import root is `Source/` (`PYTHONPATH=Source` locally; Databricks
`source_path` widget points at workspace `.../Source`). Package names stay
`AllocationV2.<sp_name>.output` and `AllocationV2.<sp_name>.output.updated`.

This repo may omit production `ai_*.py` / orchestrator files (they live on the
monolith). Still treat `output/` as production: do not add optimized modules there.

## Required inputs

Resolve these from the request and source tree; ask only when they cannot be inferred:

- SP directory: `Source/AllocationV2/<sp_name>/`
- production orchestrator filename and public entry function
- output tables and their RunID partition column
- required run parameters
- Databricks `source_path` (workspace `.../Source`)

## Non-negotiable invariants

1. Never edit production files under `Source/AllocationV2/<sp>/output/` except
   creating an empty `__init__.py` if the package marker is missing.
2. Put every candidate change under `Source/AllocationV2/<sp>/output/updated/`.
3. Never create sibling `*_updated.py` files in `output/` (legacy anti-pattern:
   `load_allocation_input_updated.py`, `checkpoint_updated.py`, etc.).
4. Never put the A/B notebook at SP-root `notebooks/`. It belongs in
   `output/updated/notebook/`.
5. Preserve business logic, filters, join conditions, schemas and write semantics.
6. Do not parallelize dependent stages, shared mutable temp-view creation, conflicting
   writes, gating validations, or operations whose ordering changes results.
7. Do not add a checkpoint solely because node count is high. Recommend it only after
   combining plan growth, depth, downstream reuse and measured action time.
8. Never report an optimization as successful until output fingerprints match.
9. Support classic PySpark and Spark Connect by duck-typing DataFrames; do not rely on
   `isinstance(obj, pyspark.sql.DataFrame)`.
10. Updated orchestrators import checkpointing from
    `Common_V2.core.checkpoint_V2` only (`checkpoint_V2` / alias `checkpoint`,
    `initialize_checkpoint_V2`, `normalize_checkpoint_mode`). Do **not** create
    `output/updated/checkpoint.py` and do **not** import
    `Common_V2.core.checkpoint` (production stats-on writer) from updated code.
    Call `initialize_checkpoint_V2(cfg, CheckpointMode)` once at start; default
    mode is **2**. Modes: 1=all stats-off Delta; 2=odd localCheckpoint / even
    stats-off Delta; 3=odd localCheckpoint / even uncompressed Volume Parquet;
    4=all localCheckpoint. Pass `CheckpointMode` from the notebook/orchestrator.
    LocalCheckpoint failures fall back to stats-off Delta. Call
    `track_checkpoint_plan` via V2 (already hooked when `profile_plan` is on).
11. Do not rename public helper symbols the orchestrator already imports. After
    rewriting a helper, grep the copied orchestrator for `from .X import` and export
    every name in that list. Allocation Input required names:
    - `shared_views.register_shared_views_parallel`
    - `validation_parallel.run_validations_parallel` (accept `workers=`)
    - `finalize_parallel.collect_results_parallel` (accept `workers=`)
12. Do not drop checkpoint Delta tables or Volume paths on the SP hot path.
    Unique UUID (or sequence) names make the next run collision-free.
    `drop_checkpoints` / `drop_checkpoints_V2` stay exported for optional
    debug cleanup; default is skip. Catalog/volume leftovers are removed by a
    shared end-of-day sweeper (`_tmp_%`, `_tmp_v2_%`, `volume/_checkpoints/`
    older than a safety window). `localCheckpoint` needs no drop. Never drop
    mid-run. Prefer `finally` only when an explicit opt-in flag is set.
13. Never call `coalesce()` or `repartition()` solely to narrow a Delta
    checkpoint write. Large checkpoint plans must retain available write
    parallelism. Tune `spark.sql.shuffle.partitions` for the run instead; do
    not add `CheckpointCoalesce` / `checkpoint_coalesce` controls.

## Output package

Create only this tree (file names may match the SP's orchestrator, e.g.
`load_allocation_input.py` instead of `orchestrator.py`):

```text
Source/AllocationV2/<sp_name>/output/updated/
├── __init__.py
├── orchestrator.py              # or the SP's real entry module name
├── parent.py                    # import unchanged prod services from output/
├── plan_profiler.py             # shim → AllocationV2.plan_profiler
├── parallel_helpers.py          # only when parallel work exists
├── run_id_pruning.py            # only when run-scoped reads exist
├── output_reconcile.py
├── OPTIMIZATION_REPORT.md
└── notebook/
    └── benchmark_<sp_name>.py
```

Copy the production orchestrator into `output/updated/`, then modify only that copy.
Use relative imports for updated helpers. Import unchanged production services from
the parent `output` package via `parent.py` / `output_module()`.

Export the same public entry function from `output/updated/__init__.py`.

## Workflow

### 1. Establish the baseline

- Read the complete production orchestrator and every service it calls.
- Map stages, DataFrame dependencies, actions, temp views, shared `cfg` mutation,
  output writes and existing checkpoints.
- Record the original function signature and return contract.
- Identify the output tables needed for parity.

Write the dependency map and initial hypotheses to `OPTIMIZATION_REPORT.md`.

### 2. Add FEP-style plan profiling

Use the shared `AllocationV2.plan_profiler` implementation when available. Add a local
`plan_profiler.py` shim with safe no-op fallbacks so profiling can never break the SP.
If the shared package is absent, create it once from the FEP profiler contract in the
implementation reference; do not duplicate its core logic separately in every SP.

Instrument every DataFrame-producing builder with `track_plan`. Activate separate
builder and checkpoint sinks for one invocation and emit both reports:

```python
plan_profile_report(builder_records, threshold, label="BUILDER")
plan_profile_report(checkpoint_records, threshold, label="CHECKPOINT")
plan_profile_report(action_records, threshold, label="ACTION")
```

The report prints every row with a recommendation:

```text
function  nodes=N  depth=D  (+delta)  ...  <-- add|measure|collapse
checkpoint  nodes=N  depth=D  (+delta)  ...  <-- keep|measure|remove
```

- BUILDER ``add``: ``delta >= threshold`` (consider a new break).
- BUILDER ``measure``: plan already large/deep, this step added little.
- BUILDER ``collapse``: low growth — do not add a checkpoint here.
- CHECKPOINT ``keep`` / ``measure`` / ``remove``: size of the plan the existing
  break truncates. ``remove`` is a collapse candidate for that seam.
- ACTION ``add``: an explicit Spark action consumes a plan at/above the threshold
  or the same named action repeats. ACTION ``measure``: an action exists but plan
  size/reuse evidence is insufficient.

This is a recommendation only. Do not insert or drop checkpoints from the
flag alone. Put the evidence-based decision in ``OPTIMIZATION_REPORT.md``.

Wrap explicit `.count()`, `.isEmpty()`, `.collect()`, `.first()`, `.take()`,
`.toPandas()`, writes, and result-storer calls with `profile_action`. It records
the input plan before execution and action wall time. Do not globally monkeypatch
Spark/DataFrame to discover actions.

Measure checkpoint input plans before materialization with `track_checkpoint_plan`.
Keep profiling opt-in via `profile_plan=False` and
`plan_checkpoint_threshold=30`.

Never wrap Common_V2 production `checkpoint`. Import V2 in the updated
orchestrator (`from Common_V2.core.checkpoint_V2 import checkpoint_V2 as checkpoint`).

### 3. Produce checkpoint recommendations

Rank candidates using evidence:

- builder `delta`: new nodes added versus the largest DataFrame input
- total nodes and depth
- expensive operators, especially joins, aggregates, unions and windows
- number of downstream consumers / fan-out
- measured action or checkpoint wall time
- Delta write/commit overhead

Classify each candidate:

- **Keep/add**: high plan growth plus repeated downstream evaluation or fan-out, with
  expected recomputation greater than materialization cost.
- **Measure**: large/deep plan but single consumer or unknown action cost.
- **Collapse/avoid**: low-value single-consumer seam or materialization cost exceeds
  saved recomputation.

Do not automatically implement recommended checkpoints unless the user asks. Put the
ranked, evidence-based recommendation in `OPTIMIZATION_REPORT.md`.

### 4. Add safe thread-pool parallelism

Add named entry parameters:

```python
max_threads: int = 4,
MaxThreads: int | None = None,
```

Normalize to `1..8`; PascalCase overrides only when supplied. Use the resulting value
for all eligible pools, with stage-specific worker counts capped by task count.

Parallelize only independent, latency-bound groups such as:

- unrelated initial table/config loads
- non-gating validation warnings
- independent result builders using isolated result dictionaries
- writes to distinct output tables/partitions

Keep sequential:

- gating checks and early-abort logic
- stages that consume prior-stage output
- concurrent writes to the same Delta table/partition
- functions mutating the same `cfg` keys without isolated copies and deterministic merge
- temp-view registration with cross-view dependencies

Use `ThreadPoolExecutor`, map each future to a stable task name, re-raise failures, and
log per-task duration plus pool wall time. See the implementation reference.

### 5. Apply RunID pruning, bounded broadcasts and caching

- Push `RunID == cfg["run_id"]` into reads before joins.
- For lower-tier flowups, derive the allowed lower-tier RunIDs and use a broadcast
  left-semi join before expensive joins.
- Also push ClientID and TaxPeriodID filters when those columns exist.
- Broadcast only proven-small lookup/key DataFrames; never broadcast an unbounded fact.
- Reuse a DataFrame through `cfg` or a local variable when it is referenced repeatedly.
- Call `.persist()`/`.cache()` only when the same expensive DataFrame feeds multiple
  actions; materialize deliberately and always `unpersist()` in `finally`.

Document each prune, broadcast and cache with its consumer count and safety reason.

### 6. Create the A/B benchmark notebook

Create `Source/AllocationV2/<sp_name>/output/updated/notebook/benchmark_<sp_name>.py`
as a Databricks source notebook.
It must run production and updated implementations against identical inputs, purge the
RunID output before each variant, capture timing, and compare order-independent output
fingerprints.

Include widgets for source path, run count, execution order, SP parameters, catalog,
schema, result type, volume path when needed, `MaxThreads` (default `4`),
`ProfilePlan` (default `off`), `PlanCheckpointThreshold` (default `30`),
`CheckpointMode` (default `2`; values `1|2|3|4`), and `SqlShufflePartitions`.

Evict the SP package and `AllocationV2.plan_profiler` from `sys.modules` before fresh
imports so workspace syncs are not hidden by Python module caching.

### 7. Verify

- Syntax-check every new Python file.
- Confirm the updated orchestrator imports
  `Common_V2.core.checkpoint_V2` (not `Common_V2.core.checkpoint` and not
  `output/updated/checkpoint.py`). Confirm `initialize_checkpoint_V2` runs once
  with default mode 2. Confirm `drop_checkpoints_V2` is not on the hot path.
- Confirm no new files under `output/` except `__init__.py` and `updated/`.
- Search for unresolved imports and accidental production-file modifications.
- Run the original and updated variants in both orders when practical.
- Require row count, schema, numeric aggregates and row fingerprint parity.
- Report original wall time, updated wall time, percent change, profiler rankings,
  checkpoint recommendation and any remaining uncertainty.

If parity fails, stop optimization work and diagnose the first mismatching output.

## Completion summary

State:

- files created or changed
- stages parallelized and why they are independent
- RunID pruning, broadcast and caching applied
- profiler controls and report format
- checkpoint recommendations (not just node counts)
- benchmark/parity status and exact command/notebook to run

## Examples

### Optimize an SP

User: “Use optimize-spark-sp on `usp_example`.”

Result: Creates `Source/AllocationV2/usp_example/output/updated/`, instruments the
copied orchestrator, applies only safe proven optimizations, and creates
`output/updated/notebook/benchmark_usp_example.py`.

### Profile without adding checkpoints

User: “Profile this SP and recommend checkpoint candidates.”

Result: Adds opt-in builder/checkpoint profiling, runs or prepares the benchmark, and
records keep/measure/collapse recommendations without inserting speculative breaks.

### Parallelism-only pass

User: “Keep only thread pools, RunID pruning, cache and broadcasts.”

Result: Removes unrelated candidate optimizations while retaining parity-safe parallel
groups and evidence-backed read optimizations.
