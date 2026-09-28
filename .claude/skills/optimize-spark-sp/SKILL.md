---
name: optimize-spark-sp
description: Optimizes Databricks PySpark AllocationV2 stored procedures in two modes. Production (mode 1) applies inline changes in output/ with a run notebook only—no plan profiler, no updated/, no business/. Development (mode 2) creates outputV2 with a side-by-side A/B notebook, optional plan profiler slim shim, and per-table validation hashes. Both modes use Checkpoint V2 and the same business logic. Use when optimizing an AllocationV2 SP, adding parallel thread phasing, or preparing an SP for production output or outputV2.
---

# Optimize Spark Stored Procedure

Read [implementation-reference.md](implementation-reference.md) before
editing. Read [benchmark-reference.md](benchmark-reference.md) before
creating a notebook.

The **pipeline logic is the same in both modes**: Checkpoint V2, parallel
thread phasing, isolated `cfg`, RunID pruning, bounded broadcasts, and
preserved join/filter/write semantics. Only packaging and notebooks differ.

## Execution profile at orchestrator start

The **SP orchestrator** (not `Common_V2.core.__init__`, not the notebook)
imports the tier map and applies it once at the start of `run_*`, after
`cfg` exists and **before** checkpoint init and before worker threads
copy `cfg`. Default tier is **`low`**.

```python
from Common_V2.core.execution_profiles import resolve_execution_profile

# low    shuffle 32  CheckpointMode 4  MaxThreads 4
# medium shuffle 48  CheckpointMode 2  MaxThreads 4
# big    shuffle 64  CheckpointMode 1  MaxThreads 4
profile = resolve_execution_profile(
    ExecutionProfile or execution_profile or "low"
)
```

Then apply `profile["shuffle_partitions"]`, `profile["checkpoint_mode"]`,
and `profile["max_threads"]`. Explicit `SqlShufflePartitions`,
`CheckpointMode`, and `MaxThreads` override the profile. Do not set AQE.
Do not `from Common_V2.core import resolve_execution_profile`.

## Choose the mode

| User says | Mode |
|---|---|
| production, inline, main `output/`, ship it | **1 Production** |
| development, outputV2, side by side, A/B, profiler | **2 Development** |
| (unspecified) | **2 Development** until they ask to land in production |

Never mix modes in one change set. Never add `updated/` or `business/`
folders in either mode.

## SP-specific skills

Use the parent skill for packaging, Checkpoint V2, phasing, profiles,
and notebooks. Use the child skill when the work is that SP:

| SP | Skill |
|---|---|
| `usp_get_final_effective_percentage` | [usp-get-final-effective-percentage/SKILL.md](usp-get-final-effective-percentage/SKILL.md) |
| `usp_load_footnotes_allocation_to_output` | [usp-load-footnotes-allocation-to-output/SKILL.md](usp-load-footnotes-allocation-to-output/SKILL.md) |

The child skill owns SP-only checkpoints, extra tables, and locked
timings. Do not copy those into other SPs.

## Mode 1 — Production

Inline edits in the live package. Ready to run as production.

```text
Source/AllocationV2/<sp_name>/output/
├── __init__.py
├── orchestrator.py              # modified in place (or the SP entry module)
├── parallel_helpers.py          # only if phasing is added
└── notebook/
    └── run_<sp_name>.py         # runs THIS orchestrator only
```

**Do not create:** `outputV2/`, `updated/`, `business/`, `_pre_opt/`,
`plan_profiler.py`, `output_reconcile.py`, or any profiler shim.

**Notebook:** single-variant run of `AllocationV2.<sp>.output.<entry>`.
Widgets for source path, SP parameters, ExecutionProfile, MaxThreads,
ParallelGroups, CheckpointMode, SqlShufflePartitions. Wall time and row
counts only. The live orchestrator resolves the profile.
**No** original-vs-updated compare, **no** ProfilePlan, **no** table-hash
A/B.

## Mode 2 — Development

Leave production `output/` unchanged. Candidate lives in `outputV2/` and
imports production helpers from the sibling `output/` package of this SP
(`parent.py` / `..output`). Do not copy production business modules into
`outputV2/` except orchestrator-local optimization modules.

```text
Source/AllocationV2/<sp_name>/
├── output/                      # PRODUCTION — do not edit
└── outputV2/
    ├── __init__.py
    ├── orchestrator.py          # candidate (same public API)
    ├── parent.py                # import unchanged prod helpers
    ├── parallel_helpers.py
    ├── plan_profiler.py         # slim shim; no-op unless ProfilePlan on
    ├── output_reconcile.py      # per-table fingerprints / hashes
    └── notebook/
        └── benchmark_<sp_name>.py
```

**Notebook:** side by side. Original =
`AllocationV2.<sp>.output.<entry>`. Updated =
`AllocationV2.<sp>.outputV2.<entry>`. Purge RunID between variants.
Compare every output table: row count, schema, numeric sums, order-independent
hash (`xxhash64` sum/min/max). Optional `ProfilePlan` (default off).

Do not add a `business/` folder unless this SP already uses that FEP-only
pattern **and** the user asks for it.

## Required inputs

- SP directory and entry function
- output tables and RunID column
- run parameters and Databricks `source_path`
- **mode 1 or 2**

## Non-negotiable invariants

1. Same business logic in both modes. Do not “optimize” by changing filters,
   joins, schemas, or writes.
2. Checkpoint **only** via `Common_V2.core.checkpoint_V2` (`checkpoint_V2`
   aliased `checkpoint`, `initialize_checkpoint_V2`, `resolve_checkpoint_mode`).
   No local `checkpoint.py`. No `Common_V2.core.checkpoint`.
   `checkpoint_mode=None` / `CheckpointMode=None` then resolve. Initialize
   once before worker threads copy `cfg`.
   Modes: 1=Delta, 2=odd local/even Delta, 3=odd local/even Volume, 4=all
   localCheckpoint. Inherit `DEFAULT_CHECKPOINT_MODE` unless overridden.
3. The **SP orchestrator** resolves `ExecutionProfile` (`low`/`medium`/`big`)
   at invocation start. Import
   `Common_V2.core.execution_profiles.resolve_execution_profile` directly.
   Do not resolve or re-export profiles from `Common_V2.core.__init__`.
   Explicit `CheckpointMode`, `SqlShufflePartitions`, and `MaxThreads`
   override the profile. Do not set AQE from a profile.
4. Parallelize only independent work, in **phases**. Cap workers **1..4**.
   Observe every future. Wave time is `max(task)`, not the sum.
5. No `output/*_updated.py`. No SP-root `notebooks/`.
6. Mode 1: no plan profiler code at all. Mode 2: slim profiler only, off
   unless the notebook sets ProfilePlan on.
7. Mode 2: never report success until every table hash matches.
8. Duck-type DataFrames. Do not rename public helper symbols.
9. Do not drop checkpoints on the hot path. Do not coalesce/repartition
   solely to shrink a Delta checkpoint.
10. Preserve production checkpoint seams by default.

## Common optimizations (both modes)

| Optimization | Notes |
|---|---|
| Parallel thread phasing | Independent loads/builders/writes |
| Bounded ThreadPoolExecutor | Max 4; log START/DONE |
| Isolated `cfg` | Parallel tasks that mutate config |
| Checkpoint V2 | Existing seams |
| RunID / ClientID / TaxPeriodID prune | When columns exist |
| Bounded broadcast | Proven-small lookups only |
| DataFrame reuse / persist | Persist only for multi-action; unpersist in `finally` |
| Concurrent distinct-table writes | Different tables only |
| Execution profile `low`/`medium`/`big` | Orchestrator resolves shuffle, checkpoint mode, MaxThreads=4 |

Do not copy FEP CPBT/footnote/state rewrites into other SPs.
Do not put AQE in the profile; it stays at the cluster default (`true`).

**Plan profiler:** Mode 2 only. See implementation reference.

## Workflow

1. Map the DAG (stages, actions, `cfg`, checkpoints, writes).
2. Apply phasing + Checkpoint V2 + portable read optimizations in the
   mode’s target package. The orchestrator resolves the execution profile
   at start. Logic stays equivalent.
3. Mode 1: add `output/notebook/run_<sp>.py`. Mode 2: add
   `outputV2/notebook/benchmark_<sp>.py` plus reconcile hashes.
4. Syntax-check. Mode 2: require per-table hash parity before claiming a win.

## Completion summary

- mode (1 Production or 2 Development)
- files created or changed
- phases and why they are independent
- checkpoint V2 usage and which execution profile the orchestrator applied
- Mode 2: per-table hash result; profiler on/off

## Examples

User: “Production mode on `usp_example`.”

→ Edit `output/`, add `output/notebook/run_usp_example.py`. No profiler,
no `outputV2`.

User: “Development mode on `usp_example`.”

→ Unchanged `output/`. Create `outputV2/` with slim profiler and A/B
notebook that hashes each table.
