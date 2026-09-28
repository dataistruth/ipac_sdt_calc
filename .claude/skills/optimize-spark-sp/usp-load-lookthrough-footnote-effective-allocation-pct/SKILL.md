---
name: optimize-usp-load-lookthrough-footnote-effective-allocation-pct
description: Optimizes Development outputV2 for uspLoadLookThroughFootnoteEffectiveAllocationPercentage (Checkpoint V2 on lt_output, parallel cost/book/temp-input plans, sequential Output then Input writes). Use when generating, benchmarking, or diagnosing this SP. Do not parallelize Output+Input writes.
---

# Optimize uspLoadLookThroughFootnoteEffectiveAllocationPercentage

Use this skill only for
`AllocationV2/usp_load_lookthrough_footnote_effective_allocation_pct`.

Parent packaging lives in [optimize-spark-sp](../SKILL.md). The **SP
orchestrator** resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles` at start. Default **low**. No AQE.
No `business/`. Production `output/` is the correctness baseline.

Public entry: `run_load_lt_footnote_effective_allocation_pct`.

## Allowed outputV2 tree

```text
outputV2/
├── __init__.py
├── load_lt_footnote_effective_allocation_pct.py
├── parent.py
├── parallel_helpers.py
├── write_helpers.py
├── plan_profiler.py
├── output_reconcile.py
├── OPTIMIZATION_REPORT.md
└── notebook/benchmark_load_lt_footnote_effective_allocation_pct.py
```

Import production via `output_module("load_lt_footnote_effective_allocation_pct")`.
Do not copy the production module into `outputV2/`.

## Parallel groups

```python
{"independent_builders"}
```

| Group | Tasks |
|---|---|
| sequential | yearly %, partners, FEP, `build_lt_allocation_output`, Checkpoint V2 `lt_output` |
| `independent_builders` | cost %, book %, temp allocation input (lazy plans only) |
| sequential | empty-input gate, single/multi, K1 amounts, final %, build frames |
| sequential writes | LookThroughAllocationOutput append, then LookThroughAllocationInput overwrite |

Keep sequential: `_load_sp_config` and skip gates, `load_mappings` →
`expand_parent_k1_mappings` (isEmpty), `build_distinct_mappings`, K1
gate, yearly/partners/FEP/`lt_output` (those fire Spark actions), and
both result writes. Do not drop `lt_output`. Do not parallelize classify
→ K1 amounts → final % → build frames.

Thread prefix: `lt-fn-eff-pct`. Isolated `{**cfg}` on parallel builder
tasks.

## Reconcile

Snapshot `LookThroughAllocationOutput` and `LookThroughAllocationInput`
on `RunID` after inspecting live columns. **Restore** (do not purge
Output) before each variant — §7 reads existing Output rows.

## Notebook defaults (FEP Development run)

| Widget | Default |
|---|---|
| source_path | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| EntityID | `4137` |
| ClientID | `15348` |
| TaxPeriodID | `1` |
| RunID | `17376` |
| CatalogName | `QA7` |
| SchemaName | `iPC_2025_QA7_15348` |
| VolumePath | `/Volumes/qa7/datavolume/databrickdata` |
| ExecutionProfile | `low` |
| MaxThreads | blank |
| ProfilePlan | `off` |
| SqlShufflePartitions | blank |

Pass profile / MaxThreads / ParallelGroups / ProfilePlan / CheckpointMode
only to updated. Fair timing: ProfilePlan off.

## Known bad

- Purging `LookThroughAllocationOutput` before a variant
- Copying the 2k-line production module into `outputV2/`
- Dropping `lt_output`
- Parallelizing mapping expand or the classify→write DAG
- Parallel `output_writes` (Output append + Input overwrite) — A/B
  LookThroughAllocationOutput count mismatch (original 110 rows)
- Parallel yearly/partners/FEP/`lt_output` (Spark actions / `collect`)
- FEP CPBT / footnotes `cost_snapshot` rewrites
