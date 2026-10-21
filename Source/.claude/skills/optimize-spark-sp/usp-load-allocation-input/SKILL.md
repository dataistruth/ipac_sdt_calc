---
name: optimize-usp-load-allocation-input
description: Regenerates or extends uspLoadAllocationInput with profile checkpoint (including alloc_form_k1), three parallel groups, and AllocationInput-first flow-up writes. Use when implementing, generating, benchmarking, or diagnosing this SP. Do not invent extra modules.
---

# Optimize uspLoadAllocationInput

Use this skill only for `AllocationV2/usp_load_allocation_input`.

When the user asks to **generate**, **optimize in Development mode**, or
**recreate outputV2**, reproduce the locked candidate below. Do not
redesign packaging. Do not add files that are not in the allowed tree.
Portable two-mode rules live in the parent
[optimize-spark-sp](../SKILL.md) skill. Read
[SP_OPTIMIZATION_GUIDE.md](SP_OPTIMIZATION_GUIDE.md) for the DAG.

The **SP orchestrator** (`outputV2/load_allocation_input.py`) resolves
`ExecutionProfile` from `Common_V2.core.execution_profiles` at run start.
Do not resolve profiles from `Common_V2.core.__init__`. Default `DEFAULT_EXECUTION_PROFILE` (**small**).
Do not set AQE.

Public entry: `run_load_allocation_input`. Adapter:
`run_usp_load_allocation_input` in `usp_load_allocation_input.py`.

Do not copy FEP / footnotes / look-through SP-only opts. Development:
**flat** `outputV2/` (no `business/`, `tests/`, `updated/`). Production:
**inline** in `output/`.

## Locked candidate (regenerate this)

Exact-parity A/B, 2026-09-28, RunID `16560`, EntityID `115`,
ClientID `15348`, TaxPeriodID `1`, catalog `QA7`, schema
`IPC_2025_QA7_15348`, ProfilePlan **off**, ExecutionProfile **small**:

| Variant | Notebook wall | Reported |
|---|---|---|
| production `output/` | 69.814s | 67.6s |
| updated `outputV2/` | 49.194s | **48.4s** |

Store: production `GenericResultStorer` batch **17.2s** vs updated
`output_writes` **5.666s** (8 tables, 4 workers). Fingerprints **PASS**.

Treat **48.4s / exact parity** as the Development bar. Do not replace this
shape with experiments that failed (see Known bad).

## Allowed outputV2 tree

```text
Source/AllocationV2/usp_load_allocation_input/
├── output/                          # PRODUCTION — do not edit in Development
└── outputV2/
    ├── __init__.py                  # export run_load_allocation_input only
    ├── usp_load_allocation_input.py # run_usp_load_allocation_input adapter
    ├── load_allocation_input.py     # orchestrator (public API)
    ├── parent.py                    # unused / self package; copy ai_*.py into outputV2/
    ├── plan_profiler.py             # slim shim; no-op unless ProfilePlan on
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/
        └── benchmark_load_allocation_input.py
```

**Do not create:** `form_flowup_collect.py`, nested `business/`,
`tests/`, `updated/`, `output/*_updated.py`, copies of `ai_*.py` inside
`outputV2/`. Production services stay in `output/` and are imported via
`parent.py`.

`parent.py` must be:

```python
_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"

def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")
```

Example: `output_module("ai_finalization_service")` is
`AllocationV2.usp_load_allocation_input.output.ai_finalization_service`.

## Parallel helpers (shared `Common_V2.core.parallel_helpers`)

No SP-folder `parallel_helpers.py`. The orchestrator imports
`normalize_workers`, `parse_enabled_groups`, `run_parallel` from
`Common_V2.core.parallel_helpers` and declares `KNOWN_GROUPS` exactly:

```python
KNOWN_GROUPS = frozenset({"independent_input_builders", "output_collect", "output_writes"})
enabled_groups = parse_enabled_groups(parallel_groups, ParallelGroups, known_groups=KNOWN_GROUPS)
```

`run_parallel` is sequential if the group is disabled, workers<=1, or
len(tasks)<=1; otherwise a pool capped at `min(workers, task_count, 4)`
that observes every future and returns results in **declared task order**.

## Orchestrator (`load_allocation_input.py`)

Import production via `output_module` only:

`ai_config_service`, `ai_shared_views`, `ai_validation_service`,
`ai_hierarchy_service`, `ai_k1_service`, `ai_form_service`,
`ai_pfic_service`, `ai_pfic_flowup_service`, `ai_finalization_service`.

Checkpoint: `Common_V2.core.checkpoint` — `checkpoint as common_checkpoint`,
`drop_checkpoints` after the try on the SUCCESS path. Profile `small` → local
(mode 2), `large` → Delta.
`ai_pfic_flowup_service` `base_flowup` and `ai_shared_views` `reclass_data`
stay Delta (`checkpoint_mode=DELTA_CHECKPOINT_MODE`).

Wrap `_checkpoint` with `track_checkpoint_plan`. Wrap listed builders with
`track_plan`. Do **not** wrap `load_config` with `track_plan`.

Signature must accept production params plus Development-only
`profile_plan` / `ProfilePlan`, `plan_checkpoint_threshold`,
`execution_profile` / `ExecutionProfile` default `DEFAULT_EXECUTION_PROFILE` (**small**), `MaxThreads`,
`ParallelGroups`, `CheckpointMode`, `SqlShufflePartitions`.

### Stage order (do not reorder)

1. **S1** `load_common_config` if needed → apply profile shuffle /
   checkpoint mode / MaxThreads → `cfg["checkpoint_mode"]` → abort
   `run_status=FAIL` → `load_config` → `register_shared_views`.
2. **S2 sequential:** `build_entity_hierarchy` (temp view
   `_entity_hierarchy_{run_id}`), `build_lower_tier_funds`,
   `build_workflows`.
3. **S3 sequential:** `run_validations`; abort FAIL; `purge_output_tables`.
4. **S4–S6 parallel `independent_input_builders`:**  
   `build_all_form_inputs(spark, {**cfg}, k1_workflow_df)`,  
   `build_k1_and_related_inputs(spark, {**cfg})`,  
   `build_pfic_snapshot(spark, {**cfg}, k1_workflow_df)`.  
   Isolated `{**cfg}`. No temp views in these three.
5. **S6 sequential unions:** checkpoint `pfic_snapshot` → union form+k1 →
   checkpoint **`alloc_form_k1`** → `build_pfic_allocation_input` → union →
   `build_custom_footnote_input` (temp view `_cf_latest_txn_*`) → union →
   checkpoint `alloc_input`.

   `alloc_form_k1` is a required seam in both Production and Development
   generation. Put it immediately after `form_df.unionByName(k1_df,
   allowMissingColumns=True)` and before PFIC allocation/custom-footnote rows:

   ```python
   allocation_input_df = form_df.unionByName(
       k1_df, allowMissingColumns=True
   )
   allocation_input_df = _checkpoint(
       spark, allocation_input_df, "alloc_form_k1", cfg
   )
   ```

   Keep the later `alloc_input` checkpoint. The first seam cuts the form+K1
   lineage; the second materializes the completed AllocationInput after PFIC
   and custom-footnote unions. Do not replace one with the other.
6. **S7 sequential:** `build_pfic_flowup_pipeline` (inner production
   `base_flowup` 7a-1 / 7a-2 stay inside that helper) → checkpoint
   **`pfic_raw`** → `check_pfic_xml_override_alert` →
   `apply_pfic_election_deletes` → `apply_part_v_vii_flags` → checkpoint
   **`pfic_flowup`**.
7. **S8 sequential:** master feed, blocker cleanup, distribution
   suppression → checkpoint `alloc_filtered` → `apply_tag_percentages` →
   checkpoint `alloc_tagged` only if `investment_tag_workflow_id != 0`.
8. **S9:** `collect_output_frames_parallel` then
   `flush_collected_results` via `profile_action`.

Print `[outputV2] ExecutionProfile=... CheckpointMode=... shuffle=... MaxThreads=... ProfilePlan=...`.

Expose `get_last_run_profile()`.

## Output collect + flush (`ai_finalization_service.py`)

No SP-folder `write_helpers.py`. `collect_output_frames_parallel`,
`flush_collected_results`, and `SMALL_TABLES` live in
`ai_finalization_service.py` next to `write_allocation_input`,
`write_pfic_flowup`, `write_form_flowups`, and delegate the threading /
isolated-cfg merge / storer flush to `Common_V2.core.write_helpers`
(`run_isolated_tasks`, `flush_collected_results(...,
replace_where_run_tables=("AllocationInput",), small_tables=SMALL_TABLES)`).
Do **not** split `write_form_flowups` into a generated per-form module.

### `output_collect` — exactly 3 tasks

| Task name | Production call |
|---|---|
| `AllocationInput` | `write_allocation_input` (collect only) |
| `PFICFootnoteFlowup` | `write_pfic_flowup` (collects PFIC tables) |
| `FormFlowups` | `write_form_flowups` (all form flowups in **one** task) |

Each task uses `isolated_cfg` so `_parquet_results` do not race; merge
after the wave with unionByName on colliding keys.

### Flush

1. Sequential Delta write of `AllocationInput` with
   `replaceWhere RunID = {run_id}`. Log `[ok] AllocationInput (delta)`.
2. Remaining keys in `_parquet_results` (not AllocationInput) →
   `output_writes`, one task per table. Writer **inside** the task.
   If `result_type == deltalake` and frame has `RunID`: Delta overwrite
   `replaceWhere`. Else single-table `GenericResultStorer.save_results`.
3. `SMALL_TABLES` coalesce(1) before write:
   Form926/199A/8865/8886, AtRisk, CustomFootnote, Form200616,
   PFICFootnoteFlowup.

Log `[store] Writing N flow-up tables in parallel` and
`[parallel] START phase=output_writes task=<TableName>`.

## output_reconcile.py

`TABLE_SPECS` RunID tables only:

AllocationInput, PFICFootnoteFlowup, PFICFootnoteFlowupWithTrackingKey,
Form926/199A/8865/8886 Flowup, AtRiskFlowup, CustomFootnoteFlowup,
Form200616Flowup, AllocationRunErrors.

Inspect live columns before `WHERE RunID`. Skip missing tables and tables
without `RunID`. **Do not** snapshot `PFICUpdateAlert` /
`PFICAlertDetails` on RunID.

Export aliases used by the notebook: `create_run_snapshots`,
`capture_outputs`, `compare_outputs`, `restore_run_snapshots`,
`drop_run_snapshots`.

## Notebook widgets (required defaults)

Frozen parent-skill Mode 2 widget **names and count** (10 only). This SP
overrides identity for the locked 48.4s A/B: EntityID `115`, RunID
`16560`, SchemaName `IPC_2025_QA7_15348`. Hardcode VolumePath /
ResultType. Widget 10 is `ProfilePlan` (`off`/`on`). Do **not** add
MaxThreads / shuffle / CheckpointMode / ExecutionOrder / ParallelGroups
widgets.

Put `source_path` first on `sys.path`. Evict
`AllocationV2.usp_load_allocation_input.output`, `.outputV2`,
`AllocationV2.plan_profiler`, `Common_V2`. Original module
`...output.load_allocation_input`; updated
`...outputV2.load_allocation_input`. Pass `ExecutionProfile` and
`ProfilePlan` **only** to updated. Purge only tables that have a `RunID`
column. Restore snapshots in `finally`.

Default `ProfilePlan` **off** for fair timing. Last display cells use
the parent-skill locked `runtime_schema` / `parity_schema`. Hash sum is
`sum(_h.cast("decimal(38,0)"))`.

## Target configuration

- `ExecutionProfile=small` → shuffle 32, CheckpointMode 2, MaxThreads 4
- Explicit CheckpointMode / shuffle / MaxThreads override the profile
- Required orchestrator checkpoints include `pfic_snapshot`,
  **`alloc_form_k1`**, `alloc_input`, `pfic_raw`, `pfic_flowup`,
  `alloc_filtered`, and optional `alloc_tagged`.
- Inner production `base_flowup` checkpoints stay local/eager as in
  production helpers; do not rewrite `ai_pfic_flowup_service.py`

### `alloc_form_k1` measured evidence

RunID `16560`, profile `small`, CheckpointMode 2:

- Before the extra seam, `alloc_input` materialization was **18.015s**.
- With `alloc_form_k1`, that work split into **5.182s**
  (`alloc_form_k1`) + **3.383s** (`alloc_input`) = **8.565s**.
- The following inner `base_flowup` 7a-1 changed from **8.5s** to **5.7s**.
- Output row counts stayed the same for the observed run. This evidence is
  local to the changed section; do not claim an end-to-end wall-clock win
  without a same-cluster A/B with identical write routing.

## Non-negotiable contracts

1. Production `output/` behavior is authoritative.
2. Exact fingerprints on reconcile tables (schema, rows, sums,
   `xxhash64` with `hash_sum = sum(_h.cast("decimal(38,0)"))`).
3. FAIL / SKIPPED gates unchanged.
4. No `__pycache__`. No `Common_V2.core.checkpoint` in the orchestrator.
5. Mode 1 Production: inline `output/` only, no profiler, no outputV2 mix.

## Known bad experiments (do not regenerate)

- `form_flowup_collect.py` / splitting `write_form_flowups` per table
- Overlapping AllocationInput Delta with flow-up writes
- Dropping orchestrator `pfic_raw` after 7a-2
- One `GenericResultStorer.save_results` for all flow-up tables
- Copying `ai_*.py` into `outputV2/`
- Parallel custom footnotes, hierarchy, or PFIC election/flowup chain
- FEP / footnotes-only plan breaks
- Snapshotting PFICUpdateAlert / PFICAlertDetails with `WHERE RunID`
- Inlining snapshot SQL in the notebook instead of `output_reconcile.py`
- Forcing SqlShufflePartitions=16 (overrides profile small)
- `F.sum("_h")` hash_sum (ANSI BIGINT overflow)
- Schema-less `display(spark.createDataFrame(runtime_rows|parity_rows))`

## Completion report

Report production vs outputV2 wall, per-table hash, groups that ran,
checkpoint names, ExecutionProfile, and `output_writes` wall vs ~17s
production store. Locked bar: **48.4s updated, PASS hashes**.
