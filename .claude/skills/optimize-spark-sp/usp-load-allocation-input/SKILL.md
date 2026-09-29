---
name: optimize-usp-load-allocation-input
description: Regenerates or extends the locked Development outputV2 for uspLoadAllocationInput (Checkpoint V2, three parallel groups, AllocationInput-first then eight parallel flow-up Delta writes). Use when implementing, generating, benchmarking, or diagnosing this SP. Do not invent extra modules or revert to GenericResultStorer batch writes.
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
Do not resolve profiles from `Common_V2.core.__init__`. Default **low**.
Do not set AQE.

Public entry: `run_load_allocation_input`. Adapter:
`run_usp_load_allocation_input` in `usp_load_allocation_input.py`.

Do not copy FEP / footnotes / look-through SP-only opts. No `business/`.

## Locked candidate (regenerate this)

Exact-parity A/B, 2026-09-28, RunID `16560`, EntityID `115`,
ClientID `15348`, TaxPeriodID `1`, catalog `QA7`, schema
`IPC_2025_QA7_15348`, ProfilePlan **off**, ExecutionProfile **low**:

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
    ├── parent.py                    # output_module("ai_*") → sibling output/
    ├── parallel_helpers.py
    ├── write_helpers.py             # output_collect + output_writes
    ├── plan_profiler.py             # slim shim; no-op unless ProfilePlan on
    ├── output_reconcile.py
    ├── OPTIMIZATION_REPORT.md
    └── notebook/
        └── benchmark_load_allocation_input.py
```

**Do not create:** `form_flowup_collect.py`, `business/`, `updated/`,
`output/*_updated.py`, copies of `ai_*.py` inside `outputV2/`. Business
services stay in production `output/` and are imported via `parent.py`.

`parent.py` must be:

```python
_OUTPUT_PACKAGE = f"{__package__.rsplit('.', 1)[0]}.output"

def output_module(name: str):
    return importlib.import_module(f"{_OUTPUT_PACKAGE}.{name}")
```

Example: `output_module("ai_finalization_service")` is
`AllocationV2.usp_load_allocation_input.output.ai_finalization_service`.

## parallel_helpers.py (required)

`KNOWN_GROUPS` exactly:

```python
{"independent_input_builders", "output_collect", "output_writes"}
```

- `normalize_workers`: clamp 1..4.
- `parse_enabled_groups`: `all` → all KNOWN_GROUPS; `none` → empty; else CSV.
- `isolated_cfg(cfg)`: shallow copy; new `_parquet_results={}`; copy
  `_schema_cache`.
- `run_parallel(tasks, workers, activity, label, enabled_groups)`:
  sequential if group disabled or workers<=1 or len(tasks)<=1.
  Else `ThreadPoolExecutor` prefix `ai-alloc-input`, cap
  `min(workers, task_count, 4)`. Observe every future. Raise after all
  complete if any failed. Log START/DONE and
  `critical=max-task`. Return results in **declared task order**.

## Orchestrator (`load_allocation_input.py`)

Import production via `output_module` only:

`ai_config_service`, `ai_shared_views`, `ai_validation_service`,
`ai_hierarchy_service`, `ai_k1_service`, `ai_form_service`,
`ai_pfic_service`, `ai_pfic_flowup_service`, `ai_finalization_service`.

Checkpoint V2: `checkpoint_V2 as checkpoint`, `initialize_checkpoint_V2`,
`resolve_checkpoint_mode`, `drop_checkpoints_V2` in `finally` on success
path (same as current file: drop after try when returning SUCCESS).

Wrap `_checkpoint` with `track_checkpoint_plan`. Wrap listed builders with
`track_plan`. Do **not** wrap `load_config` with `track_plan`.

Signature must accept production params plus Development-only
`profile_plan` / `ProfilePlan`, `plan_checkpoint_threshold`,
`execution_profile` / `ExecutionProfile` default **low**, `MaxThreads`,
`ParallelGroups`, `CheckpointMode`, `SqlShufflePartitions`.

### Stage order (do not reorder)

1. **S1** `load_common_config` if needed → apply profile shuffle /
   checkpoint mode / MaxThreads → `initialize_checkpoint_V2` → abort
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
   `build_pfic_allocation_input` → union → `build_custom_footnote_input`
   (temp view `_cf_latest_txn_*`) → union → checkpoint `alloc_input`.
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

## write_helpers.py (required)

Import production `write_allocation_input`, `write_pfic_flowup`,
`write_form_flowups` via `output_module("ai_finalization_service")`.
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

Default `ProfilePlan` **off** for fair timing.

## Target configuration

- `ExecutionProfile=low` → shuffle 32, CheckpointMode 4, MaxThreads 4
- Explicit CheckpointMode / shuffle / MaxThreads override the profile
- Inner production `base_flowup` checkpoints stay local/eager as in
  production helpers; do not rewrite `ai_pfic_flowup_service.py`

## Non-negotiable contracts

1. Production `output/` behavior is authoritative.
2. Exact fingerprints on reconcile tables (schema, rows, sums, xxhash64).
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
- Forcing SqlShufflePartitions=16 (overrides profile low)

## Completion report

Report production vs outputV2 wall, per-table hash, groups that ran,
checkpoint names, ExecutionProfile, and `output_writes` wall vs ~17s
production store. Locked bar: **48.4s updated, PASS hashes**.
