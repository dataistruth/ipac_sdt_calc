# SparkMigrate Agent — Lessons Learned

**Source:** uspGetFinalEffectivePercentage conversion & optimization (May 2026)
**Baseline:** 216s → 142s (Round 1) → 147s (Round 3 with localCheckpoint) → pending (Round 4 with footnote fix)

---

## 1. Checkpoint Strategy

### 1.1 Delta vs localCheckpoint — Decision Framework

The agent should select checkpoint type based on consumer analysis:

| Scenario | Checkpoint Type | Reason |
|---|---|---|
| DataFrame has 3+ downstream consumers | Delta (`saveAsTable`) | Fault tolerance + prevents N× re-evaluation |
| DataFrame has 1-2 downstream consumers AND is internal to a function | `localCheckpoint(eager=True)` | Breaks lineage without I/O overhead (~10-50× faster) |
| DataFrame is used across conversation boundaries (Surgical mode) | Delta | Must survive Spark session restart |
| DataFrame feeds into `build_final_output` / final write | Delta | Must persist for result verification |
| DataFrame inside a priority-matching loop (e.g., CPBT 7-step) | `localCheckpoint` | Internal DAG breaks; no need for persistence |

**Rule:** The agent MUST analyze consumer count before choosing checkpoint type. Default to Delta for safety; switch to `localCheckpoint` only after verifying:
1. No dot-qualified column refs (`D.InvestmentID`) survive in downstream joins
2. DataFrame is not the sole materialization point for a large pipeline
3. All downstream consumers are within the same Spark session

### 1.2 localCheckpoint Incompatibility — Alias Context Stripping

**Problem:** `df.localCheckpoint(eager=True)` strips table-alias metadata from column names. If downstream code uses `F.col("D.InvestmentID")` where `D` was an alias, Spark raises `UNRESOLVED_COLUMN` because the alias context is gone.

**Detection rule for the agent:**
Before switching a checkpoint to `localCheckpoint`, scan ALL downstream code for:
```python
F.col("ALIAS.column_name")   # e.g., F.col("D.InvestmentID")
df.alias("X").join(...)       # followed by F.col("X.column")
```

If any dot-qualified column references exist between this checkpoint and the next checkpoint, **do NOT use localCheckpoint**. Use Delta checkpoint instead.

**Affected pattern:** `compute_effective_percentage_dated` uses `no_transfer_entities.alias("D")` followed by `F.col("D.InvestmentID")` in anti-joins. This function MUST use Delta checkpoints.

**Agent action:** Add a pre-flight scan step before switching checkpoint types. Log: *"Dot-qualified column refs detected in downstream code — using Delta checkpoint."*

### 1.3 Checkpoint Naming — No Spaces

**Problem:** Checkpoint names are interpolated into Delta table names:
```python
fqn = f"{catalog}.{schema}._tmp_fep_{name}_{run_id}"
```

If `name` contains spaces (e.g., `"eff_pct_dated after transfer steps"`), the resulting table name is invalid SQL, causing `PARSE_SYNTAX_ERROR`.

**Rule:** The agent MUST use only `[a-z0-9_]` characters in checkpoint names. Replace descriptive phrases with abbreviated identifiers:
- ✗ `"eff_pct_dated after transfer steps"` → SQL error
- ✓ `"eff_pct_dated_post_transfer"` → valid

**Agent action:** When generating checkpoint names, validate against the regex `^[a-z0-9_]+$`. Reject any name with spaces, hyphens, or special characters.

---

## 2. Checkpoint Removal — Cascading DAG Re-evaluation

### 2.1 The Multiplier Effect

**Problem:** Removing a checkpoint doesn't just save the I/O time of that one checkpoint. It EXPOSES the upstream lazy pipeline to re-evaluation at EVERY downstream checkpoint or action.

**Example from this SP:**
- Removing `input_lines` checkpoint (saved ~4s I/O) caused:
  - `build_cost_percentage_by_type` internal checkpoints (7×) each re-evaluated `build_input_lines` (3.3s) = +23s
  - `post_missing_entities` checkpoints re-evaluated the full chain = +15s
  - `compute_effective_percentage_dated` re-evaluated again = +16s
  - **Net: −4s saved, +54s added = 50s REGRESSION**

**Rule:** Before removing ANY checkpoint, the agent MUST:
1. Count all downstream checkpoints/actions between this checkpoint and the next one
2. Multiply: `upstream_compute_time × downstream_checkpoint_count`
3. If `multiplied_cost > checkpoint_io_time`, the checkpoint is a NET POSITIVE — keep it

**Formula:**
```
removal_safe = (upstream_compute_time × downstream_checkpoints) < checkpoint_io_overhead
```

For Delta checkpoints with ~2s I/O each:
- If upstream takes 3s and there are 7 downstream checkpoints: 3×7 = 21s >> 2s → KEEP
- If upstream takes 0.1s and there are 2 downstream checkpoints: 0.1×2 = 0.2s << 2s → SAFE TO REMOVE

### 2.2 Multi-Branch Consumer Analysis

**Problem:** A DataFrame that appears to have "1 consumer" may actually be consumed N times inside that consumer function (e.g., `footnote_input_lines` is read 8× inside `build_footnote_dated_entities`).

**Rule:** The agent MUST look INSIDE consumer functions to count actual references, not just count function calls. Specifically:
- Search for the parameter name in the function body
- Count `.join(`, `.filter(`, `.select(` operations that reference it
- If the parameter is passed to a helper that's called N times, multiply

**Example:**
```python
# Looks like 1 consumer:
build_footnote_dated_entities(spark, cfg, footnote_input_lines, ...)

# But internally reads footnote_input_lines 8 times:
_package_entity_join(input_lines, "PFICFootnotePackage", ...)    # 1
_package_entity_join(input_lines, "Form926Package", ...)         # 2
_package_entity_join(input_lines, "Form8865Package", ...)        # 3
_package_entity_join(input_lines, "Form1042SPackage", ...)       # 4
_package_entity_join(input_lines, "Form8886Package", ...)        # 5
_package_entity_join(input_lines, "Form199APackage", ...)        # 6
_package_entity_join(input_lines, "AtRiskPackage", ...)          # 7
_package_entity_join(input_lines, "CustomFootnotePackage", ...)  # 8
```

**Agent action:** Before passing any uncheckpointed DataFrame to a function, scan the function body for multi-reference patterns. If the parameter is used N>2 times, checkpoint it first.

---

## 3. Performance Optimization Strategy

### 3.1 Diagnose Before Optimizing — Compute vs I/O

**Problem:** The agent initially assumed checkpoint I/O was the bottleneck (removing checkpoints to save ~2s each). In reality, the bottleneck was COMPUTE (footnote materialization = 32s, CPBT matching = 22s, effective_calc = 22s). Removing I/O checkpoints made compute WORSE by triggering re-evaluation.

**Rule:** The agent MUST distinguish compute time from I/O time before optimizing:

1. **I/O-bound checkpoint:** Time between `[LOCAL-CHECKPOINT] name` log and the next section start. If the checkpoint itself takes >2s but the preceding `[TIMING]` shows 0.0s, the cost is I/O.

2. **Compute-bound checkpoint:** If `[TIMING] function: Xs` shows X>5s AND the section includes multiple joins/windows, the cost is compute. Removing downstream checkpoints will make this WORSE.

**Optimization priority:**
1. First: reduce COMPUTE (restructure joins, add upstream checkpoints to prevent re-evaluation)
2. Second: switch I/O-only checkpoints from Delta to `localCheckpoint`
3. Last: remove checkpoints (only when formula in 2.1 confirms safety)

### 3.2 The Footnote Materialization Pattern

**Problem:** `build_footnote_dated_entities` takes 32s to materialize because `footnote_input_lines` (an uncheckpointed complex pipeline) is re-evaluated 8× internally (once per footnote type).

**Fix:** Checkpoint `footnote_input_lines` BEFORE passing it to `build_footnote_dated_entities`.

**Generalized rule:** When a function takes a DataFrame parameter and passes it to a helper function N times (e.g., `_package_entity_join(input_lines, ...)`), the agent MUST checkpoint that parameter before the call. The checkpoint type depends on the alias analysis in §1.2.

**Pattern to detect:**
```python
# Anti-pattern: uncheckpointed input consumed N times
result = expensive_function(spark, cfg, uncheckpointed_df, ...)

# Fix: checkpoint first
checkpointed_df = _local_checkpoint(spark, uncheckpointed_df, "name", cfg)
result = expensive_function(spark, cfg, checkpointed_df, ...)
```

### 3.3 WorkflowHelper — Eliminate Multiple Spark Actions

**Problem:** Original config_loader.py executed 5 separate Spark actions to get workflow/transaction IDs:
1. AllocationRun query → collect
2. WorkflowStatus query → collect  
3. WorkFlowChain query → collect
4. TransactionLog aggregation → collect (×2)

**Fix:** Replace with single `WorkflowHelper.get_run_workflows()` call that reads AllocationRun once and derives all IDs.

**Generalized rule:** When the SP loads scalar configuration values from multiple related tables, check if an existing helper (e.g., `WorkflowHelper`) already provides them in a single read. The agent SHOULD:
1. Check `Common_V2/core/` and `common/utilities/` for existing helpers
2. If a helper exists, import and use it — never redefine
3. If no helper exists but 3+ scalar reads hit the same base table, propose extracting one

### 3.4 Pre-CPBT Checkpoints Are Mandatory

**Problem:** Removing `pre_cpbt_checkpoints_m2` (non_dated + dated + all_underlyings) saved ~35s of I/O but caused `build_cost_percentage_by_type` to explode from 22s to 166s because every internal checkpoint re-evaluated the entire footnote pipeline.

**Rule:** Pre-CPBT checkpoints are ALWAYS required when the per-mode augmentation path was taken (footnote or state allocation). They serve as DAG circuit breakers that prevent the 7 internal CPBT checkpoints from cascading upstream.

**Agent action:** NEVER remove pre-CPBT checkpoints. They are structural, not optional. The only optimization is switching them from Delta to `localCheckpoint` (if alias analysis passes).

### 3.5 Redundant Checkpoint Detection

**Safe optimization:** When the same DataFrame is checkpointed twice under the same (or compatible) name, the second checkpoint is pure waste.

**Example from this SP:**
```python
# Checkpoint 1: footnote augmentation
mode_all_underlyings = _checkpoint(spark, mode_all_underlyings, f"all_und_final_m{mode}", cfg)
# ... footnote processing ...
# Checkpoint 2: pre-CPBT (same DataFrame, same name!)
mode_all_underlyings = _checkpoint(spark, mode_all_underlyings, f"all_und_final_m{mode}", cfg)
```

The second checkpoint re-writes the same data. Safe to skip.

**Agent action:** Before writing a checkpoint, check if `cfg["_checkpoint_tables"]` already contains this exact table name. If so, skip the write and read from the existing table instead.

---

## 4. Conversion Correctness

### 4.1 _mode Column Is Integer, Not BIT

**Problem:** `validate_output.py` flags `F.col("_mode") == 1` as GAP-05 (BIT column comparison). But `_mode` is a synthetic integer column added by the orchestrator (values: 1, 2, 3, 4), not a SQL Server BIT column.

**Fix needed in validate_output.py:** The GAP-05 rule should exclude columns that:
- Start with `_` (synthetic/internal columns)
- OR are explicitly listed in a per-SP allowlist

**Agent action:** When encountering this false positive, annotate it as known-safe and do not attempt to fix it. Add a `# noqa: GAP-05` comment or equivalent suppression.

### 4.2 Spark 4.0 UTF8_LCASE Collation

When `collation_strategy: UTF8_LCASE` is set in `project_manifest.json`, string comparisons are case-insensitive at the engine level. The agent should:
- Skip `F.lower()` wrapping on string comparisons (GAP-02 warnings are false positives)
- Document this in the SP's `_state/` folder
- Not "fix" GAP-02 warnings that the validator raises

### 4.3 Empty DataFrame Handling in Chains

**Pattern:** The `nolt` chain produces empty DataFrames (0 rows) because `lt_input` is empty for modes 2/3. Checkpointing empty DataFrames to Delta takes ~2s each (schema write + read).

**Optimization:** Use `localCheckpoint` for chain checkpoints. Empty DataFrames checkpoint instantly in memory.

**Anti-pattern to avoid:** Do NOT skip checkpoints entirely for empty DataFrames. Even empty DataFrames carry lineage that can cascade when unioned with non-empty DataFrames from other modes.

---

## 5. Testing & Validation

### 5.1 Always Run validate_output.py After Changes

Every code modification — even "safe" refactors like renaming checkpoints — must be validated. The validator catches:
- Syntax issues (invalid checkpoint names)
- Missing imports
- Column reference patterns that will fail at runtime

### 5.2 Incremental Testing Strategy

When optimizing performance:
1. Make ONE change at a time
2. Run the notebook
3. Compare timing logs line-by-line against the previous run
4. If regression detected, revert immediately — do not stack more changes

**Anti-pattern:** Making 4+ checkpoint removals in one batch, then discovering a 2× regression with no way to identify which removal caused it.

### 5.3 Row Count Verification After Checkpoint Changes

After switching checkpoint types (Delta → localCheckpoint) or removing checkpoints:
1. Run the pipeline
2. Compare final output row counts against the Delta checkpoint baseline
3. If row counts differ, a checkpoint was providing deduplication (via `saveAsTable` + `spark.table()` round-trip) that `localCheckpoint` doesn't provide

---

## 6. Agent Workflow Improvements

### 6.1 Phase 7 (Performance) Should Start with Profiling

Before ANY optimization:
1. Collect full timing logs
2. Build a waterfall chart (phase → time → compute vs I/O)
3. Identify the top 3 bottlenecks by absolute time
4. Only then propose changes — targeting compute bottlenecks first

### 6.2 Checkpoint Inventory

At the start of Phase 7, the agent should build a checkpoint inventory:

```
| Checkpoint Name | Type | Location | Consumers | Upstream Cost | I/O Cost |
|---|---|---|---|---|---|
| input_lines_lt | Delta | orchestrator:465 | 4 (non_dated, dated, entity_und, amounts) | 3.2s | 2s |
| tcp_post_et_m0 | local | cost_pct_loader:548 | 1 (sequential) | 7s | 0.5s |
```

This inventory prevents blind removal and enables informed decisions.

### 6.3 Revert Protocol

If a change causes >10% regression:
1. Revert ALL changes from that round (not just the suspected one)
2. Re-run to confirm baseline is restored
3. Then apply changes ONE AT A TIME with individual timing validation

---

## 7. Summary of Optimization Results

| Round | Change | Impact | Status |
|---|---|---|---|
| Baseline | Original conversion | 216s | — |
| Round 1 | Checkpoint footnote underlyings immediately after build | 216→142s (−74s) | ✅ Kept |
| Round 1 | Remove 4 redundant checkpoints in cost_pct_loader | −4s (within 142s) | ✅ Kept |
| Round 2 | Remove pre_cpbt_m2, input_lines, tcp_by_type_fused, txfr_adj_fused | 142→287s (+145s REGRESSION) | ❌ Reverted |
| Round 2 | Remove 2 effective_calc checkpoints | 142→150s (+8s REGRESSION) | ❌ Reverted |
| Round 3 | Switch 15+ checkpoints from Delta to localCheckpoint | 150→147s (−3s) | ✅ Kept |
| Round 3 | Remove tcp_post_ptk, tcp_post_parent in CPBT | −3s (within 147s) | ✅ Kept |
| Round 3 | Switch effective_calc back to Delta (alias issue) | — (correctness fix) | ✅ Kept |
| Round 4 | Checkpoint footnote_input_lines before 8× consumption | Pending test | ⏳ Pending |
| All | WorkflowHelper integration (5 actions → 1) | −2s config time | ✅ Kept |

**Key insight:** The biggest win (−74s) came from adding a checkpoint, not removing one. The biggest regression (+145s) came from removing checkpoints. In DAG-heavy pipelines, checkpoints are primarily compute optimizations (preventing re-evaluation), not I/O costs.
