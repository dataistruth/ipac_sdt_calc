# Post-Conversion Errors & Fixes — uspGetFinalEffectivePercentage

**SP:** `uspGetFinalEffectivePercentage` (9,027 lines, Score 23, Surgical mode)  
**Phase 6 row count target:** 1,250 rows (SQL Server Mode 1, RunID=2074, EntityID=144)  
**Runs to match:** 8 pipeline executions over ~2 sessions

---

## Error 1: `_drop_checkpoints` Race Condition

**Symptom:** `result.count()` on the returned DataFrame threw errors or returned 0 because checkpoint Delta tables were already dropped.

**Root cause:** The `finally` block in `run_mode()` calls `_drop_checkpoints(spark, cfg)`, which drops all checkpoint tables. But the returned `result` DataFrame still references those tables via lazy evaluation. When the caller runs `result.count()`, the tables no longer exist.

**Fix:** For Phase 6 testing via Databricks Connect, monkey-patched `_drop_checkpoints` to a no-op in the test harness:
```python
_orch_mod._drop_checkpoints = lambda spark, cfg: None
```

**Production impact:** None — in production, the result is consumed inside `run_mode()` (written to output table) before `finally` runs. This only affects external callers who hold the lazy `result` DataFrame after `run_mode()` returns.

**Agent lesson → GAP-46:** When `run_mode()` returns a lazy DataFrame as `status["result"]`, the `finally` block must NOT drop checkpoint tables that the result depends on. Either materialize the result before cleanup, or defer cleanup to the caller.

---

## Error 2: UNRESOLVED_COLUMN `EffPercentage`

**Symptom:** `AnalysisException: UNRESOLVED_COLUMN.WITH_SUGGESTION` on `EffPercentage` in `build_final_output`.

**Root cause:** The column was created by `apply_plugging()` as `EffPercentage` but a downstream `.select()` referenced it with wrong casing or it was shadowed by a prior `.withColumnRenamed()`.

**Fix:** Ensured consistent column naming through the pipeline — the column produced by `apply_plugging()` is `EffPercentage` (matching `_columns.md`), and all downstream references use the same casing.

**Agent lesson:** Already covered by Rule 22 and GAP-42 (column name casing must match `_columns.md`). Reinforces the need to verify column names at every DataFrame handoff.

---

## Error 3: `entity_partners` Returns 0 Rows

**Symptom:** `build_entity_partners` returned 0 rows, causing downstream cross-joins to produce empty results.

**Root cause:** The function inlines `dbo.udf_PE_GetPartnerImportEventID`, which checks `GlobalMenu` for "Partner Import Methodology". If the menu state is "Master Import", the event name is `MasterImport_Partner`; otherwise it's `Import_Partner`. The initial conversion hardcoded the wrong event name.

**Fix:** Added dynamic event name resolution:
```python
import_type_row = (
    global_menu.join(enu_gmg, ...)
    .filter(F.col("M.State") == "C")
    .select("M.MenuName").first()
)
partner_event_name = "MasterImport_Partner" if import_type == "Master Import" else "Import_Partner"
```

**Agent lesson → GAP-47:** When inlining UDFs that branch on configuration values (GlobalMenu states), always preserve the branching logic. Never hardcode one branch — the configuration may differ across clients/environments.

---

## Error 4: `GPPartnerReceivingCarry` Type Mismatch (BOOLEAN vs INT)

**Symptom:** `unionByName` between `cost_percentage_snapshot_modes123` and yearly cost rows failed due to type mismatch — one side had `BOOLEAN`, the other had `INT`.

**Root cause:** `CostPercentage_704c_Snapshot.GPPartnerReceivingCarry` is a BIT column (maps to BOOLEAN in Delta), but the yearly prorata rows used `F.lit(None).cast("int")` for this column.

**Fix:** Changed the yearly cost rows to use `F.lit(None).cast("int")` consistently, and the modes123 snapshot to cast the column appropriately for unionByName compatibility.

**Agent lesson:** Already covered by Rule 3 (`F.lit(None)` must have `.cast(type)`) and GAP-05 (BIT columns as BOOLEAN). Reinforces: when building rows for `unionByName`, check the target schema's column types in `_columns.md` — especially BIT columns that may be BOOLEAN in some sources and INT in others.

---

## Error 5: `compute_minimum_quarter` Q0 Dedup Doubling

**Symptom:** 670 rows instead of expected 335 in `cost_pct_min_quarter` — exactly double.

**Root cause:** The Q0 dedup logic used `left_anti` to find deals without Q0, then `.unionByName(cost_pct_min_quarter.filter(Quarter == "Q0"))` to add Q0 rows back. But when ALL rows are already Q0 (the common case for this entity), `left_anti` keeps all 335 rows (no non-Q0 rows to anti-join against), and the union adds the same 335 Q0 rows again → 670.

**Fix:** Removed the `.unionByName(...)` — the `left_anti` alone correctly handles the dedup:
```python
# BEFORE (wrong):
deduped = non_q0.join(q0_deals, ..., "left_anti").unionByName(q0_rows)

# AFTER (correct):
deduped = non_q0.join(q0_deals, ..., "left_anti")
```
The `left_anti` keeps rows where the deal does NOT have a Q0 entry. When all rows are Q0, there are no non-Q0 rows to keep, and the Q0 rows are already in the source — no union needed.

**Impact on output:** None — `.distinct()` downstream eliminated the duplicates. But the fix prevents unnecessary data inflation in the DAG.

**Agent lesson → GAP-48:** When translating SQL's "keep Q0, skip other quarters for deals that have Q0" pattern (common in allocation SPs), the `left_anti` approach alone is sufficient. Do NOT union Q0 rows back — they're already in the source. The SQL pattern `DELETE ... WHERE DealId IN (SELECT DealId WHERE Quarter='Q0') AND Quarter<>'Q0'` translates to a single `left_anti`, not `left_anti + union`.

---

## Error 6: `compute_missing_entities` — SQL Bug Replication (Critical)

**Symptom:** PySpark returned 995 rows vs SQL Server's 1,250. Missing 255 Cost rows for Q0.

**Root cause:** SQL SP line ~7147 has:
```sql
WHERE D.TypeID <> @CostAllocationTypeID 
  AND D.UnderlyingEntityID IS NULL
```
The `D.UnderlyingEntityID IS NULL` checks the **LEFT** table's column (which is always non-NULL because it's the join key), not the **RIGHT** table's `C.DealId IS NULL` (which would indicate no match). This means `#TempNonDatedEntitiesCost` is always EMPTY in SQL — the TypeID update never happens.

The PySpark conversion correctly implemented the intended logic (`C.DealId.isNull()` — checking for non-matching entities), which caused entities with TypeID=6 to be updated to TypeID=2. But their TrackingKey stayed unchanged, causing the subsequent join on TrackingKey in Step 2 of `compute_effective_percentage_non_dated` to fail — producing 0 Cost rows for those entities.

**Fix:** Changed PySpark to replicate the SQL bug:
```python
# BEFORE (logically correct but differs from SQL):
.filter(... & F.col("C.DealId").isNull())

# AFTER (replicates SQL behavior — always returns 0 rows):
.filter(... & F.col("D.UnderlyingEntityID").isNull())
```

**Impact:** With the function effectively being a no-op (matching SQL), entities keep their original TypeIDs (mix of 2 and 6). TypeID=2 entities match cost_pct in Step 2 → get Cost rows. TypeID=6 entities don't match → fall to ProRata. Result: 88 entities × 5 = 440 Cost + 112 entities × 5 = 560 ProRata = 1,000 + 250 CostAdjustedDatedTransfer = **1,250 rows** ✓

**Agent lesson → GAP-49:** When a SQL SP contains a WHERE clause like `LEFT_TABLE.Column IS NULL` where that column is also the join key (always non-NULL), this is likely a SQL bug that makes the block a no-op. **Replicate the bug in PySpark.** Do NOT "fix" it — the downstream logic depends on the no-op behavior. Flag it in a comment:
```python
# NOTE: Replicates SQL bug at line ~7147. D.UnderlyingEntityID IS NULL
# checks the LEFT table (always non-NULL), making this filter always
# return 0 rows. The intended logic was likely C.DealId IS NULL.
# Fixing this changes downstream behavior. See GAP-49.
```

---

## Error 7: `build_cost_percentage_by_type` Hanging (Databricks Connect)

**Symptom:** The function took >10 minutes and appeared to hang during Phase 6 testing.

**Root cause:** Databricks Connect serializes the entire query plan via gRPC protobuf. The 3-tier cost matching (direct → parent → parent+TrackingKey) builds a deep DAG with 15+ joins. Without internal checkpoints, the serialized plan exceeded gRPC limits or took minutes to serialize.

**Fix:** Added 3 internal checkpoints within `build_cost_percentage_by_type` (one per tier), breaking the DAG into manageable chunks.

**Agent lesson:** Already covered by GAP-12 (checkpointing required) and GAP-44 (over-checkpointing). The lesson specific to Databricks Connect: **query plans with 10+ joins MUST have intermediate checkpoints when tested via Connect**, even if native execution could handle the full DAG. Budget 1 checkpoint per 5-7 joins in complex functions.

---

## Error 8: `transfers_adj` Empty (Not an Error)

**Symptom:** `transfers_adj` DataFrame was empty (0 rows), causing Step 1 of `compute_effective_percentage_non_dated` to be skipped entirely.

**Root cause:** For this test entity (EntityID=144), there are no transfer-adjusted cost records in `TransfersAdjCostDefaultPercentage`. This is legitimate — not all entities have transfers.

**Impact:** None. The function correctly handles empty transfers_adj by skipping Step 1 and proceeding to Step 2 (Cost from FinalCostPercentage) and Step 3 (ProRata).

**Agent lesson:** This validates that POSSIBLY-EMPTY annotations and `if transfers_adj is not None and not transfers_adj.isEmpty()` guards work correctly. No new GAP needed.

---

## Summary

| # | Error | Category | Runs to find | Fix type | New GAP? |
|---|---|---|---|---|---|
| 1 | `_drop_checkpoints` race | Runtime/lifecycle | 1 | Monkey-patch (test) | GAP-46 |
| 2 | UNRESOLVED_COLUMN | Column casing | 1 | Rename fix | Existing (Rule 22) |
| 3 | `entity_partners` 0 rows | UDF inlining | 2 | Dynamic config | GAP-47 |
| 4 | BOOLEAN vs INT union | Type safety | 2 | Cast alignment | Existing (GAP-05) |
| 5 | Q0 dedup doubling | Translation logic | 6 | Remove union | GAP-48 |
| 6 | SQL bug replication | Semantic parity | 7 | Replicate bug | GAP-49 |
| 7 | Connect hanging | Infrastructure | 3 | Internal checkpoints | Existing (GAP-12) |
| 8 | Empty transfers_adj | Expected behavior | N/A | None needed | None |

**Key takeaway:** The hardest bug to find (#6) was a SQL bug in the source SP that the PySpark conversion "fixed" — producing different results. Always compare SQL output before assuming PySpark logic is wrong. When SQL has a no-op block due to a bug, replicate the no-op.
