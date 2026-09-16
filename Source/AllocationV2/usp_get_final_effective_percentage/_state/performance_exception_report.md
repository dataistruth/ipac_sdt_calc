# Performance Exception Report

**SP:** `uspGetFinalEffectivePercentage`  
**Complexity:** Score 23 / Very Complex (Hard) | **Volume:** X Large (>2M rows) | **SLA target:** 20s  
**Best achieved (estimated native):** ~65s (after Level 1 optimizations)  
**Measured via Databricks Connect:** 446s (gRPC overhead inflates checkpoints ~10x)

---

## SP Characteristics

| Metric | Value |
|---|---|
| SQL lines | 9,027 |
| Complexity score | 23 (Very Complex) |
| Temp tables in SQL | 110 |
| UDFs inlined | 5 |
| IF/ELSE blocks | 155 |
| Joins in PySpark | 57 |
| Output modules | 14 files |
| Functions | 55 |
| Checkpoints | 15 (8 orchestrator + 7 internal) |
| Modes | 4 (K1, Input Amounts, State, 704c) |

This SP is the single largest and most complex SP in the migration project. It exceeds the "Hard" tier by a wide margin — the SLA matrix was calibrated for typical Hard SPs (score 8–12, 500–2000 lines).

---

## Optimization History

| Level | Action | Estimated savings | Applied? |
|---|---|---|---|
| 1a | Added `F.broadcast()` on ~35 lookup table joins across 10 files (`ENU_UnderlyingType`, `ENU_LineType`, `ENU_DF_DataList`, `ENU_Event`, `ENU_RuleType`, `ENU_AllocationBy`, `ENU_AllocationPercentageType`, `ENU_AttributeType`, `Enu_AssetClass`, `VW_Entity`, `WORKFLOWSTATUS`, `GlobalMenu`, `ENU_GlobalMenuGroup`) | 5–15s at X Large volume | Yes |
| 1b | Filter pushdown verified — `RunID`, `EntityID`, `ClientID` filters applied at source reads | Already correct | N/A |
| 1c | Unnecessary sorts checked — all `.orderBy()` are inside window functions (necessary) | 0s | N/A |
| 1d | Column pruning — partial (some tables read full before select). Not applied due to risk of missing columns in downstream joins | 0–3s potential | Deferred |
| 1e | Checkpoint reduction reviewed — all 15 justified by deep lineage + multi-consumer patterns | 0s | N/A |
| 1f | Redundant actions — ~20 `.isEmpty()` checks needed for SQL branching logic fidelity | 0s | N/A |

**Level 1 total estimated savings: 5–15s**

Levels 2–3 were assessed but not applied:

| Level | Assessment | Reason not applied |
|---|---|---|
| 2a | Join reorder | Joins follow SQL SP order for correctness parity; reordering risks semantic changes |
| 2b | Pre-aggregate before join | No applicable pattern — joins feed downstream logic, not aggregation |
| 2c | Single-pass reads | Already consolidated — each table read once (config_loader batches 12 lookups) |
| 2d | Fuse checkpoint into write | No output writes — result is returned as DataFrame |
| 2e | Move `.isEmpty()` after checkpoint | Already done where possible |
| 3a | Spark UI analysis | Not yet available — requires native notebook execution |
| 3b | Rewrite hot function | `compute_effective_percentage_dated` (128s Connect / ~15s native) has 4 sequential dependent checkpoints — cannot be parallelized |
| 3c | Split pipeline | Single `run_mode()` call must return complete DataFrame — splitting would change the API contract |

---

## Root Cause Analysis

The SP performs a **sequential 5-phase pipeline** where each phase's output feeds the next:

```
Phase 1: Config (12 scalar lookups)
    ↓
Phase 2–3: CostPercentage_Snapshot → entity hierarchy → underlyings combined
    ↓ (3 checkpoints — deep lineage, multi-consumer)
Phase 4: Cost % by type (3-tier matching: direct → parent → parent+TK)
    ↓ (3 internal checkpoints — each tier builds on prior)
Phase 5: Effective calc (dated + non-dated, transfer-adjusted, plugging)
    ↓ (4 internal checkpoints — transfer steps, pickup order, yearly, missing partners)
Output: Final assembly with type ID update
```

**15 checkpoints × ~3s each (native) = ~45s checkpoint I/O alone.**

The checkpoints cannot be reduced because:
1. Each checkpoint breaks DAG lineage on DataFrames with 3–7 upstream joins
2. 12 of 15 checkpointed DataFrames have 2+ downstream consumers
3. Without checkpoints, the full upstream DAG re-executes on each consumer — taking the pipeline from ~65s to >30 minutes (verified during Phase 6 debugging)

The remaining ~20s is split across:
- 12 config `.collect()`/`.first()` calls on small lookups (~3–5s)
- 57 join operations, all now broadcast-hinted where applicable (~10–12s)
- Window functions for ranking, plugging, dedup (~3–5s)

**Minimum theoretical execution time:** 15 checkpoints × 2s (best case) + 10s computation = **40s**, which exceeds the 20s target by 2x purely from I/O.

---

## Unit Test & Row Count Verification

| Check | Result |
|---|---|
| Unit tests | 24/24 PASS (49.8s local) |
| Row count match (Mode 1) | PySpark 1250 == SQL Server 1250 |
| Test parameters | RunID=2074, EntityID=144, ClientID=15349, TaxPeriodID=1 |
| SQL Server execution time | 26.46s |

---

## Options for Stakeholder Decision

### Option 1: Accept ~65s (Recommended)
Document as performance exception. Monitor for regression via `[TIMING]` logs. The 20s SLA was designed for typical Hard SPs (score 8–12). This SP scores 23 — it is fundamentally a different class of workload.

**Risk:** Low. The SP runs as a background job, not user-facing. 65s is still 97% faster than the full DAG re-execution without checkpoints (~30+ min).

### Option 2: Reduce checkpoints aggressively (to ~8)
Remove 7 checkpoints where consumer count is exactly 2 and lineage depth is borderline (3 joins). Re-execute those consumers from the prior checkpoint instead.

**Expected:** ~45s (saving ~21s from removed checkpoints). 
**Risk:** Medium. Some consumers may trigger longer re-execution than the checkpoint cost. Requires re-running Phase 6 row count validation.

### Option 3: Split into prep + query
Run Phases 1–4 as a prep job that writes intermediate tables. Run Phase 5 (effective calc) as the SLA-measured query job reading from those tables.

**Expected:** Phase 5 alone ~15–20s (within SLA).  
**Risk:** Low, but changes the execution model. Requires coordination between two jobs and intermediate table cleanup.

### Option 4: Reclassify SLA tier
Create a new "Extreme" complexity tier for SPs scoring >20 with an SLA of 90s.

**Expected:** 65s would be well within a 90s target.  
**Risk:** None. Honest acknowledgment that 9,027-line SPs are not comparable to 500-line SPs.
