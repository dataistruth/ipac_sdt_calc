---
name: optimize-usp-load-lookthrough-footnote-effective-allocation-pct
description: Optimizes Development outputV2 for uspLoadLookThroughFootnoteEffectiveAllocationPercentage. Same production builders and write order; Checkpoint V2 on lt_output plus extra plan-break seams like footnotes. Sequential Output then Input writes. Use when generating or diagnosing this SP.
---

# Optimize uspLoadLookThroughFootnoteEffectiveAllocationPercentage

Use this skill only for
`AllocationV2/usp_load_lookthrough_footnote_effective_allocation_pct`.

Production `output/load_lt_footnote_effective_allocation_pct.py` is the
correctness baseline. `outputV2` imports those builders via `parent.py`
and only changes scheduling, Checkpoint V2, and packaging.

Public entry: `run_load_lt_footnote_effective_allocation_pct`.

## Logic identity (do not change)

Call production functions in this order:

1. `_load_sp_config` then SKIPPED if `RunStatus=FAIL`, AllocationTypeName
   not `PE Book Allocation`, or `register_type_id` empty.
2. `load_mappings` → `expand_parent_k1_mappings` (may `isEmpty`) →
   `build_distinct_mappings` → K1 `isEmpty` gate (`OK_NO_K1`).
3. `build_yearly_effective_pct` → `load_partners` →
   `load_final_effective_percentages` → `build_lt_allocation_output`.
4. `build_cost_effective_pct` → `build_book_effective_pct` →
   `unionByName` → `load_temp_allocation_input` → SKIPPED if empty.
5. `build_single_multi_alloc_type` → `build_k1_data_amounts` →
   `build_final_effective_pct` → `build_allocation_output`.
6. `write_allocation_output` then `update_allocation_input`.

Do not parallelize those steps. Parallel Output+Input writes produced a
row-count mismatch. Do not copy builder bodies into `outputV2/`.

## Checkpoint V2 seams (footnotes-style extras)

Keep production seam **`lt_output`**. Add extra local plan breaks the
same way footnotes adds `cost_snapshot` / `temp_alloc_input` /
`alloc_pass*` on top of production:

| Name | After | Role |
|---|---|---|
| `distinct_mappings` | K1 gate | fan-out to yearly, LT output, temp input, classify, K1 |
| `yearly_line_amounts` | yearly % (skip if None) | INSERT 3 of final % |
| `partners` | `load_partners` | output join |
| `fep` | FEP load | cost + book |
| `lt_output` | **production** | cost, book, K1 amounts |
| `temp_final_eff_pct` | cost∪book | classify (`isEmpty` + join) |
| `temp_alloc_input` | temp input load | empty gate + classify + final % + write join |
| `single_percent` | classify (skip if None) | K1 amounts + final % |
| `k1_amount_pct` | K1 amounts (skip if None) | final % INSERT 2 |
| `final_pct` | final % (skip if None) | output join |
| `alloc_output` | build output | Output write |
| `grouped_output` | build output | Input overwrite |

After a **local** Checkpoint V2 backend, `toDF(*columns)` like footnotes.
Do not drop `lt_output`. Checkpoint `distinct_mappings` **after** the
temp-input empty gate, not before. Gate `isEmpty` on the uncheckpointed
temp-input frame.

## Writes

Sequential: append `LookThroughAllocationOutput`, then overwrite
`LookThroughAllocationInput` for the RunID. Restore both tables before
each A/B variant (Output is also an input to §7). Restore with
`writeTo(...).overwrite(RunID)` plus `refreshTable` — never DELETE then
INSERT (Spark caches the empty post-DELETE scan; updated then SKIPPED
and fingerprints 0 Input rows while original wrote 24).

## Notebook widgets (frozen)

Exactly the parent-skill 10 widgets. Identity matches locked lookthrough
allocation input (QA job):

| # | Name | Default |
|---|---|---|
| 1 | `source_path` | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| 2 | `EntityID` | `4755` |
| 3 | `ClientID` | `15348` |
| 4 | `TaxPeriodID` | `1` |
| 5 | `RunID` | `18266` |
| 6 | `CatalogName` | `qa7` |
| 7 | `SchemaName` | `iPC_2025_QA7_15348` |
| 8 | `ExecutionProfile` | `low` |
| 9 | `number_of_runs` | `1` |
| 10 | `ProfilePlan` | `off` |

`removeAll()` in its own cell. Pass `ExecutionProfile` and `ProfilePlan`
only to updated. Original then updated. Restore Output and Input (do
not purge Output). Last display cell uses explicit Spark schemas.

Updated always returns a status dict (`elapsed_seconds`, `skip_reason`).

## Known bad

- Parallel `output_writes`
- Parallel yearly / partners / FEP / `lt_output` (`collect` / `isEmpty`)
- Purging Output before a variant
- Copying the production module into `outputV2/`
- Dropping `lt_output`
- FEP CPBT / footnotes `cost_snapshot` *rewrites* (extra **seams** are OK)
- Checkpoint `distinct_mappings` before `load_temp_allocation_input`
- A/B restore via `DELETE` + `INSERT` without `refreshTable`
