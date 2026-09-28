---
name: optimize-usp-load-lookthrough-cost-alloc
description: Optimizes Development outputV2 for uspLoadLookThroughCostAllocationToOutput. Same production builders; Checkpoint V2 extras (footnotes-style plan breaks that saved ~12s) plus sequential Output then Input writes. Use when generating or diagnosing this SP.
---

# Optimize uspLoadLookThroughCostAllocationToOutput

Use this skill only for
`AllocationV2/usp_load_lookthrough_cost_alloc_to_output`.

Production `output/` is the correctness baseline. `outputV2` imports
`_data_loading`, `_hierarchy`, `_allocation`, and the production
orchestrator helpers via `parent.py`. Do not copy those modules into
`outputV2/`. No `business/`.

Public entry: `run_load_lookthrough_cost_alloc`.

The **SP orchestrator** resolves `ExecutionProfile` from
`Common_V2.core.execution_profiles` at start. Default **low**. No AQE.

## Extra checkpoints (the ~12s footnotes pattern)

Production hierarchy already has V1 seams (`cost_underlyings`, loop
levels, `entity_hier_final`, `all_underlyings`). Production orchestrator
only adds **`alloc_output`** before dual writes.

Development **keeps `alloc_output`** and adds the same class of extra
plan breaks that cut ~12s on footnotes / LT footnote effective %:

| Name | After |
|---|---|
| `partners` | `load_partners` |
| `input_raw` | look-through input load |
| `distinct_mappings` | 704c-to-K1 mapping (skip if None) |
| `cost_percentages` | cost % load / 704c mapped cost |
| `all_underlyings_ordered` | rule-ordered underlyings |
| `book_effective` | book rules + footnote inheritance + Cost type rewrite |
| `line_items` | footnote-source re-add |
| `temp_alloc_input` | prepared look-through input |
| `fep` | FinalEffectivePercentages |
| `alloc_pass1` | by-amount output, or 704c pivot |
| `alloc_pass2` | remaining input after by-amount deduct, or 704c K1 line items |
| `alloc_pass3` | FEP with AMOUNT types removed (K1 path) |
| `alloc_pass4` | by-percentage output (K1 path) |
| `alloc_output` | **production** — union / 704c result before writes |
| `cost_all_snap` | cost snapshot used by parts 1–4 |
| `deal_hier_lvl_0` / `deal_hier_lvl_N` | recursive deal EntityRelationship walk |
| `deal_hier_acc_N` | growing union after each level |
| `deal_hierarchy` | after self-ref rows; feeds part 4 |
| `cost_underlyings` / `hierarchy_lvl_*` / `entity_hier_final` / `all_underlyings` | production recursive CTE loop, routed to V2 |

The deal-level walk in `_get_cost_percentage_details` had **no**
checkpoint. Each `isEmpty` rebuilt the union. That is the recursive DF
used across the cost-% parts. Break it every level (footnotes
`entity_levels`). Also swap `_hierarchy._checkpoint` to V2 for the
other recursive CTE (`build_entity_hierarchy`).

After a **local** Checkpoint V2 backend, `toDF(*columns)` like footnotes.
Do not drop `alloc_output`. Do not edit production `_hierarchy.py`.

## Logic identity

Same production call order: config → workflow/FEP skip → loads →
optional 704c-to-K1 map → hierarchy → book → prepare input → validate →
704c **or** by-amount (`isEmpty` gate) then subtract then by-percentage →
checkpoint `alloc_output` → write Output then update Input.

Do **not** parallelize Output append and Input overwrite (production
ThreadPool caused count mismatches on a sibling SP). Sequential writes.

SKIP when `RunStatus=FAIL` or no Cost/704c FEP rows (`load_workflow_ids`).

## Notebook defaults (same FEP Development run widgets)

`outputV2/notebook/benchmark_load_lookthrough_cost_alloc.py` uses the
same identity widgets and defaults as FEP
`benchmark_final_effective_percentage.py`:

| Widget | Default |
|---|---|
| source_path | `/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source` |
| EntityID | `4137` |
| ClientID | `15348` |
| TaxPeriodID | `1` |
| RunID | `17376` |
| CatalogName | `QA7` |
| SchemaName | `iPC_2025_QA7_15348` |
| ExecutionProfile | `low` |
| number_of_runs | `1` |
| ExecutionOrder | `alternate` |

No MaxThreads / shuffle / CheckpointMode / plan-profiler widgets: the
execution profile tier resolves them. LineType `K1 with Cost`,
RankForRule `0`, ResultType `deltalake`, and VolumePath are constants.
Put `dbutils.widgets.removeAll()` in its own cell (Databricks keeps old
values if widgets are recreated in the same cell).

Production `output/` uses flat `from _data_loading import ...`. Alias
`_data_loading`, `_hierarchy`, `_allocation` in `sys.modules` to the
package-qualified modules before importing production (notebook
`fresh_import` and `parent.register_flat_aliases`); do not edit
production imports.

Restore `LookThroughAllocationOutput` and `LookThroughAllocationInput`
before each variant (Output is also an input).

## Known bad

- Parallel Output + Input writes
- Dropping `alloc_output`
- Copying `_hierarchy` / `_allocation` into `outputV2/`
- Purging Output before a variant
- FEP CPBT rewrites
