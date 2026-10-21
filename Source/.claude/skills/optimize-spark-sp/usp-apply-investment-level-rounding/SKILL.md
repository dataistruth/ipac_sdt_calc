---
name: optimize-usp-apply-investment-level-rounding
description: Development outputV2 for uspApplyInvestmentLevelRounding. Same production services; profile checkpoint on production seams plus multi-consumer frames; parallel lookthrough vs partners and distinct post-summary writes.
---

# Optimize uspApplyInvestmentLevelRounding

Use only for `AllocationV2/usp_apply_investment_level_rounding`.

Production `output/` is the baseline. Development `outputV2/` is
self-contained: copy the `*_service.py` helpers into `outputV2/` and
import `...outputV2.*`. Never import `...output.*`. Lazy `__init__.py`
only. No `business/`, `tests/`. Production: inline in `output/`.

Public entry: `apply_investment_level_rounding`. Orchestrator resolves
`ExecutionProfile` from `Common_V2.core.execution_profiles` (default
`DEFAULT_EXECUTION_PROFILE` (**small**)). No AQE. Isolated `cfg = {**cfg}`.

## Phases

- `independent_early`: lookthrough load || partner snapshots
- `independent_builders`: allocation input || temp output || max type
- Sequential: UBTI/passive, rounding diff, rounding branch, `write_final_summaries`
- `output_writes`: IsRounded flag || AllocationSummary tables

Keep same-table writes sequential. CallFrom is None (not a widget).

## Notebook

Frozen 10 widgets. FEP identity 4137 / 17376 / QA7.
ResultType `deltalake`, VolumePath `/Volumes/qa7/datavolume/databrickdata`.
Hash sum is `sum(_h.cast("decimal(38,0)"))`, never `F.sum("_h")`. Last
display cells use the parent-skill locked `runtime_schema` /
`parity_schema` (`reported` can be None).

## Known bad

- `F.sum("_h")` hash_sum or schema-less `createDataFrame` display
