---
name: optimize-usp-apply-investment-level-rounding
description: Development outputV2 for uspApplyInvestmentLevelRounding. Same production services; Checkpoint V2 on production seams plus multi-consumer frames; parallel lookthrough vs partners and distinct post-summary writes.
---

# Optimize uspApplyInvestmentLevelRounding

Use only for `AllocationV2/usp_apply_investment_level_rounding`.

Production `output/` is the baseline. Import services via `parent.py`
(`config_service`, `lookthrough_service`, `input_service`,
`aggregation_service`, `partner_service`, `rounding_service`,
`write_service`). Do not copy bodies. Development: flat `outputV2/` (no
`business/`, `tests/`). Production: inline in `output/`.

Public entry: `apply_investment_level_rounding`. Orchestrator resolves
`ExecutionProfile` from `Common_V2.core.execution_profiles` (default
**low**). No AQE. Isolated `cfg = {**cfg}`.

## Phases

- `independent_early`: lookthrough load || partner snapshots
- `independent_builders`: allocation input || temp output || max type
- Sequential: UBTI/passive, rounding diff, rounding branch, `write_final_summaries`
- `output_writes`: IsRounded flag || AllocationSummary tables

Keep same-table writes sequential. CallFrom is None (not a widget).

## Notebook

Frozen 10 widgets. FEP identity 4137 / 17376 / QA7.
ResultType `deltalake`, VolumePath `/Volumes/qa7/datavolume/databrickdata`.
