# iPACSCore_SDT_Databricks — Source/ inventory

Maps each top-level `Source/` folder from the monolith to a **target repo** and **tag** for the hybrid split.

| Source path | Tag | Target repo | Target path |
|-------------|-----|-------------|-------------|
| `Source/ingestion` | ingest | ipac_delta_sync | `src/ingestion/` (migrate) |
| `Source/import` | ingest | ipac_delta_sync | `src/import/` (migrate) |
| `Source/Framework` | platform | ipac-sdt-calc | `platform/framework/` |
| `Source/Common_V2` | platform | ipac-sdt-calc | `platform/common/` (merge) |
| `Source/common` | legacy | ipac-sdt-calc | deprecated → `platform/common/` |
| `Source/UnitTest` | platform | ipac-sdt-calc | `platform/testing/` |
| `Source/test/unittest` | legacy | ipac-sdt-calc | merge → `platform/testing/` |
| `Source/AllocationV2` | allocation | ipac-sdt-calc | `domains/allocation/` |
| `Source/allocation` | legacy | ipac-sdt-calc | **deprecate** after parity |
| `Source/database` | database | ipac-sdt-calc | `domains/database/` |
| `Source/report` | report | ipac-sdt-calc | `domains/report/` |
| `Source/Client/Script` | client | split | ingest overrides → ipac_delta_sync; calc → ipac-sdt-calc `config/clients/` |
| `Source/_migration_state` | platform | ipac-sdt-calc | `platform/migration_state/` |

## AllocationV2 workflow groups (for regrouping)

| Workflow | Example `usp_*` folders | Target under `domains/allocation/` |
|----------|-------------------------|-------------------------------------|
| lookthrough | `usp_sm_load_lookthrough_*`, `usp_add_lookthrough_*`, `usp_apply_*_lookthrough_*` | `lookthrough/` |
| k3 | `uspLoadK3*`, `usp_*_k3_*` | `k3/` |
| contributions | `usp_apply_contributions_*` | `contributions/` |
| allocation_detail | `usp_add_allocation_detail_step_*` | `allocation_detail/` |
| pfic | `usp_apply_pfic_*` | `pfic/` |
| rounding | `usp_apply_rounding`, `usp_apply_investment_level_rounding` | `shared/rounding/` |
| effective_pct | `usp_get_final_effective_percentage`, `usp_apply_effective_pct_*` | `shared/effective_pct/` |

## Tag definitions

- **ingest** — Lakeflow, JDBC, CT-mirror, writeback; deploy via ipac_delta_sync bundle (`pipelines.p_*`)
- **platform** — Framework, Common, pools, orchestrator, shared tests
- **allocation** — Tax calc stored-procedure Spark steps
- **database** — Entity hierarchy, run tracking jobs (domain data)
- **report** — Reporting notebooks/jobs
- **legacy** — Superseded by V2; schedule removal after parity tests
- **client** — Per-client scripts; split by ingest vs calc concern

See also: [source_folder_map.json](source_folder_map.json) (machine-readable).
