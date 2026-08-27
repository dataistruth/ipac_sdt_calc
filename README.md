# ipac-sdt-calc

Calculation monorepo for iPAC SDT Databricks — allocation, report, database domains plus embedded **platform** (Framework, Common, testing).

Part of the hybrid split from `iPACSCore_SDT_Databricks`:

| Repo | Role |
|------|------|
| [ipac_delta_sync](../ipac_delta_sync) | Lakeflow CDC ingestion, client JSON, `p_{client}_{n}` pipelines |
| **ipac-sdt-calc** (this repo) | Calc jobs, platform orchestration, allocation steps |
| iPACSCore_SDT_Databricks (monolith) | **deprecated** after migration |

## Layout

```
config/clients/           # calc-specific client params (ingest → ipac_delta_sync)
platform/
  framework/              # from Source/Framework
  common/                 # Common_V2 + common merged
  testing/                # UnitTest + test/unittest
domains/
  allocation/             # from AllocationV2 (grouped workflows)
  database/
  report/
docs/migration/         # inventory, deprecation schedule
```

## Deploy

**Jobs only** — no Lakeflow pipelines in this bundle.

```bash
databricks bundle deploy --select jobs.lookthrough_allocation
```

Ingestion pipelines deploy from `ipac_delta_sync`.

## UC contract

Reads raw data from `${var.uc_catalog}.{client_nm}_raw` (written by ipac_delta_sync).

See [docs/migration/INVENTORY.md](docs/migration/INVENTORY.md).
