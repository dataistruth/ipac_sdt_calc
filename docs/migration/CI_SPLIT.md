# CI and bundle deploy split

Two repos, two bundles, **no overlapping resources**.

## ipac_delta_sync (ingestion)

| Item | Value |
|------|--------|
| Bundle name | `ipac-delta-sync` |
| Includes | `generated/bundle/*.yml` |
| Deploy scope | `pipelines.p_*` only |
| Variables | `uc_catalog`, `num_of_tables_in_pipeline` |
| Does **not** deploy | Calc jobs, pools, allocation notebooks |

```bash
cd ipac_delta_sync
./ipac-delta-sync generate
databricks bundle deploy --select pipelines.p_client_a_1,pipelines.p_client_a_2
```

## ipac-sdt-calc (calc + platform)

| Item | Value |
|------|--------|
| Bundle name | `ipac-sdt-calc` |
| Includes | `resources/jobs/*.yml` |
| Deploy scope | `jobs.*` (e.g. `jobs.lookthrough_allocation`) |
| Variables | `uc_catalog` (must match ingest repo) |
| Does **not** deploy | Lakeflow pipelines |

```bash
cd ipac-sdt-calc
databricks bundle deploy --select jobs.lookthrough_allocation
```

## Shared contract (no shared Python imports)

- **UC paths:** `{uc_catalog}.{client_nm}_raw` — ingest writes, calc reads
- **Optional:** thin contracts package for table names only (future)

## CI pipeline recommendation

| Repo | CI stages |
|------|-----------|
| ipac_delta_sync | `pytest`, `./ipac-delta-sync validate`, `./ipac-delta-sync generate`, bundle validate |
| ipac-sdt-calc | lint, workflow manifest validate, bundle validate (no pipeline generate) |

## Monolith (iPACSCore_SDT_Databricks)

After cutover: **disable** monolith bundle deploy in CI; archive only.
