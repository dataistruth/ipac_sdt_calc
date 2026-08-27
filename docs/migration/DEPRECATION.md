# Legacy deprecation schedule

Folders in `iPACSCore_SDT_Databricks/Source/` marked **legacy** in [INVENTORY.md](INVENTORY.md).

## Deprecation gates (all required before delete)

| Gate | Criteria |
|------|----------|
| Parity | New repo job produces same row counts / key metrics as monolith for pilot client |
| CI green | Allocation workflow tests pass in ipac-sdt-calc |
| Deploy cutover | Production runs from new bundle, not monolith |
| Sign-off | Allocation team approves lookthrough pilot |

## Schedule

| Path | Status | Target removal |
|------|--------|----------------|
| `Source/allocation` | deprecated | After AllocationV2 parity for affected workflows |
| `Source/common` | deprecated | After PLATFORM_MERGE checklist complete |
| `Source/test/unittest` | deprecated | After merge into `platform/testing/` |
| `Source/ingestion` | migrated | Remove after ipac_delta_sync ingest live |
| `Source/import` (ingest paths) | migrated | Same as ingestion |
| Full monolith `Source/` | archived | After all domains migrated |

## Archive strategy

1. Tag monolith: `archive/pre-sdt-split-YYYY-MM-DD`
2. README pointer to `ipac_delta_sync` + `ipac-sdt-calc`
3. No new features on monolith after Phase 1 ingest cutover

## Do not delete until

- [ ] `docs/migration/source_folder_map.json` all `migrate` → `completed`
- [ ] DEPRECATION sign-off recorded in release notes
