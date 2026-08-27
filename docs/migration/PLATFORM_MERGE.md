# Platform merge: Common_V2 + common

## Goal

Single package at `platform/common/` replacing duplicate trees:

| Monolith | Action |
|----------|--------|
| `Source/Common_V2` | **Primary** — migrate as base |
| `Source/common` | Merge unique modules; delete duplicates |

## Merge process

1. Copy `Source/Common_V2` → `platform/common/` (preserve package layout).
2. Diff `Source/common` vs `Common_V2` — import only modules not present in V2.
3. Run monolith test suite against merged tree (`platform/testing/`).
4. Update all `AllocationV2` imports from `Common_V2.*` / `common.*` → `platform.common.*` (or published whl `ipac_sdt_common`).
5. Delete `Source/common` on monolith after green CI.

## Framework relocation

| Monolith | Target |
|----------|--------|
| `Source/Framework` | `platform/framework/` |
| `Framework/payloads` | `platform/framework/payloads/` |
| Pool/policy shell scripts | `platform/framework/scripts/` |

Orchestrator reads workflow manifests such as `domains/allocation/lookthrough/pipeline.lookthrough.yml` instead of discovering folders ad hoc.

## Optional: publish wheel

If calc steps need imports outside repo layout on Databricks:

```toml
# pyproject.toml (future)
[project]
name = "ipac-sdt-platform-common"
```

Calc notebooks install wheel; bundle `artifacts` section builds it.

## Validation checklist

- [ ] All `validate_on_databricks.py` imports resolve
- [ ] Pool scripts run against dev workspace
- [ ] No remaining imports from `Source.common` in AllocationV2
- [ ] UnitTest + test/unittest consolidated under `platform/testing/`
