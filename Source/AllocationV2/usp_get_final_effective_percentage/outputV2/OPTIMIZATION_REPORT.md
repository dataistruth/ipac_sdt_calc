# Final Effective Percentage outputV2

This is an isolated, conservative A/B candidate. Production `output/*.py`, the
existing `output/updated` candidate, and the `sdt_d` baseline remain read-only.

## Schedule compatibility

The public entry point remains `run_final_effective_percentages`; compatibility
aliases `run_mode` and `run_modes` are also preserved. Existing schedule
configuration changes only the module path:

```text
AllocationV2.usp_get_final_effective_percentage.output.orchestrator
AllocationV2.usp_get_final_effective_percentage.outputV2.orchestrator
```

The method name and SP name do not change.

## Dependency and risk map

- `run_final_effective_percentages` delegates to the unchanged private production
  orchestrator; `run_modes` owns config, shared phases, fused mode 1/2/3 work,
  standalone mode 4, validations, saves, and return shaping.
- Shared production helpers are imported by that private orchestrator from its
  parent `output` package. They are not duplicated in `outputV2`.
- `cfg["mode"]` and `cfg["_current_mode"]` are mutated across dependent phases.
  Mode-specific inputs feed a fused CPBT/effective-calculation chain, and all
  variants write the same RunID partitions.
- The highest correctness risks are changing checkpoint relation qualifiers,
  reordering mode mutations, bypassing gating actions, or concurrently writing
  one RunID. This candidate changes none of those business operations.

## Exact candidate changes

1. Every production `_checkpoint` seam delegates to
   `Common_V2.core.checkpoint_V2.checkpoint_V2`.
2. `CheckpointMode` defaults to 2 and accepts either casing. Shared V2
   initialization occurs once in the wrapper before the first checkpoint.
3. Names beginning `final_cost_pct` always use explicit mode 1 (stats-off Delta)
   because local checkpoint relations are a proven self-join qualifier hazard.
4. Other seams use the configured mode. An actual local result is returned via
   `toDF(*columns)` to reproduce the fresh-relation behavior of a table read.
5. Production cleanup is replaced only in the private module with a no-op.
   Unique V2 names and common end-of-day cleanup own temporary-object hygiene.
6. Production builders are wrapped for detailed elapsed timing and opt-in shared
   plan profiling. BUILDER, CHECKPOINT, and ACTION reports are emitted when
   `ProfilePlan` is enabled; ACTION may be empty because production action sites
   are intentionally not copied or monkeypatched.
7. Every production checkpoint seam remains active. There is no
   `CheckpointProfile`, runtime bypass list, or checkpoint-profile widget.
8. No speculative collapsed checkpoints, post-builder checkpoints, CPBT input
   splits, experimental cost loader, caching, or helper substitutions are active.

## Parallelism decision

`max_threads` and `MaxThreads` normalize to 1..4 and the effective value is
reported. Four bounded groups contain only independent work:

1. `common_dimensions`: the applicable cost snapshot, entity-partner lookup,
   and asset-class relationship plan.
2. `common_inputs`: line items, book-effective data, quarters, and yearly data.
3. `lookthrough_metadata`: lookthrough input and footnote-line mapping.
4. `output_writes`: writes the three mode-specific results to distinct Delta
   tables.

Each task uses the same read-only configuration after config loading. Failures
are re-raised on the main thread. Mode preparation, shared `cfg` mutations,
checkpoint materialization order, fused CPBT/effective calculations, gating
validations, and per-mode output assembly remain sequential.

## Checkpoint safety rule

| Checkpoint name | Backend rule | Reason |
|---|---|---|
| `final_cost_pct*` | Explicit mode 1 / Delta | Proven localCheckpoint self-join qualifier hazard |
| Every other production seam | Configured mode 1..4 | Preserve seam order and shared V2 policy |
| Actual local result | `toDF(*columns)` | Mimic qualifier reset from a fresh table relation |

There is no hot-path cleanup and no `outputV2/checkpoint.py`.

The outputV2 checkpoint set therefore matches the production checkpoint set;
only the configured V2 backend and the `final_cost_pct*` Delta safety override
can differ.

## Output tables and acceptance metrics

| Table | RunID purge | Schema | Rows | Business sums | Row fingerprint |
|---|---:|---:|---:|---:|---:|
| `FinalEffectivePercentages` | yes | yes | yes | `EffPercentage`, `EffAmount` when present | order-independent |
| `FNFinalEffectivePercentages` | yes | yes | yes | same | order-independent |
| `SM_FinalEffectivePercentages` | yes | yes | yes | same | order-independent |

## Acceptance gate

Run `notebook/benchmark_final_effective_percentage.py` in Databricks with
`ExecutionOrder=alternate` and at least two passes. Accept only if all three
table fingerprints, schemas, counts, and sums match in both orders and measured
wall time improves beyond normal cluster variance. Profile separately with
`ProfilePlan=on`; leave it off for timing. The notebook displays benchmark
timings, parity, checkpoint activity, parallel task activity, ranked step
timings, and BUILDER/CHECKPOINT/ACTION profile tables. No local runtime
improvement is claimed.
