# usp_load_allocation_input optimization report

## Baseline

Source: unchanged production package at
`sdt_d/Source/AllocationV2/usp_load_allocation_input/output/`.

Public entry: `load_allocation_input.run_load_allocation_input`.

Ordered dependency chain:

1. Load scalar configuration, then register shared temp views.
2. Build hierarchy, lower-tier funds and workflow identifiers.
3. Run gating validation and warnings.
4. Build form inputs.
5. Build and union K1/related inputs.
6. Build the PFIC snapshot, elections, PFIC allocation rows and custom
   footnotes.
7. Build PFIC flowup, apply election deletes and Part V/VII flags.
8. Apply master-feed, blocker and distribution filters, then optional tags.
9. Build output DataFrames and write RunID-scoped output partitions.

Production checkpoint seams are preserved in this clean candidate:
`reclass_data`, `pfic_snapshot`, `alloc_input`, inner `base_flowup`,
`pfic_raw`, `pfic_flowup`, `alloc_filtered`, and conditional `alloc_tagged`.
No profiler recommendation was automatically converted into a new checkpoint.

Output tables reconciled by the benchmark include AllocationInput, both PFIC
flowup tables, Form 926/199A/8865/8886 flowups, AtRiskFlowup,
CustomFootnoteFlowup, and Form200616Flowup. Conditional PFIC alert tables do
not expose RunID and are therefore deliberately skipped by run-scoped purge
and fingerprint logic; deleting them by broader keys would not be benchmark
safe.

## Implemented candidate changes

- Parent-package imports preserve every unchanged production service.
- The PFIC flowup service is a production copy with only its checkpoint import
  redirected to the updated stats-off Delta implementation.
- Shared lookup/temp-view registration retains production order because it
  mutates the shared Spark session catalog.
- Validation and warning execution retains production order because it is an
  early-abort boundary and writes AllocationRunErrors.
- Output-DataFrame collectors retain production order because they register
  temp views and mutate a shared collector map.
- Writes to distinct flowup tables run concurrently, each with a separate
  result storer. AllocationInput remains a separate atomic RunID
  `replaceWhere` write.
- Builder, checkpoint and explicit orchestrator action profiling is opt-in.

Forms, K1 and custom-footnote construction are dependency-independent after
workflow setup, but their observed planning times were too small to justify an
additional pool. They remain sequential pending action-time evidence.

## Checkpoint evidence

The 2026-09-16 diagnostic run showed:

- `form_inputs`: 307 incoming nodes, 5.395 seconds to materialize.
- `distribution_line_suppression`: 320 nodes, 7.598 seconds.
- The immediately following `alloc_filtered` then had one node and still cost
  2.045 seconds.
- `pfic_flowup`: 25 nodes, 2.162 seconds.
- Updated wall time with the two speculative checkpoints and profiling enabled
  was 80.931 seconds versus 74.832 seconds production.

Conclusion: large plan growth alone did not justify either speculative
checkpoint. Both `form_inputs` and `distribution_line_suppression` additions
are absent from this rebuild. The production `alloc_filtered` and
`pfic_flowup` seams remain until a profiling-off, both-order A/B test proves an
individual removal.

`reclass_data` and `pfic_raw` previously appeared as low-node collapse
candidates. They are intentionally restored here because the skill requires a
clean production checkpoint baseline before changing one seam at a time.

## Benchmark acceptance

Run:

`output/updated/notebook/benchmark_load_allocation_input.py`

Use two passes with `ExecutionOrder=alternate`. Use `ProfilePlan=on` for
diagnosis, then repeat with `ProfilePlan=off` for performance acceptance.

Accept an optimization only when:

1. every output fingerprint matches,
2. both execution orders pass,
3. profiling-off wall time improves beyond run variance, and
4. the same checkpoint/backend configuration is used for each updated run.

Local validation can verify syntax/import surfaces, but Spark output parity and
runtime remain pending until the Databricks benchmark runs.
