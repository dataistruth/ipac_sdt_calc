"""Optimized (updated) package for usp_add_lookthrough_alloc_detail_step01.

Preserves the production S1-S13 write semantics from the parent ``output``
module while adding:

* a single ``base_lt_out`` checkpoint (lineage break) shared by all 13 sections,
* a thread pool that plans the 13 independent section writes concurrently,
* the shared ``AllocationV2.plan_profiler`` (node/depth attribution per section),
* a local/delta checkpoint-backend switch + optional coalesce.

Business logic is imported unchanged from the parent ``output`` package so
outputs stay bit-for-bit identical (verified by the benchmark reconcile).
"""
