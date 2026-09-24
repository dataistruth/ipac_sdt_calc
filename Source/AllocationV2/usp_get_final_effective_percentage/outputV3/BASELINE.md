# Production baseline and acceptance contract

Recorded production contract:

- RunID: `17376`
- EntityID: `4137`
- invocation: mode `0`, producing business modes `[1, 2, 3]`
- wall time: `181.893s`
- production-reported time: `172.0s`
- total rows: `79`
- output tables: `FinalEffectivePercentages`,
  `FNFinalEffectivePercentages`, `SM_FinalEffectivePercentages`

The exact fingerprint contract is equality with the production run from the
same alternating benchmark pass for every table. A fingerprint includes:

- sorted schema and data types;
- row count;
- decimal sums of `EffPercentage` and `EffAmount` when present;
- order-independent `xxhash64` sum, minimum, and maximum over all columns.

This deliberately does not freeze undocumented hash literals in source.
`notebook/benchmark_final_effective_percentage.py` captures and displays both
complete fingerprints and fails on any field mismatch. It also requires the
production side to retain 79 rows and all three tables, preventing a candidate
from passing by matching a drifted or incomplete write.

The notebook defaults to two passes and alternates order. A baseline run uses:

1. production, then outputV3;
2. outputV3, then production.

For a non-baseline `ExperimentID`, each pass also includes a fresh unchanged
outputV3 control (32 shuffle partitions). The notebook permits exactly one
candidate switch, checks both outputV3 runs against production, and evaluates
the candidate against its paired control. Promotion requires at least two
paired runs with median gain of at least 3 seconds and 5%. Final sub-50
acceptance requires five pairs, candidate median below 50 seconds, and no
candidate run above 55 seconds.

Before each variant it purges only RunID 17376. Before the benchmark it
snapshots that RunID from all output tables and restores it in `finally`.
Stage, checkpoint-policy, critical-action, parallel-wave,
execution-strategy, effective Spark configuration, and cfg-artifact merge
records are shown separately. Parallel activity must contain
`lt_nolt_branches`, `mode_prep`, `output_build`, and `output_writes` for a
non-empty modes 1/2/3 run with `MaxThreads > 1`.

No Databricks execution or runtime-performance claim is part of local
verification. Runtime acceptance requires a Databricks run with exact
fingerprint parity in both orders and timing reviewed against the recorded
baseline.
