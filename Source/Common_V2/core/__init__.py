"""Common_V2.core — shared utilities for converted SPs."""

# Shared defaults for bounded driver-side concurrency. Updated orchestrators
# may accept MaxThreads, but must never exceed this common safety cap.
DEFAULT_MAX_THREADS = 4
MAX_PARALLEL_THREADS = 4
