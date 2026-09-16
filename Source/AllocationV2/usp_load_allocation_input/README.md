# usp_load_allocation_input

Production orchestrator and `ai_*.py` services stay in the monolith:

`Source/AllocationV2/usp_load_allocation_input/output/`

All optimizations live only in:

`output/updated/`

```python
from AllocationV2.usp_load_allocation_input.output.updated.load_allocation_input import (
    run_load_allocation_input,
)
```

A/B notebook: `output/updated/notebooks/benchmark_load_allocation_input.py`
