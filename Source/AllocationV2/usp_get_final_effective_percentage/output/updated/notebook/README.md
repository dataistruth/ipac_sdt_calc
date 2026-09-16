# Benchmark notebook

This folder is Databricks-only. It is not a Python package.

- Notebook: `benchmark_final_effective_percentage.py`
- Importable modules stay one level up in `output/updated/`

Sync as two destinations so `cost_pct_loader.py` never lands here:

1. Python modules → `.../output/updated`
2. This notebook → `.../output/updated/notebook`
