"""Locked catalog of the 12 optimized AllocationV2 SPs.

Identities stay on each SP's own run / A/B notebook widgets.
"""

SP_SPECS = (
    {
        "name": "usp_load_allocation_input",
        "prod_notebook": "AllocationV2/usp_load_allocation_input/output/notebook/run_load_allocation_input",
        "dev_notebook": "AllocationV2/usp_load_allocation_input/outputV2/notebook/benchmark_load_allocation_input",
    },
    {
        "name": "usp_load_footnotes_allocation_to_output",
        "prod_notebook": "AllocationV2/usp_load_footnotes_allocation_to_output/output/notebook/run_load_footnotes_allocation_to_output",
        "dev_notebook": "AllocationV2/usp_load_footnotes_allocation_to_output/outputV2/notebook/benchmark_load_footnotes_allocation_to_output",
    },
    {
        "name": "usp_get_final_effective_percentage",
        "prod_notebook": "AllocationV2/usp_get_final_effective_percentage/output/notebook/run_get_final_effective_percentage",
        "dev_notebook": "AllocationV2/usp_get_final_effective_percentage/outputV2/notebook/benchmark_final_effective_percentage",
    },
    {
        "name": "usp_apply_investment_level_rounding",
        "prod_notebook": "AllocationV2/usp_apply_investment_level_rounding/output/notebook/run_apply_investment_level_rounding",
        "dev_notebook": "AllocationV2/usp_apply_investment_level_rounding/outputV2/notebook/benchmark_apply_investment_level_rounding",
    },
    {
        "name": "usp_add_allocation_summary",
        "prod_notebook": "AllocationV2/usp_add_allocation_summary/output/notebook/run_add_allocation_summary",
        "dev_notebook": "AllocationV2/usp_add_allocation_summary/outputV2/notebook/benchmark_add_allocation_summary",
    },
    {
        "name": "usp_load_k3_allocation_summary",
        "prod_notebook": "AllocationV2/usp_load_k3_allocation_summary/output/notebook/run_usp_load_k3_allocation_summary",
        "dev_notebook": "AllocationV2/usp_load_k3_allocation_summary/outputV2/notebook/benchmark_usp_load_k3_allocation_summary",
    },
    {
        "name": "usp_load_lookthrough_cost_alloc_to_output",
        "prod_notebook": "AllocationV2/usp_load_lookthrough_cost_alloc_to_output/output/notebook/run_load_lookthrough_cost_alloc",
        "dev_notebook": "AllocationV2/usp_load_lookthrough_cost_alloc_to_output/outputV2/notebook/benchmark_load_lookthrough_cost_alloc",
    },
    {
        "name": "usp_load_lookthrough_allocation_input",
        "prod_notebook": "AllocationV2/usp_load_lookthrough_allocation_input/output/notebook/run_load_lookthrough_allocation_input",
        "dev_notebook": "AllocationV2/usp_load_lookthrough_allocation_input/outputV2/notebook/benchmark_load_lookthrough_allocation_input",
    },
    {
        "name": "usp_load_lookthrough_footnote_effective_allocation_pct",
        "prod_notebook": "AllocationV2/usp_load_lookthrough_footnote_effective_allocation_pct/output/notebook/run_load_lt_footnote_effective_allocation_pct",
        "dev_notebook": "AllocationV2/usp_load_lookthrough_footnote_effective_allocation_pct/outputV2/notebook/benchmark_load_lt_footnote_effective_allocation_pct",
    },
    {
        "name": "usp_add_lookthrough_alloc_detail_step01",
        "prod_notebook": "AllocationV2/usp_add_lookthrough_alloc_detail_step01/output/notebook/run_add_lookthrough_allocation_detail_step01",
        "dev_notebook": "AllocationV2/usp_add_lookthrough_alloc_detail_step01/outputV2/notebook/benchmark_add_lookthrough_allocation_detail_step01",
    },
    {
        "name": "usp_sm_load_lookthrough_cost_allocation_to_output",
        "prod_notebook": "AllocationV2/usp_sm_load_lookthrough_cost_allocation_to_output/output/notebook/run_sm_load_lookthrough_cost_allocation_to_output",
        "dev_notebook": "AllocationV2/usp_sm_load_lookthrough_cost_allocation_to_output/outputV2/notebook/benchmark_sm_load_lookthrough_cost_allocation_to_output",
    },
    {
        "name": "usp_sm_load_lookthrough_effective_allocation_pct",
        "prod_notebook": "AllocationV2/usp_sm_load_lookthrough_effective_allocation_pct/output/notebook/run_sm_load_lt_effective_alloc_pct",
        "dev_notebook": "AllocationV2/usp_sm_load_lookthrough_effective_allocation_pct/outputV2/notebook/benchmark_sm_load_lt_effective_alloc_pct",
    },
)
