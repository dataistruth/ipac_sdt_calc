"""Named execution-stage contracts for the outputV3 FEP wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet


class StageName(str, Enum):
    COMMON_READS = "common_reads"
    WITH_LT_BRANCH = "with_lt_branch"
    NO_LT_BRANCH = "no_lt_branch"
    MODE_PREP = "mode_prep"
    FUSED_CPBT = "fused_cpbt"
    FUSED_EFFECTIVE = "fused_effective"
    OUTPUT_BUILD = "output_build"
    OUTPUT_WRITE = "output_write"


@dataclass(frozen=True)
class StageContract:
    name: StageName
    production_functions: FrozenSet[str]
    parallel_safe: bool
    contract: str


STAGE_CONTRACTS = (
    StageContract(
        StageName.COMMON_READS,
        frozenset(
            {
                "load_config",
                "build_cost_percentage_snapshot_modes123",
                "build_cost_percentage_snapshot_mode4",
                "build_mode1_704c_pe_book_allocations",
                "build_entity_partners",
                "build_cost_underlying_types",
                "build_entity_hierarchy",
                "build_asset_class_relationship",
                "build_underlyings_combined",
                "load_allocation_rules",
                "load_line_items",
                "load_book_effective_data",
                "load_yearly_lines",
                "load_quarters",
                "load_yearly_data",
                "filter_asset_class_underlyings",
                "build_underlyings_hlevel_ordered",
                "build_lookthrough_input_modes14",
                "build_footnote_lines",
                "build_footnote_book_effective",
                "build_temp_cost_percentage",
                "build_underlying_mod",
            }
        ),
        True,
        "Read-only, mode-independent inputs; only proven independent groups run concurrently.",
    ),
    StageContract(
        StageName.WITH_LT_BRANCH,
        frozenset(
            {
                "build_all_underlyings_ordered",
                "build_input_lines",
                "compute_amount_based_allocation",
                "build_non_dated_entities",
                "build_dated_entities",
                "build_entity_underlyings",
            }
        ),
        True,
        "Production chain with look-through input; runs after shared dependencies exist.",
    ),
    StageContract(
        StageName.NO_LT_BRANCH,
        frozenset(
            {
                "build_all_underlyings_ordered",
                "build_input_lines",
                "compute_amount_based_allocation",
                "build_non_dated_entities",
                "build_dated_entities",
                "build_entity_underlyings",
            }
        ),
        True,
        "The same production chain with empty look-through input for modes 2/3, in an isolated cfg fork.",
    ),
    StageContract(
        StageName.MODE_PREP,
        frozenset(
            {
                "build_allocation_input",
                "build_sm_lookthrough_allocation_input",
                "build_lookthrough_allocation_input",
                "build_footnote_underlyings_ordered",
                "build_footnote_input_lines",
                "build_footnote_dated_entities",
                "compute_form199a_effective_percentage",
                "build_state_allocation_input",
                "build_state_entities",
                "load_transfers_adj_cost",
            }
        ),
        True,
        "Modes 1/2/3 prepare concurrently in isolated cfg forks; artifacts merge explicitly.",
    ),
    StageContract(
        StageName.FUSED_CPBT,
        frozenset(
            {
                "build_cost_percentage_by_type",
                "compute_missing_entities",
                "build_final_cost_percentage",
                "compute_minimum_quarter",
            }
        ),
        False,
        "One production fused cost-percentage-by-type chain preserves _mode isolation.",
    ),
    StageContract(
        StageName.FUSED_EFFECTIVE,
        frozenset(
            {
                "compute_effective_percentage_dated",
                "compute_effective_percentage_non_dated",
                "apply_plugging",
                "apply_type_id_update",
            }
        ),
        False,
        "One production fused effective and plugging chain preserves _mode isolation.",
    ),
    StageContract(
        StageName.OUTPUT_BUILD,
        frozenset({"build_final_output"}),
        True,
        "Mode-specific production output assembly runs in isolated cfg forks.",
    ),
    StageContract(
        StageName.OUTPUT_WRITE,
        frozenset({"_save_results"}),
        True,
        "Only distinct target tables may be written concurrently.",
    ),
)

FUNCTION_STAGE = {
    function_name: contract.name.value
    for contract in STAGE_CONTRACTS
    if contract.name is not StageName.NO_LT_BRANCH
    for function_name in contract.production_functions
}


def stage_contracts() -> list[dict]:
    """Return serializable contracts for profiles and structure tests."""
    return [
        {
            "name": item.name.value,
            "production_functions": sorted(item.production_functions),
            "parallel_safe": item.parallel_safe,
            "contract": item.contract,
        }
        for item in STAGE_CONTRACTS
    ]


__all__ = [
    "FUNCTION_STAGE",
    "STAGE_CONTRACTS",
    "StageContract",
    "StageName",
    "stage_contracts",
]
