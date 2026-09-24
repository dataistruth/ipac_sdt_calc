"""Runtime-selectable outputV3 optimization profiles."""

from __future__ import annotations

PROFILES = {
    "baseline": {
        "read_optimizations": False,
        "reuse_lookthrough_input": False,
        "checkpoint_bypasses": frozenset(),
        "fused_mode_prep": False,
        "missing_entity_identity": False,
        "output_materialization": "off",
        "candidate_claim_cpbt": False,
    },
    "action_lean": {
        "read_optimizations": True,
        "reuse_lookthrough_input": True,
        "checkpoint_bypasses": frozenset(
            {
                "all_und_common_nolt",
                "input_lines_nolt",
                "entity_und_common_nolt",
                "uc_ordered_common",
            }
        ),
        "fused_mode_prep": False,
        "missing_entity_identity": True,
        "output_materialization": "shared",
        "candidate_claim_cpbt": False,
    },
    "aggressive": {
        "read_optimizations": True,
        "reuse_lookthrough_input": True,
        "checkpoint_bypasses": frozenset(
            {
                "all_und_common_nolt",
                "input_lines_nolt",
                "entity_und_common_nolt",
                "uc_ordered_common",
            }
        ),
        "fused_mode_prep": True,
        "missing_entity_identity": True,
        "output_materialization": "shared",
        "candidate_claim_cpbt": True,
    },
}


def resolve_optimization_profile(
    value: object,
    *,
    output_materialization: object | None = None,
    candidate_claim_cpbt: object | None = None,
) -> tuple[str, dict]:
    name = str(value or "baseline").strip().lower()
    if name not in PROFILES:
        raise ValueError(
            f"Unknown OptimizationProfile {value!r}; "
            f"expected one of {sorted(PROFILES)}"
        )
    options = dict(PROFILES[name])
    options["checkpoint_bypasses"] = set(options["checkpoint_bypasses"])
    if output_materialization is not None:
        materialization = str(output_materialization).strip().lower()
        if materialization not in {"off", "shared", "per_output"}:
            raise ValueError(
                "OutputMaterialization must be off, shared, or per_output"
            )
        options["output_materialization"] = materialization
    if candidate_claim_cpbt is not None:
        if isinstance(candidate_claim_cpbt, str):
            enabled = candidate_claim_cpbt.strip().lower() in {
                "1", "on", "true", "yes",
            }
        else:
            enabled = bool(candidate_claim_cpbt)
        options["candidate_claim_cpbt"] = enabled
    return name, options


__all__ = ["PROFILES", "resolve_optimization_profile"]
