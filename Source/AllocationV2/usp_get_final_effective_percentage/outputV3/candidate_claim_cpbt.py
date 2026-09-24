"""Adapter for the proven direct candidate-claim CPBT implementation."""

from __future__ import annotations

def load_candidate_claim_builder():
    """Load the isolated candidate implementation only when requested.

    Keeping this lazy means baseline/action_lean deployments retain the exact
    production builder and do not import the experimental implementation.
    """
    from .candidate_claim_builder import build_cost_percentage_by_type

    return build_cost_percentage_by_type


__all__ = ["load_candidate_claim_builder"]
