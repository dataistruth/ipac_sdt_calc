"""Production amount builders with every checkpoint routed through V2."""

from __future__ import annotations

import functools
import types

from Common_V2.core.checkpoint_V2 import checkpoint_V2

from .parent import service_module

_production = service_module("amount_service")


def _with_v2_checkpoint(fn):
    """Clone a production function with isolated candidate globals."""
    candidate_globals = dict(fn.__globals__)
    candidate_globals["checkpoint"] = checkpoint_V2
    clone = types.FunctionType(
        fn.__code__,
        candidate_globals,
        fn.__name__,
        fn.__defaults__,
        fn.__closure__,
    )
    clone.__kwdefaults__ = fn.__kwdefaults__
    return functools.update_wrapper(clone, fn)


_build_shared = _with_v2_checkpoint(
    _production._build_sm_lt_input_and_state_lines
)
_build_k1 = _with_v2_checkpoint(_production.build_k1_amounts)
_build_ubti = _with_v2_checkpoint(_production.build_ubti_amounts)
_compute_state = _with_v2_checkpoint(
    _production.compute_state_mapped_amounts
)


def _build_sm_lt_input_and_state_lines(spark, cfg, mappings):
    return _build_shared(spark, cfg, mappings)


def build_k1_amounts(
    spark,
    cfg,
    mappings,
    k1_sp_detail,
    k1_sp_residual_detail,
    fed_lines,
    non_sp_fp,
):
    return _build_k1(
        spark,
        cfg,
        mappings,
        k1_sp_detail,
        k1_sp_residual_detail,
        fed_lines,
        non_sp_fp,
    )


def build_ubti_amounts(spark, cfg, fed_lines, non_sp_fp):
    return _build_ubti(spark, cfg, fed_lines, non_sp_fp)


def compute_state_mapped_amounts(
    spark,
    cfg,
    pruned_dm,
    pruned_ubti_dm,
    partner_alloc,
    total_input,
    partner_alloc_ubti,
    total_ubti_input,
):
    return _compute_state(
        spark,
        cfg,
        pruned_dm,
        pruned_ubti_dm,
        partner_alloc,
        total_input,
        partner_alloc_ubti,
        total_ubti_input,
    )


__all__ = [
    "_build_sm_lt_input_and_state_lines",
    "build_k1_amounts",
    "build_ubti_amounts",
    "compute_state_mapped_amounts",
]
