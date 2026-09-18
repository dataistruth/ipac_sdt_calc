"""Mutation-safe production S8 clone using checkpoint_V2.

The business function's code object is reused with a private globals dictionary.
This preserves the sequential fixed-point rank loop while replacing only its
checkpoint dependency; the production module is never monkeypatched.
"""

from types import FunctionType

from Common_V2.core.checkpoint_V2 import checkpoint_V2

from .parent import output_module

_production = output_module("_country_rounding")
_globals = dict(_production.apply_country_level_rounding.__globals__)
_globals["checkpoint"] = checkpoint_V2
_implementation = FunctionType(
    _production.apply_country_level_rounding.__code__,
    _globals,
    name=_production.apply_country_level_rounding.__name__,
    argdefs=_production.apply_country_level_rounding.__defaults__,
    closure=_production.apply_country_level_rounding.__closure__,
)
_implementation.__kwdefaults__ = (
    _production.apply_country_level_rounding.__kwdefaults__
)


def apply_country_level_rounding(*args, **kwargs):
    return _implementation(*args, **kwargs)


__all__ = ["apply_country_level_rounding"]
