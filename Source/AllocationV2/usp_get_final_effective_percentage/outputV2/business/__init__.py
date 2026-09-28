"""outputV2-owned optimized business helpers.

These modules are optimized copies of the production ``output`` business
helpers. They keep every performance change (gated behind ``_output_v3_*``
flags and optional keyword arguments) inside ``outputV2`` so that the
production ``output`` package can remain the pristine SQL conversion.

The optimized functions are bound onto the isolated production orchestrator
in :mod:`outputV2.orchestrator`, so the parallel pipeline calls these copies
while the production path in ``output`` is untouched.
"""
