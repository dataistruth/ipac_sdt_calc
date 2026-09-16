"""Compatibility entry for ordered shared-view registration."""

from .ai_shared_views import register_shared_views as _register_shared_views


def register_shared_views_parallel(spark, cfg, max_threads=None):
    """Preserve the public API without parallel session-catalog mutation."""
    del max_threads
    return _register_shared_views(spark, cfg)


register_shared_views = register_shared_views_parallel
