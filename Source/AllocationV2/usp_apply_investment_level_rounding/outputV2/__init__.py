"""Development outputV2 package. Lazy exports only."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path as _Path

_V2_DIR = _Path(__file__).resolve().parent
__path__ = [str(_V2_DIR)]


class _OutputV2Finder:
    """Load sibling .py files from this folder when Databricks misses them."""

    def __init__(self, pkg, directory):
        self.pkg = pkg
        self.directory = directory

    def find_spec(self, fullname, path=None, target=None):
        if fullname != self.pkg and not fullname.startswith(self.pkg + "."):
            return None
        if fullname == self.pkg:
            return None
        rest = fullname[len(self.pkg) + 1 :]
        parts = rest.split(".")
        file_py = self.directory.joinpath(*parts).with_suffix(".py")
        if file_py.is_file():
            return importlib.util.spec_from_file_location(fullname, file_py)
        pkg_dir = self.directory.joinpath(*parts)
        init_py = pkg_dir / "__init__.py"
        if init_py.is_file():
            return importlib.util.spec_from_file_location(
                fullname,
                init_py,
                submodule_search_locations=[str(pkg_dir)],
            )
        return None


def _install_v2_finder():
    pkg = __name__
    sys.meta_path[:] = [
        finder
        for finder in sys.meta_path
        if not (
            type(finder).__name__ == "_OutputV2Finder"
            and getattr(finder, "pkg", None) == pkg
        )
    ]
    sys.meta_path.insert(0, _OutputV2Finder(pkg, _V2_DIR))


_install_v2_finder()


__all__ = ['apply_investment_level_rounding']


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from . import apply_investment_level_rounding as _mod
    return getattr(_mod, name)
