"""
_helpers.py — Local helper re-exports for uspLoadLookThroughAllocationInput.

Imports from Common_V2/core/helpers.py. Keeps SP code concise.
"""

import sys
import os

try:
    _common_v2_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "Common_V2"))
except NameError:
    _common_v2_path = "/Workspace/Users/usa-kcalathoorumakan@deloitte.com/iPACSCore_SDT_Databricks/Source/Common_V2"
if _common_v2_path not in sys.path:
    sys.path.insert(0, _common_v2_path)

from core.helpers import (
    get_logger,
    table_prefix,
    tbl,
    tbl_name,
    ns,
    ns0,
    sql_round,
    checkpoint,
    drop_checkpoints,
    log_timing as _log_timing,
    log_section as _log_section,
)

logger = get_logger("load_lookthrough_allocation_input")


def log_timing(name, start):
    _log_timing(name, start, logger)


def log_section(name):
    _log_section(name, logger)
