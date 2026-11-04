"""Common_V2.core — shared utilities for all converted SPs."""
try:
    from Common_V2.core.helpers import (
        read_table, tbl, tbl_name, ns, ns0, sql_round, safe_divide, table_prefix,
        get_logger, checkpoint, drop_checkpoints, log_section, log_timing,
    )
    from Common_V2.core.writers import (
        write_output, write_insert, write_delete_insert, write_merge, DualWriter,
    )
except ImportError:
    from core.helpers import (
        read_table, tbl, tbl_name, ns, ns0, sql_round, safe_divide, table_prefix,
        get_logger, checkpoint, drop_checkpoints, log_section, log_timing,
    )
    from core.writers import (
        write_output, write_insert, write_delete_insert, write_merge, DualWriter,
    )
