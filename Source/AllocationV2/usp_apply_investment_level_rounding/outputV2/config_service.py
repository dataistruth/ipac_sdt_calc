"""SP-specific config aliases and BookK1 transaction lookup.

All scalar config comes from Common_V2.load_common_config. This module:
  - Aliases the pre-resolved cfg keys to the legacy names that downstream
    service files in this SP read.
  - Computes the BookK1AdjustmentEnabled flag + the NotRoundedLines DataFrame
    (entity-scoped fact-table reads — not generic config).
"""

import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from Common_V2.core.helpers import read_table
from Common_V2.core.observability import log_section, log_timing
from .plan_profiler import profile_action


def load_sp_config(spark: SparkSession, cfg: dict) -> dict:
    """Alias common cfg scalars + compute SP-specific BookK1 derived values."""
    log_section("load_sp_config")
    t0 = time.time()

    # ── Aliases from load_common_config (preserve legacy SP key names) ──
    cfg["foreign_currency_rate_transaction_id"] = cfg.get("foreign_currency_rate_txn_id")
    cfg["is_investment_level_rounding"] = cfg.get("flag_investment_level_rounding_logic")
    cfg["passive_line_type_id"] = cfg.get("passive_income_line_type_id")
    cfg["box_jkl_line_type_id"] = cfg.get("boxjkl_line_type_id")
    cfg["book_k1_adjustment_line_type_id"] = cfg.get("book_k1_adjustments_line_type_id")
    cfg["rounding_logic"] = cfg.get("rounding_logic_selected")
    cfg["rounding_override_import"] = (
        (cfg.get("flag_rounding_override_import") or "").strip().upper() == "C"
    )

    # ── @MaxBookK1ImportTransactionID (entity-scoped fact data) ──
    # SQL L284-295
    book_k1_evt_id = cfg.get("event_type_id_import_book_k1_adjustments")
    excluded_status_ids = (cfg.get("workflow_status_ids_excluded") or []) + [0]

    max_txn_id = None
    if book_k1_evt_id is not None:
        max_transaction = (
            read_table(spark, "TransactionLog", cfg)
            .filter(
                (F.col("EntityID") == cfg["entity_id"])
                & (F.col("EventTypeID") == book_k1_evt_id)
                & (F.col("ClientID") == cfg["client_id"])
                & (F.col("TaxPeriodID") == cfg["tax_period_id"])
                & (~F.col("StatusID").isin(excluded_status_ids))
            )
            .agg(F.max("TransactionID").alias("max_txn"))
        )
        row = profile_action(
            "load_sp_config.max_transaction.first",
            max_transaction,
            max_transaction.first,
            cfg,
        )
        max_txn_id = row["max_txn"] if row else None

    book_k1_adj_import = (
        (cfg.get("flag_book_k1_adjustment_import") or "U").strip().upper() == "C"
    )
    book_k1_enabled = book_k1_adj_import and (max_txn_id or 0) != 0
    cfg["book_k1_adjustment_enabled"] = book_k1_enabled

    # ── #NotRoundedLines DataFrame (read once if BookK1 enabled) ──
    if book_k1_enabled:
        cfg["not_rounded_lines_df"] = (
            read_table(spark, "BookK1AdjustmentbyPartner", cfg)
            .filter(F.col("TransactionID") == max_txn_id)
            .select(
                F.lit(cfg["entity_id"]).alias("EntityID"),
                F.col("LineID"),
                F.lit(cfg["k1_line_type_id"]).alias("LineTypeID"),
            )
            .distinct()
        )
    else:
        cfg["not_rounded_lines_df"] = None

    log_timing("load_sp_config", t0)
    return cfg
