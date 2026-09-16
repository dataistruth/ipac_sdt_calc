"""Sequential gates plus parallel warning computation and one Delta append."""

from __future__ import annotations

import importlib.util
import logging
import time
import uuid

import pyspark.sql.functions as F
from Common_V2.core.helpers import log_section, log_timing, table_prefix

from .parallel_helpers import run_parallel
from .parent import output_module

logger = logging.getLogger(__name__)


def _private_service():
    source = output_module("ai_validation_service")
    spec = importlib.util.spec_from_file_location(
        f"{source.__name__}__updated_{uuid.uuid4().hex}", source.__file__
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _warning_task(spark, cfg, fn_name, *args):
    service = _private_service()
    rows = []

    def capture(_spark, _cfg, message, error_type="Error"):
        rows.append((message, error_type))

    service._insert_run_error = capture
    getattr(service, fn_name)(spark, cfg, *args)
    return rows


def _append_messages(spark, cfg, messages):
    if not messages:
        return
    rows = [
        (cfg["run_id"], cfg["entity_id"], message, cfg.get("log_id", 0), kind)
        for message, kind in messages
    ]
    df = spark.createDataFrame(
        rows, ["RunID", "EntityID", "ErrorMessage", "LogID", "ErrororWarning"]
    )
    (
        df.withColumn("RunID", F.col("RunID").cast("long"))
        .withColumn("EntityID", F.col("EntityID").cast("int"))
        .withColumn("LogID", F.col("LogID").cast("int"))
        .write.format("delta").mode("append")
        .saveAsTable(f"{table_prefix(cfg)}.AllocationRunErrors")
    )


def run_validations(spark, cfg, lower_tier_df, max_threads=4) -> bool:
    log_section("run_validations")
    started = time.time()
    if str(cfg.get("run_status") or "").upper() == "FAIL":
        log_timing("run_validations", started)
        return False

    messages = []
    first = [
        ("tax_capital", lambda: _warning_task(
            spark, cfg, "_check_tax_capital_warning"
        )),
        ("extra_partners", lambda: _warning_task(
            spark, cfg, "_check_extra_partners_warning"
        )),
    ]
    for _, rows in run_parallel(first, max_threads, "validation_pre_gate_warnings"):
        messages.extend(rows)

    rounding_logic = cfg.get("rounding_logic")
    if rounding_logic and rounding_logic.lower() == "plugged to gp":
        service = _private_service()
        gp_exists = not (
            service._entity_partner_rows(spark, cfg)
            .filter(F.upper(F.coalesce(F.col("GPorLP"), F.lit(""))) == "G")
            .isEmpty()
        )
        if not gp_exists:
            messages.append((
                "GP Partner does not exist. Please select one of the Partner as GP.",
                "Error",
            ))
            _append_messages(spark, cfg, messages)
            log_timing("run_validations", started)
            return False

    warning_specs = [
        ("lower_tier_partner", "_check_lower_tier_partner_warnings", (lower_tier_df,)),
        ("multiple_partner", "_check_multiple_partner_flowup", (lower_tier_df,)),
        ("pcap_financial", "_check_pcap_financial_mismatch", ()),
        ("financial_entity", "_check_financial_partner_not_in_entity", ()),
        ("multiple_upper_tier", "_check_multiple_upper_tier_flowup", ()),
        ("unlinked_relationship", "_check_entity_relationship_unlinked", ()),
    ]
    tasks = [
        (
            name,
            lambda fn=fn, args=args: _warning_task(spark, cfg, fn, *args),
        )
        for name, fn, args in warning_specs
    ]
    for _, rows in run_parallel(tasks, max_threads, "validation_warnings"):
        messages.extend(rows)

    _append_messages(spark, cfg, messages)
    log_timing("run_validations", started)
    return True
