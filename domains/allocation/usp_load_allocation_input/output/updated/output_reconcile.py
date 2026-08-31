"""
Post-run output parity checks — row counts and optional Delta file bytes per RunID.

Used by notebooks/benchmark_load_allocation_input.py to compare original vs updated.
"""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Tables written by load_allocation_input (Delta + GenericResultStorer flow-ups)
OUTPUT_TABLES: tuple[str, ...] = (
    "AllocationInput",
    "PFICFootnoteFlowup",
    "PFICFootnoteFlowupWithTrackingKey",
    "Form926Flowup",
    "Form199AFlowup",
    "Form8865Flowup",
    "Form8886Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
    "Form200616Flowup",
)


def _fqn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _run_scope_filter(df, run_id: int, client_id: int, tax_period_id: int):
    cols = set(df.columns)
    out = df
    if "RunID" in cols:
        out = out.filter(F.col("RunID") == run_id)
    if "ClientID" in cols:
        out = out.filter(F.col("ClientID") == client_id)
    if "TaxPeriodID" in cols:
        out = out.filter(F.col("TaxPeriodID") == tax_period_id)
    return out


def _partition_file_bytes(spark: SparkSession, fqn: str, run_id: int) -> int | None:
    """Sum file sizes for this RunID partition when table_files is available."""
    try:
        files = spark.sql(f"SELECT * FROM table_files('{fqn}')")
        if "partition" not in files.columns:
            return None
        run_key = str(run_id)
        sized = files.filter(
            F.col("partition").getField("RunID").cast("string") == run_key
            | F.col("partition").getField("runid").cast("string") == run_key
        )
        if "size" in sized.columns:
            val = sized.agg(F.sum("size").alias("s")).first()["s"]
            return int(val or 0)
        if "size_bytes" in sized.columns:
            val = sized.agg(F.sum("size_bytes").alias("s")).first()["s"]
            return int(val or 0)
    except Exception:
        return None
    return None


def capture_output_metrics(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
    client_id: int,
    tax_period_id: int,
) -> list[dict[str, Any]]:
    """Row counts (+ amount sum, partition bytes) per output table for this run."""
    rows: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        fqn = _fqn(catalog, schema, table)
        entry: dict[str, Any] = {
            "table": table,
            "fqn": fqn,
            "exists": False,
            "row_count": 0,
            "amount_sum": None,
            "partition_bytes": None,
            "error": None,
        }
        try:
            if not spark.catalog.tableExists(fqn):
                entry["error"] = "table not found"
                rows.append(entry)
                continue
            entry["exists"] = True
            scoped = _run_scope_filter(
                spark.table(fqn), run_id, client_id, tax_period_id
            )
            entry["row_count"] = int(scoped.count())
            if "Amount" in scoped.columns:
                amt = scoped.agg(
                    F.sum(F.coalesce(F.col("Amount"), F.lit(0.0))).alias("s")
                ).first()["s"]
                entry["amount_sum"] = float(amt or 0.0)
            entry["partition_bytes"] = _partition_file_bytes(spark, fqn, run_id)
        except Exception as exc:
            entry["error"] = str(exc)
        rows.append(entry)
    return rows


def summarize_metrics(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    present = [m for m in metrics if m.get("exists") and not m.get("error")]
    tables_with_rows = sum(1 for m in present if m.get("row_count", 0) > 0)
    total_rows = sum(int(m.get("row_count") or 0) for m in present)
    total_bytes = sum(
        int(m["partition_bytes"])
        for m in present
        if m.get("partition_bytes") is not None
    )
    amount_tables = [m for m in present if m.get("amount_sum") is not None]
    total_amount = sum(float(m.get("amount_sum") or 0.0) for m in amount_tables)
    return {
        "tables_present": len(present),
        "tables_with_rows": tables_with_rows,
        "total_rows": total_rows,
        "total_partition_bytes": total_bytes if total_bytes else None,
        "total_amount_sum": total_amount if amount_tables else None,
    }


def purge_output_partitions_for_run(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: int,
) -> list[str]:
    """
    Delete this RunID from output tables before a benchmark variant.

    Required for fair A/B: write_form_flowups pass-through reads existing Form*Flowup /
    PFICFootnoteFlowup rows (lower-tier LTRunIDs). Without purge, the second variant
    sees the first variant's writes and can inflate pass-through row counts.
    """
    purged: list[str] = []
    run_id = int(run_id)
    for table in OUTPUT_TABLES:
        fqn = _fqn(catalog, schema, table)
        try:
            if not spark.catalog.tableExists(fqn):
                continue
            spark.sql(f"DELETE FROM {fqn} WHERE RunID = {run_id}")
            purged.append(table)
        except Exception:
            continue
    return purged


def format_mismatch_lines(compare_rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for row in compare_rows:
        if row.get("parity_ok"):
            continue
        name = row["table"]
        o_cnt = row.get("original_rows", 0)
        u_cnt = row.get("updated_rows", 0)
        delta = row.get("row_delta", 0)
        o_amt = row.get("original_amount_sum")
        u_amt = row.get("updated_amount_sum")
        amt_note = ""
        if o_amt is not None and u_amt is not None and not row.get("amount_match"):
            amt_note = f" amounts orig={o_amt} upd={u_amt}"
        lines.append(f"  {name}: orig={o_cnt} upd={u_cnt} delta={delta:+d}{amt_note}")
    return lines


def compare_variants(
    original: list[dict[str, Any]],
    updated: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per-table comparison original vs updated."""
    orig_by_table = {m["table"]: m for m in original}
    upd_by_table = {m["table"]: m for m in updated}
    compare_rows: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        o = orig_by_table.get(table, {})
        u = upd_by_table.get(table, {})
        o_cnt = int(o.get("row_count") or 0)
        u_cnt = int(u.get("row_count") or 0)
        o_bytes = o.get("partition_bytes")
        u_bytes = u.get("partition_bytes")
        o_amt = o.get("amount_sum")
        u_amt = u.get("amount_sum")
        row_match = o_cnt == u_cnt
        amount_match = (
            True
            if o_amt is None and u_amt is None
            else o_amt is not None
            and u_amt is not None
            and abs(float(o_amt) - float(u_amt)) < 0.01
        )
        bytes_match = (
            True
            if o_bytes is None or u_bytes is None
            else int(o_bytes) == int(u_bytes)
        )
        compare_rows.append(
            {
                "table": table,
                "original_rows": o_cnt,
                "updated_rows": u_cnt,
                "row_delta": u_cnt - o_cnt,
                "rows_match": row_match,
                "original_amount_sum": o_amt,
                "updated_amount_sum": u_amt,
                "amount_match": amount_match,
                "original_partition_bytes": o_bytes,
                "updated_partition_bytes": u_bytes,
                "partition_bytes_delta": (
                    (int(u_bytes) - int(o_bytes))
                    if o_bytes is not None and u_bytes is not None
                    else None
                ),
                "partition_bytes_match": bytes_match,
                "parity_ok": row_match and amount_match,
                "original_error": o.get("error"),
                "updated_error": u.get("error"),
            }
        )
    return compare_rows
