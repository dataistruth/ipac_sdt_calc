"""Per-table snapshot / restore / xxhash64 fingerprints for Development A/B."""

from __future__ import annotations

from pyspark.sql import functions as F


def _fqn(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _live_columns(spark, fqn):
    try:
        if not spark.catalog.tableExists(fqn):
            return []
        return list(spark.table(fqn).columns)
    except Exception:
        return []


def fingerprint(df):
    ordered = sorted(df.columns)
    if not ordered:
        return {"rows": 0, "hash_sum": 0, "hash_min": None, "hash_max": None}
    row_hash = F.xxhash64(
        *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in ordered]
    )
    row = (
        df.select(row_hash.alias("_h"))
        .agg(
            F.count("*").alias("rows"),
            F.sum(F.col("_h").cast("decimal(38,0)")).alias("hash_sum"),
            F.min("_h").alias("hash_min"),
            F.max("_h").alias("hash_max"),
        )
        .first()
    )
    return {
        "rows": int(row["rows"] or 0),
        "hash_sum": row["hash_sum"],
        "hash_min": row["hash_min"],
        "hash_max": row["hash_max"],
    }


def _key_frame(spark, catalog, schema, table, key, run_id):
    fqn = _fqn(catalog, schema, table)
    cols = _live_columns(spark, fqn)
    if not cols:
        return None, cols, False
    df = spark.table(fqn)
    if key not in cols:
        return df, cols, False
    return df.filter(F.col(key) == int(run_id)), cols, True


def snapshot_tables(spark, catalog, schema, tables, run_id):
    snaps = {}
    for table, key in tables:
        df, cols, keyed = _key_frame(spark, catalog, schema, table, key, run_id)
        if df is None:
            snaps[table] = None
            continue
        snaps[table] = df.cache()
        snaps[table].count()
    return snaps


def purge_run(spark, catalog, schema, tables, run_id):
    for table, key in tables:
        fqn = _fqn(catalog, schema, table)
        cols = _live_columns(spark, fqn)
        if key not in cols:
            continue
        spark.sql(f"DELETE FROM {fqn} WHERE {key} = {int(run_id)}")
        try:
            spark.catalog.refreshTable(fqn)
        except Exception:
            spark.sql(f"REFRESH TABLE {fqn}")


def restore_tables(spark, catalog, schema, tables, run_id, snaps, input_tables=()):
    input_set = set(input_tables)
    for table, key in tables:
        snap = snaps.get(table)
        fqn = _fqn(catalog, schema, table)
        cols = _live_columns(spark, fqn)
        if snap is None or key not in cols:
            continue
        if table in input_set:
            try:
                (
                    snap.writeTo(fqn)
                    .option("overwriteSchema", "true")
                    .overwrite(F.col(key) == int(run_id))
                )
                spark.catalog.refreshTable(fqn)
                continue
            except Exception:
                pass
        spark.sql(f"DELETE FROM {fqn} WHERE {key} = {int(run_id)}")
        if snap.take(1):
            snap.write.mode("append").option("overwriteSchema", "false").insertInto(
                f"{catalog}.{schema}.{table}"
            )
        try:
            spark.catalog.refreshTable(fqn)
        except Exception:
            spark.sql(f"REFRESH TABLE {fqn}")


def unpersist_snaps(snaps):
    for df in snaps.values():
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                pass


def hash_tables(spark, catalog, schema, tables, run_id):
    rows = {}
    for table, key in tables:
        df, cols, keyed = _key_frame(spark, catalog, schema, table, key, run_id)
        if df is None:
            rows[table] = {
                "table": table,
                "present": False,
                "keyed": False,
                "rows": None,
                "hash_sum": None,
                "hash_min": None,
                "hash_max": None,
            }
            continue
        fp = fingerprint(df)
        rows[table] = {
            "table": table,
            "present": True,
            "keyed": keyed,
            "rows": fp["rows"],
            "hash_sum": fp["hash_sum"],
            "hash_min": fp["hash_min"],
            "hash_max": fp["hash_max"],
        }
    return rows


def compare_hashes(original, updated):
    report = []
    all_ok = True
    for table, orig in original.items():
        upd = updated.get(table, {})
        match = (
            orig.get("present") == upd.get("present")
            and orig.get("rows") == upd.get("rows")
            and orig.get("hash_sum") == upd.get("hash_sum")
            and orig.get("hash_min") == upd.get("hash_min")
            and orig.get("hash_max") == upd.get("hash_max")
        )
        if orig.get("present") is False and upd.get("present") is False:
            match = True
        if not match:
            all_ok = False
        report.append(
            {
                "table": table,
                "exact_match": "PASS" if match else "FAIL",
                "orig_rows": orig.get("rows"),
                "upd_rows": upd.get("rows"),
                "orig_hash_sum": orig.get("hash_sum"),
                "upd_hash_sum": upd.get("hash_sum"),
            }
        )
    return all_ok, report
