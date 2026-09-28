"""Per-table form flowup collect for outputV2."""

from __future__ import annotations

from Common_V2.core.helpers import read_table
import pyspark.sql.functions as F

from .parent import output_module

_final = output_module("ai_finalization_service")
_collect_result = _final._collect_result

FORM_FLOWUP_TABLES = (
    "Form926Flowup",
    "Form199AFlowup",
    "Form8886Flowup",
    "Form8865Flowup",
    "AtRiskFlowup",
    "CustomFootnoteFlowup",
)


def prepare_unblocked_footnotes(spark, cfg):
    """Register shared unblocked-footnote view once on the main thread."""
    run_id = cfg["run_id"]
    form926_lt = cfg.get("form926_line_type_id")
    form199a_lt = cfg.get("form199a_line_type_id")
    form8886_lt = cfg.get("form8886_line_type_id")
    lt_type_ids_list = [x for x in [form926_lt, form8886_lt, form199a_lt] if x]
    if not lt_type_ids_list:
        return
    (
        spark.table("_reclass_data")
        .filter(F.col("LineTypeID").isin(lt_type_ids_list))
        .select(
            "EntityID",
            "SourceEntityID",
            "FootnoteID",
            "LineTypeID",
            "ParentEntityID",
            "LTEntityID",
            "TrackingKey",
            "OriginalParentEntityID",
        )
        .distinct()
        .createOrReplaceTempView(f"_unblocked_footnotes_{run_id}")
    )


def collect_form_flowup_table(spark, cfg, k1_workflow_df, only_table: str) -> None:
    run_id = cfg["run_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    entity_id = cfg["entity_id"]
    fx_tid = cfg.get("fx_rate_transaction_id") or 0
    form926_lt = cfg.get("form926_line_type_id")
    form199a_lt = cfg.get("form199a_line_type_id")
    form8886_lt = cfg.get("form8886_line_type_id")
    form8865_lt = cfg.get("form8865_line_type_id")
    at_risk_lt = cfg.get("at_risk_line_type_id")
    line_9a_after = cfg.get("line_9a_after_line_id", 0)
    line_9a_before = cfg.get("line_9a_before_line_id", 0)

    # Register UnBlockedFootnotes view for non-allocated pass-through INSERTs
    # SQL line 1833: SELECT DISTINCT ... INTO #UnBlockedFootnotes FROM ReclassFootnoteAllocationData
    lt_type_ids_list = [x for x in [form926_lt, form8886_lt, form199a_lt] if x]
    reclass_data_df = spark.table("_reclass_data")
    unblocked_footnotes_df = None
    if only_table in {"Form926Flowup", "Form199AFlowup", "Form8886Flowup"} and lt_type_ids_list:
        unblocked_footnotes_df = spark.table(f"_unblocked_footnotes_{run_id}")

    # Helper DataFrames used across form flowups
    aiw_df = spark.table("_aiw")
    #k1_wf_df = spark.table(f"_k1_workflow_{run_id}")
    entity_df = spark.table("_entity")
    fx_avg_rate_df = spark.table("_fx_avg_rate")
    lower_tier_df = spark.table(f"_lower_tier_funds_{run_id}")

    # ─── Form 926 Flowup ──────────────────────────────────────────────────
    if form926_lt and only_table == "Form926Flowup":
        f926_snapshot = read_table(spark, "Form926Input_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        form926_transfer_date_line_id = cfg.get("form926_transfer_date_line_id", 0)
        has_transfer_date = (
            form926_transfer_date_line_id is not None
            and form926_transfer_date_line_id != 0
            and not read_table(spark, "Form926LineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id)
                & (F.col("TaxPeriodID") == tax_period_id)
                & (F.lower(F.col("ShortName")) == "transferdate")
                & (F.col("IsActive") == True)
            )
            .isEmpty()
        )

        # Build transfer date values per Form926ID.
        if has_transfer_date:
            f926_date_values = (
                f926_snapshot.alias("F926dv")
                .filter(F.col("F926dv.LineID") == form926_transfer_date_line_id)
                .join(k1_workflow_df.alias("AIWdv"),
                      F.col("F926dv.WorkflowID") == F.col("AIWdv.WorkflowID"), "left_semi")
                .select(F.col("F926dv.Form926ID"),
                        F.col("F926dv.TextValue").alias("TransferDateValue"),
                        F.col("F926dv.WorkflowID"))
            )
        else:
            f926_date_values = None

        alloc_run_for_fx = read_table(spark, "AllocationRun", cfg)
        run_type = cfg.get("run_type")
        phase_id = cfg.get("phase_id")
        max_success_fx_row = (
            alloc_run_for_fx
            .filter(
                (F.col("EntityID") == entity_id) &
                (F.col("ClientID") == client_id) &
                (F.col("TaxPeriodID") == tax_period_id) &
                (F.upper(F.col("RunStatus")) == "SUCCESS") &
                (F.col("RunType") == run_type) &
                (F.col("PhaseID") == phase_id)
            )
            .agg(F.max("RunID").alias("MaxRunID"))
            .first()
        )
        if max_success_fx_row and max_success_fx_row["MaxRunID"]:
            fx_run_row = (
                alloc_run_for_fx
                .filter(F.col("RunID") == max_success_fx_row["MaxRunID"])
                .select("ForeignCurrencyRateTransactionID")
                .first()
            )
            spot_fx_tid = fx_run_row["ForeignCurrencyRateTransactionID"] if fx_run_row else fx_tid
        else:
            spot_fx_tid = fx_tid

        fx_rate_tbl = read_table(spark, "ForeignCurrencyRate", cfg)

        # Join base flowup with date info
        base_f926 = (
            f926_snapshot.alias("F926")
            .join(k1_workflow_df.alias("AIW"), F.col("F926.WorkflowID") == F.col("AIW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("AIW.EntityID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
        )

        if f926_date_values is not None:
            any_various = not (
                f926_date_values
                .filter(F.lower(F.col("TransferDateValue")) == "various")
                .isEmpty()
            )

            if any_various:
                # 'Various' present anywhere → AverageRate for all rows.
                amount_expr = F.when(
                    F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
                ).otherwise(
                    F.round(F.col("F926.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                )
            else:
                # No 'Various' → date-based spot rate for all rows.
                date_value_concrete = (
                    f926_date_values
                    .filter(
                        (F.lower(F.col("TransferDateValue")) != "various")
                        & (F.coalesce(F.col("TransferDateValue"), F.lit("")) != "")
                    )
                    .groupBy(F.col("Form926ID"))
                    .agg(F.max(F.col("TransferDateValue")).alias("DateValue"))
                )
                # Form926Package / K1Package act as existence filters in the SQL spot-rate
                # branch. Filter by client/tax period (safe: Form926ID and K1PackageID are
                # unique to one client/tax period) to enable Delta data skipping.
                f926_package = (
                    read_table(spark, "Form926Package", cfg)
                    .filter(
                        (F.col("ClientID") == client_id)
                        & (F.col("TaxPeriodID") == tax_period_id)
                    )
                    .select(F.col("Form926ID"), F.col("K1PackageID"))
                )
                k1_package = (
                    read_table(spark, "K1Package", cfg)
                    .filter(F.col("TaxPeriodID") == tax_period_id)
                    .select(F.col("K1PackageID"))
                    .distinct()
                )
                spot_rate = (
                    fx_rate_tbl
                    .filter((F.col("TransactionID") == spot_fx_tid) & (F.col("ClientID") == client_id))
                    .groupBy("ClientID", "CurrencyCode", "TransactionID", "Range")
                    .agg(F.first("Rate", ignorenulls=True).alias("Rate"))
                )
                base_f926 = (
                    base_f926
                    .join(date_value_concrete.alias("DVC"),
                          F.col("DVC.Form926ID") == F.col("F926.Form926ID"), "inner")
                    .join(f926_package.alias("PKG"),
                          F.col("PKG.Form926ID") == F.col("F926.Form926ID"), "inner")
                    .join(k1_package.alias("K1P"),
                          F.col("K1P.K1PackageID") == F.col("PKG.K1PackageID"), "inner")
                    .join(
                        spot_rate.alias("SR"),
                        (F.col("SR.ClientID") == client_id) &
                        (F.col("SR.CurrencyCode") == F.col("E.CurrencyCode")) &
                        (F.col("SR.TransactionID") == spot_fx_tid) &
                        (F.col("SR.Range") == F.col("DVC.DateValue").cast("date")),
                        "left"
                    )
                )
                amount_expr = F.when(
                    F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
                ).otherwise(
                    F.round(F.col("F926.Amount") / F.when(
                        (F.upper(F.col("E.CurrencyCode")) == "USD") | (F.coalesce(F.col("E.CurrencyCode"), F.lit("")) == ""),
                        F.lit(1)
                    ).otherwise(F.coalesce(F.col("SR.Rate"), F.lit(1))), 0)
                )
        else:
            amount_expr = F.when(
                F.col("F926.LineID").isin(line_9a_after, line_9a_before), F.col("F926.Amount")
            ).otherwise(
                F.round(F.col("F926.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
            )

        df = base_f926.select(
            F.lit(run_id).cast("long").alias("RunID"),
            F.lit(client_id).cast("int").alias("ClientID"),
            F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
            F.lit(entity_id).cast("int").alias("EntityID"),
            F.lit(entity_id).cast("int").alias("FlowupEntityID"),
            F.col("AIW.EntityID").alias("SourceEntityID"),
            F.col("F926.Form926ID"), F.col("F926.LineID"),
            amount_expr.alias("Amount"),
            F.col("F926.TextValue"),
        )
        _collect_result(cfg, df, "Form926Flowup")

        # Reclass flowup
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form926_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form926ID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "Form926Flowup")

        # Non-allocated pass-through from existing lower-tier flowup
        f926_line_item = (
            read_table(spark, "Form926LineItem", cfg)
            .filter(
                (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
                ((F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()) &
                (F.col("IsActive") == True)
            )
        )
        f926_flowup = read_table(spark, "Form926Flowup", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_df = unblocked_footnotes_df.filter(F.col("LineTypeID") == form926_lt) if lt_type_ids_list else None
        if ub_df is not None:
            # Build suffix for LIKE match
            df = (
                f926_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f926_line_item.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_df.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form926ID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form926ID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form926Flowup")

    # ─── Form 199A Flowup ─────────────────────────────────────────────────
    if form199a_lt and only_table == "Form199AFlowup":
        f199a_snapshot = read_table(spark, "Form199AInput_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f199a_line_item = read_table(spark, "Form199ALineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) & (F.col("IsActive") == True)
        )
        df = (
            f199a_snapshot.alias("F199A")
            .join(k1_workflow_df.alias("KW"), F.col("F199A.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f199a_line_item.alias("FL"), F.col("F199A.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F199A.Form199AID"), F.col("F199A.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F199A.Amount"))
                .otherwise(F.round(F.col("F199A.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F199A.TextValue"),
            )
        )
        _collect_result(cfg, df, "Form199AFlowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form199a_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form199AID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "Form199AFlowup")

        # Non-allocated pass-through
        f199a_line_item_non_alloc = f199a_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f199a_flowup = read_table(spark, "Form199AFlowup", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_199a = unblocked_footnotes_df.filter(F.col("LineTypeID") == form199a_lt) if lt_type_ids_list else None
        if ub_199a is not None:
            df = (
                f199a_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f199a_line_item_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_199a.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form199AID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form199AID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form199AFlowup")

    # ─── Form 8865 Flowup ─────────────────────────────────────────────────
    if form8865_lt and only_table == "Form8865Flowup":
        f8865_snapshot = read_table(spark, "Form8865Input_Snapshot", cfg).withColumn(
            "Amount", F.expr("try_cast(Amount as double)")
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8865_line_item = read_table(spark, "Form8865LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) & (F.col("IsActive") == True)
        )
        # Direct from snapshot
        df = (
            f8865_snapshot.alias("F8865")
            .join(k1_workflow_df.alias("KW"), F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f8865_line_item.alias("FL"), F.col("F8865.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F8865.Form8865ID"), F.col("F8865.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F8865.TextValue"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # SchA from Form8865SchInput_Snapshot
        f8865_sch_snapshot = read_table(spark, "Form8865SchInput_Snapshot", cfg).withColumn(
            "Amount", F.expr("try_cast(Amount as double)")
        )
        f8865_sch_package = read_table(spark, "Form8865SchPackage", cfg)
        f8865_line_all = read_table(spark, "Form8865LineItem", cfg).filter(
            (F.col("IsActive") == True) & (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8865_sch_snapshot.alias("F8865")
            .join(f8865_sch_package.alias("P"), F.col("P.SchID") == F.col("F8865.SchID"), "inner")
            .join(k1_workflow_df.alias("KW"), F.col("F8865.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(f8865_line_all.alias("FL"), F.col("F8865.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("P.Form8865ID"), F.col("F8865.LineID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("F8865.Amount"))
                .otherwise(F.round(F.col("F8865.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("F8865.TextValue"), F.col("F8865.SchID"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # Reclass from Form8865AllocationSummary
        f8865_alloc_summary = read_table(spark, "Form8865AllocationSummary", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8865_line_alloc = f8865_line_item.filter(F.col("IsAllocated") == True)
        df = (
            f8865_alloc_summary.alias("F8865")
            .join(
                lower_tier_df.alias("LT"),
                (F.col("F8865.RunID") == F.col("LT.RunID")) &
                (F.col("F8865.EntityID") == F.col("LT.EntityID")) &
                (F.col("F8865.PartnerNumber") == F.col("LT.PartnerNumber")),
                "inner"
            )
            .join(f8865_line_alloc.alias("line"), F.col("line.LineID") == F.col("F8865.LineID"), "inner")
            .groupBy(
                F.col("LT.EntityID").alias("FlowupEntityID"),
                F.col("F8865.SourceEntityID"), F.col("F8865.Form8865ID"),
                F.col("F8865.LineID"), F.col("F8865.SchID")
            )
            .agg(F.sum("F8865.FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("FlowupEntityID"), F.col("SourceEntityID"),
                F.col("Form8865ID"), F.col("LineID"), F.col("Amount"),
                F.lit(None).cast("string").alias("TextValue"), F.col("SchID"),
            )
        )
        _collect_result(cfg, df, "Form8865Flowup")

        # Non-allocated pass-through
        f8865_line_non_alloc = f8865_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f8865_flowup = read_table(spark, "Form8865Flowup", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8865_flowup.alias("fl")
            .join(
                lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                "inner"
            )
            .join(f8865_line_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("ltf.EntityID").alias("FlowupEntityID"),
                F.col("fl.SourceEntityID"), F.col("fl.Form8865ID"), F.col("fl.LineID"),
                F.col("fl.Amount"), F.col("fl.TextValue"), F.col("fl.SchID"),
            )
            .distinct()
        )
        _collect_result(cfg, df, "Form8865Flowup")

    # ─── Form 8886 Flowup ─────────────────────────────────────────────────
    if form8886_lt and only_table == "Form8886Flowup":
        f8886_snapshot = read_table(spark, "Form8886Input_Snapshot", cfg).withColumn(
            "_NumericAmount", F.expr("try_cast(TextValue as double)")
        ).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        f8886_line_item = read_table(spark, "Form8886LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        df = (
            f8886_snapshot.alias("F8886")
            .join(f8886_line_item.alias("FL"), F.col("F8886.LineID") == F.col("FL.LineID"), "inner")
            .join(k1_workflow_df.alias("KW"), F.col("F8886.WorkflowID") == F.col("KW.WorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("F8886.Form8886ID"), F.col("F8886.LineID"),
                F.when(F.col("FL.IsAllocated") == True,
                    F.round(F.col("F8886._NumericAmount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                ).otherwise(
                    F.round(F.col("F8886.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0)
                ).alias("Amount"),
                F.col("F8886.TextValue"), F.col("F8886.TransactionName"),
                F.col("F8886.EntityID").alias("TransactionEntityID"),
                F.col("F8886.Comments"), F.col("F8886.SecIIComments"),
            )
        )
        _collect_result(cfg, df, "Form8886Flowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == form8886_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID",
                     "TextValue", "TransactionName", "TransactionEntityID",
                     "Comments", "SecIIComments")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("Form8886ID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
                F.col("TransactionName"), F.col("TransactionEntityID"),
                F.col("Comments"), F.col("SecIIComments"),
            )
        )
        _collect_result(cfg, df, "Form8886Flowup")

        # Non-allocated pass-through
        f8886_line_non_alloc = f8886_line_item.filter(
            (F.col("IsAllocated") == False) | F.col("IsAllocated").isNull()
        )
        f8886_flowup = read_table(spark, "Form8886Flowup", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        ub_8886 = unblocked_footnotes_df.filter(F.col("LineTypeID") == form8886_lt) if lt_type_ids_list else None
        if ub_8886 is not None:
            df = (
                f8886_flowup.alias("fl")
                .join(
                    lower_tier_df.filter(F.col("IsPficCfcQfcEntity") == False).alias("ltf"),
                    (F.col("ltf.RunID") == F.col("fl.RunID")) & (F.col("ltf.EntityID") == F.col("fl.EntityID")),
                    "inner"
                )
                .join(f8886_line_non_alloc.alias("lt"), F.col("lt.LineID") == F.col("fl.LineID"), "inner")
                .join(
                    ub_8886.alias("R"),
                    (F.col("R.LTEntityID") == F.col("ltf.EntityID")) &
                    (F.col("R.SourceEntityID") == F.col("fl.SourceEntityID")) &
                    (F.col("R.FootnoteID") == F.col("fl.Form8886ID")),
                    "inner"
                )
                .withColumn(
                    "_suffix",
                    F.when(
                        F.coalesce(F.col("R.OriginalParentEntityID"), F.lit(0)) == 0,
                        F.concat(F.lit("~"), F.col("fl.FlowupEntityID").cast("string"))
                    ).otherwise(
                        F.concat(F.col("fl.FlowupEntityID").cast("string"), F.lit("~"), F.col("ltf.EntityID").cast("string"))
                    )
                )
                .filter(F.col("R.TrackingKey").like(F.concat(F.lit("%"), F.col("_suffix"))))
                .select(
                    F.lit(run_id).cast("long").alias("RunID"),
                    F.lit(client_id).cast("int").alias("ClientID"),
                    F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                    F.lit(entity_id).cast("int").alias("EntityID"),
                    F.col("ltf.EntityID").alias("FlowupEntityID"),
                    F.col("fl.SourceEntityID"), F.col("fl.Form8886ID"), F.col("fl.LineID"),
                    F.col("fl.Amount"), F.col("fl.TextValue"),
                    F.col("fl.TransactionName"), F.col("fl.TransactionEntityID"),
                    F.col("fl.Comments"), F.col("fl.SecIIComments"),
                )
                .distinct()
            )
            _collect_result(cfg, df, "Form8886Flowup")

    # ─── At-Risk Flowup ───────────────────────────────────────────────────
    if at_risk_lt and only_table == "AtRiskFlowup":
        ar_snapshot = read_table(spark, "AtRiskInput_Snapshot", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
            (F.coalesce(F.col("Amount"), F.lit(0)) != 0)
        )
        k1_line_item_tbl = read_table(spark, "K1LineItem", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id) &
            (F.upper(F.col("LineDataType")) == "NUMBER") & (F.col("IsActive") == True)
        )
        map_k1 = read_table(spark, "MAP_K1LineItemLineType", cfg).filter(F.col("LineTypeID") == at_risk_lt)
        ar_package = read_table(spark, "AtRiskPackage", cfg)
        k1_package_df = spark.table("_k1_package")

        df = (
            ar_snapshot.alias("AR")
            .join(k1_line_item_tbl.alias("KL"), F.col("KL.LineID") == F.col("AR.LineID"), "inner")
            .join(map_k1.alias("M"), F.col("KL.LineID") == F.col("M.K1LineItemID"), "inner")
            .join(aiw_df.alias("KW"), F.col("AR.WorkflowID") == F.col("KW.ImportAtRiskWorkflowID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("KW.EntityID"), "inner")
            .join(ar_package.alias("P"), F.col("P.AtRiskID") == F.col("AR.AtRiskID"), "inner")
            .join(k1_package_df.alias("K1P"), F.col("K1P.K1PackageID") == F.col("P.K1PackageID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("KW.EntityID").alias("SourceEntityID"),
                F.col("AR.AtRiskID"), F.col("AR.LineID"),
                F.round(F.col("AR.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0).alias("Amount"),
                F.col("AR.TextValue"),
            )
        )
        _collect_result(cfg, df, "AtRiskFlowup")

        # Reclass
        df = (
            reclass_data_df
            .filter(F.col("LineTypeID") == at_risk_lt)
            .groupBy("LTEntityID", "SourceEntityID", "FootnoteID", "LineID", "TextValue")
            .agg(F.sum("FlowupAmount").alias("Amount"))
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTEntityID").alias("FlowupEntityID"),
                F.col("SourceEntityID"),
                F.col("FootnoteID").alias("AtRiskID"),
                F.col("LineID"), F.col("Amount"), F.col("TextValue"),
            )
        )
        _collect_result(cfg, df, "AtRiskFlowup")

    if only_table == "CustomFootnoteFlowup":
        # ─── Custom Footnote Flowup ───────────────────────────────────────────
        cf_input = read_table(spark, "CustomFootnoteInput", cfg).filter(
            (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        cf_txn_df = spark.table(f"_cf_latest_txn_{run_id}")
        cf_line_item = read_table(spark, "CustomFootnoteLineItem", cfg).filter(
            (F.col("IsActive") == True) & (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        )
        cf_line_item = cf_line_item.unionByName(
            spark.createDataFrame([(-1, "TEXT")], "LineID int, LineDataType string"),
            allowMissingColumns=True
        )

        df = (
            cf_input.alias("CF")
            .join(cf_txn_df.alias("TXN"), F.col("CF.CustomFootnoteTransactionID") == F.col("TXN.TransactionID"), "inner")
            .join(entity_df.alias("E"), F.col("E.EntityID") == F.col("TXN.EntityID"), "inner")
            .join(cf_line_item.alias("FL"), F.col("CF.LineID") == F.col("FL.LineID"), "inner")
            .join(fx_avg_rate_df.alias("R"), F.col("R.CurrencyCode") == F.col("E.CurrencyCode"), "left")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.lit(entity_id).cast("int").alias("FlowupEntityID"),
                F.col("TXN.EntityID").alias("SourceEntityID"),
                F.col("CF.CustomFootnoteID"), F.col("CF.LineID"),
                F.col("TXN.LineTypeID"),
                F.when(F.upper(F.col("FL.LineDataType")) == "PERCENT", F.col("CF.Amount"))
                .otherwise(F.round(F.col("CF.Amount") / F.coalesce(F.col("R.AverageRate"), F.lit(1)), 0))
                .alias("Amount"),
                F.col("CF.TextValue"),
            )
            .distinct()
        )
        _collect_result(cfg, df, "CustomFootnoteFlowup")

        # Pass-through from existing CustomFootnoteFlowup for lower-tier entities
        cff_existing = read_table(spark, "CustomFootnoteFlowup", cfg)
        df = (
            cff_existing.alias("CFF")
            .join(
                lower_tier_df.alias("LTF"),
                (F.col("LTF.RunID") == F.col("CFF.RunID")) & (F.col("LTF.EntityID") == F.col("CFF.EntityID")),
                "inner"
            )
            .join(cf_line_item.alias("FL"), F.col("FL.LineID") == F.col("CFF.LineID"), "inner")
            .select(
                F.lit(run_id).cast("long").alias("RunID"),
                F.lit(client_id).cast("int").alias("ClientID"),
                F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
                F.lit(entity_id).cast("int").alias("EntityID"),
                F.col("LTF.EntityID").alias("FlowupEntityID"),
                F.col("CFF.SourceEntityID"), F.col("CFF.CustomFootnoteID"),
                F.col("CFF.LineID"), F.col("CFF.LineTypeID"), F.col("CFF.Amount"), F.col("CFF.TextValue"),
            )
            .distinct()
        )
        _collect_result(cfg, df, "CustomFootnoteFlowup")

        # ─── Form 200616 Flowup ───────────────────────────────────────────────
        # f2006_snapshot = read_table(spark, "Form200616_Snapshot", cfg).filter(
        #     (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        # )
        # df = (
        #     f2006_snapshot.alias("F2006")
        #     .join(k1_wf_df.alias("KW"), F.col("F2006.WorkflowID") == F.col("KW.WorkflowID"), "inner")
        #     .select(
        #         F.lit(run_id).cast("long").alias("RunID"),
        #         F.lit(client_id).cast("int").alias("ClientID"),
        #         F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        #         F.lit(entity_id).cast("int").alias("EntityID"),
        #         F.lit(entity_id).cast("int").alias("FlowupEntityID"),
        #         F.col("KW.EntityID").alias("SourceEntityID"),
        #         F.col("F2006.Form2006EntityID"),
        #     )
        # )
        # _collect_result(cfg, df, "Form200616Flowup")

        # # Reclass from Form200616AllocationSummary
        # f2006_alloc = read_table(spark, "Form200616AllocationSummary", cfg).filter(
        #     (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
        # )
        # lt_funds = spark.table(f"_lower_tier_funds_{run_id}")
        # df = (
        #     f2006_alloc.alias("F2006")
        #     .join(
        #         lt_funds.alias("LT"),
        #         (F.col("F2006.RunID") == F.col("LT.RunID")) &
        #         (F.col("F2006.EntityID") == F.col("LT.EntityID")) &
        #         (F.col("F2006.PartnerNumber") == F.col("LT.PartnerNumber")),
        #         "inner"
        #     )
        #     .select(
        #         F.lit(run_id).cast("long").alias("RunID"),
        #         F.lit(client_id).cast("int").alias("ClientID"),
        #         F.lit(tax_period_id).cast("int").alias("TaxPeriodID"),
        #         F.lit(entity_id).cast("int").alias("EntityID"),
        #         F.col("LT.EntityID").alias("FlowupEntityID"),
        #         F.col("F2006.SourceEntityID"),
        #         F.col("F2006.Form2006EntityID"),
        #     )
        #     .distinct()
        # )
        # _collect_result(cfg, df, "Form200616Flowup")

