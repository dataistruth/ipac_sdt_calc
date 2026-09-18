"""
Partner-related services (Sections 12, 15, 16).

- build_partner_snapshots: Partner_Snapshot and RoundingOverride_Snapshot.
- build_highest_percent_partner: FinalEffectivePercentages-based highest percent.
- build_nocost_partner: NoCost fallback partner identification.
"""

from pyspark.sql import Window
import pyspark.sql.functions as F
import time

from Common_V2.core.helpers import tbl, ns
from Common_V2.core.observability import log_section, log_timing
from .plan_profiler import profile_action


# ---------------------------------------------------------------------------
# Section 12: build_partner_snapshots
# SQL lines: 557-580
# ---------------------------------------------------------------------------
def build_partner_snapshots(spark, cfg):
    """Build Partner_Snapshot and RoundingOverride_Snapshot."""
    log_section("build_partner_snapshots")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    partner_workflow_id = cfg.get("partner_workflow_id")
    partner_transaction_id = cfg.get("partner_transaction_id")
    rounding_override_workflow_id = cfg.get("rounding_override_workflow_id")

    wf_or_txn = partner_workflow_id if partner_workflow_id else partner_transaction_id

    partner_snap = tbl(spark, "Partner_Snapshot", cfg).alias("P").filter(
        (F.coalesce(F.col("P.WorkFlowID"), F.col("P.TransactionID")) == wf_or_txn) &
        (F.col("P.EntityID") == entity_id) &
        (F.col("P.ClientID") == client_id) &
        (F.col("P.TaxPeriodID") == tax_period_id)
    ).select(
        F.col("P.EntityID"), F.col("P.ClientID"), F.col("P.TaxPeriodID"),
        F.col("P.PartnerNumber"),
        F.coalesce(F.col("P.ShareClass"), F.lit("")).alias("ShareClass"),
        F.coalesce(F.col("P.Name1"), F.lit("")).alias("Name1"),
        F.coalesce(F.col("P.Name2"), F.lit("")).alias("Name2"),
        F.coalesce(F.col("P.Name3"), F.lit("")).alias("Name3"),
        F.col("P.GPorLP"),
    )

    rounding_override_df = None
    if rounding_override_workflow_id:
        ro_snap = tbl(spark, "RoundingOverride_Snapshot", cfg).alias("RO").filter(
            (F.col("RO.WorkFlowID") == rounding_override_workflow_id) &
            (F.col("RO.IsRoundingOverride") == True)
        )
        rounding_override_df = ro_snap.join(
            partner_snap.alias("PS"),
            F.col("PS.PartnerNumber") == F.col("RO.PartnerNumber"),
            "inner"
        ).select(
            F.col("RO.PartnerNumber"),
            F.col("PS.ShareClass"),
            F.col("PS.Name1"),
            F.col("PS.Name2"),
            F.col("PS.Name3"),
        )

    cfg["partner_snapshot_df"] = partner_snap
    cfg["has_rounding_override"] = rounding_override_df is not None and bool(
        profile_action(
            "build_partner_snapshots.override.head",
            rounding_override_df,
            lambda: rounding_override_df.head(1),
            cfg,
        )
    )

    log_timing("build_partner_snapshots", t0)
    return partner_snap, rounding_override_df


# ---------------------------------------------------------------------------
# Section 15: build_highest_percent_partner
# SQL lines: 751-830
# ---------------------------------------------------------------------------
def build_highest_percent_partner(spark, cfg, lookthrough_output_df):
    """Build HighestPercentPartner from FinalEffectivePercentages, QuarterDates, K1LineItem."""
    log_section("build_highest_percent_partner")
    t0 = time.time()

    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    run_id = cfg["run_id"]

    # @PEBookAllocation
    pe_book_df = tbl(spark, "Enu_AllocationLogic", cfg).filter(
        F.lower(F.col("AllocationTypeName")) == "pe book allocation"
    ).select("AllocationTypeId")
    pe_book_row = profile_action(
        "build_highest_percent_partner.pe_book.first",
        pe_book_df,
        pe_book_df.first,
        cfg,
    )
    pe_book_allocation = pe_book_row["AllocationTypeId"] if pe_book_row else None

    entity_alloc_type_df = tbl(spark, "Entity", cfg).filter(
        F.col("EntityID") == entity_id
    ).select("AllocationTypeId")
    entity_alloc_type = profile_action(
        "build_highest_percent_partner.entity_type.first",
        entity_alloc_type_df,
        entity_alloc_type_df.first,
        cfg,
    )

    if not entity_alloc_type or entity_alloc_type["AllocationTypeId"] != pe_book_allocation:
        log_timing("build_highest_percent_partner", t0)
        return None

    # FinalEffectivePercentages
    fe_df = tbl(spark, "FinalEffectivePercentages", cfg).filter(F.col("RunID") == run_id)
    fe_regular = fe_df.filter(F.coalesce(F.col("704cPercentageType"), F.lit("")) == "")
    fe_ordinary = fe_df.filter(F.coalesce(F.col("704cPercentageType"), F.lit("")) == "OrdinaryPercentage")
    fe_combined = fe_regular.unionByName(fe_ordinary)

    # Transfer By Date config (from pre-resolved cfg flag)
    transfer_by_date = (cfg.get("flag_transfer_by_date") or "").strip().upper() == "C"

    # QuarterDates
    if transfer_by_date:
        tax_period_df = tbl(spark, "TaxPeriod", cfg).filter(
            F.col("IsDefault") == True
        ).select("Year")
        tax_period_row = profile_action(
            "build_highest_percent_partner.tax_period.first",
            tax_period_df,
            tax_period_df.first,
            cfg,
        )
        tax_year = tax_period_row["Year"] if tax_period_row else None

        quarter_dates_df = tbl(spark, "QuarterDates", cfg).select(
            F.col("Quarter").alias("LookUpData"),
            F.when(F.year(F.col("StartDate")) == tax_year, F.month(F.col("StartDate")))
            .otherwise(F.lit(-1)).alias("LookUpValue"),
        )
    else:
        quarter_dates_df = tbl(spark, "ENU_DF_DataList", cfg).filter(
            F.lower(F.col("Category")) == "quartermonth"
        ).select(
            F.col("LookupData").alias("LookUpData"),
            F.col("LookUpValue"),
        )

    # K1LineItem
    k1_line_item = tbl(spark, "K1LineItem", cfg).filter(
        (F.col("ClientID") == client_id) & (F.col("TaxPeriodID") == tax_period_id)
    ).select("LineID", "IsTransactionDate", "TransactionDate")

    # Multi-table join
    lo = lookthrough_output_df.alias("L")
    k1 = k1_line_item.alias("K")
    qd = quarter_dates_df.alias("D")
    fe = fe_combined.alias("FE")

    lookup_value_expr = F.when(
        F.coalesce(F.col("K.IsTransactionDate").cast("int"), F.lit(0)) == 0,
        F.lit(-1) if transfer_by_date else F.lit(0)
    ).otherwise(F.month(F.col("K.TransactionDate")))

    highest_pct = lo.join(
        k1, F.col("K.LineID") == F.col("L.LineID"), "inner"
    ).join(
        qd, F.col("D.LookUpValue") == lookup_value_expr, "inner"
    ).join(
        fe,
        (F.when(F.coalesce(F.col("FE.TrackingKey"), F.lit("")) == "",
                F.col("L.TrackingKey"))
         .otherwise(F.coalesce(F.col("FE.TrackingKey"), F.lit(""))) == F.col("L.TrackingKey")) &
        (F.coalesce(F.col("L.EntityID"), F.lit(0)) == F.coalesce(F.col("FE.InvestmentID"), F.lit(0))) &
        (F.col("FE.Quarter") == F.col("D.LookUpData")) &
        (F.coalesce(F.col("FE.TypeId"), F.lit(0)) == F.coalesce(F.col("L.AllocationTypeID"), F.lit(0))) &
        (F.coalesce(F.col("L.Tag"), F.lit("")) == F.coalesce(F.col("FE.Tag"), F.lit(""))) &
        (F.col("L.PartnerNumber") == F.col("FE.PartnerNumber")),
        "inner"
    ).groupBy(
        F.col("L.PartnerNumber"), F.col("L.EntityID"), F.col("L.ParentEntityID"),
        F.col("L.LineID"), F.col("L.LineTypeID"), F.col("L.TrackingKey"),
        F.col("FE.TrackingKey").alias("FE_TrackingKey"),
        F.col("L.SuperParentEntityID"), F.col("L.Tag"),
        F.col("L.ShareClass"),
        F.coalesce(F.col("L.AdjustmentTypeID"), F.lit(0)).alias("AdjustmentTypeID"),
        F.col("FE.EffPercentage"),
    ).agg(
        F.lit(1).alias("_grp")
    ).select(
        F.col("L.EntityID").alias("EntityID"),
        F.col("L.ParentEntityID").alias("ParentEntityID"),
        F.col("L.PartnerNumber").alias("PartnerNumber"),
        F.col("L.LineID").alias("LineID"),
        F.col("L.LineTypeID").alias("LineTypeID"),
        F.col("L.ShareClass").alias("ShareClass"),
        F.col("AdjustmentTypeID"),
        F.coalesce(F.col("FE.EffPercentage"), F.lit(0.0)).alias("MaxPercent"),
        F.col("L.TrackingKey").alias("TrackingKey"),
        F.col("L.SuperParentEntityID").alias("SuperParentEntityID"),
        F.col("L.Tag").alias("Tag"),
    )

    w = Window.partitionBy(
        "SuperParentEntityID", "ParentEntityID", "EntityID",
        "LineTypeID", "LineID", "TrackingKey", "Tag"
    ).orderBy(F.col("MaxPercent").desc(), F.col("PartnerNumber").asc())

    ranked = highest_pct.withColumn("Rnk", F.rank().over(w))

    highest_pct_partner_df = ranked.filter(F.col("Rnk") == 1).select(
        "EntityID", "ParentEntityID", "PartnerNumber", "MaxPercent",
        "LineID", "LineTypeID", "ShareClass", "AdjustmentTypeID",
        "TrackingKey", "SuperParentEntityID", "Tag", "Rnk",
    )

    log_timing("build_highest_percent_partner", t0)
    return highest_pct_partner_df


# ---------------------------------------------------------------------------
# Section 16: build_nocost_partner
# SQL lines: 831-860
# ---------------------------------------------------------------------------
def build_nocost_partner(spark, cfg, lookthrough_output_df, highest_pct_partner_df, partner_snapshot_df):
    """Identify NoCostPartner and determine fallback rounding partner."""
    log_section("build_nocost_partner")
    t0 = time.time()

    run_id = cfg["run_id"]
    client_id = cfg["client_id"]

    # Fallback rounding partner: max ProRata from AllocationPercentage
    alloc_pct_df = tbl(spark, "AllocationPercentage", cfg).filter(F.col("RunID") == run_id)

    max_pct_df = alloc_pct_df.agg(
        F.max(F.coalesce(F.col("Prorata"), F.lit(0.0))).alias("MaxPct")
    )
    max_pct_row = profile_action(
        "build_nocost_partner.max_percentage.first",
        max_pct_df,
        max_pct_df.first,
        cfg,
    )
    max_percentage = max_pct_row["MaxPct"] if max_pct_row else 0.0

    rounding_partner_df = alloc_pct_df.alias("AP").join(
        partner_snapshot_df.alias("PS"),
        (F.col("AP.EntityID") == F.col("PS.EntityID")) &
        (F.col("AP.ClientID") == F.col("PS.ClientID")) &
        (F.col("AP.PartnerNumber") == F.col("PS.PartnerNumber")) &
        (ns(F.col("AP.ShareClass")) == ns(F.col("PS.ShareClass"))),
        "inner"
    ).filter(
        (F.col("AP.RunID") == run_id) &
        (F.col("AP.ClientID") == client_id) &
        (F.coalesce(F.col("AP.Prorata"), F.lit(0.0)) == max_percentage)
    ).orderBy(
        F.col("PS.Name1").asc(), F.col("PS.Name2").asc(), F.col("PS.Name3").asc()
    ).select(
        F.col("AP.PartnerNumber"), F.col("AP.ShareClass")
    )
    rounding_partner_row = profile_action(
        "build_nocost_partner.rounding_partner.first",
        rounding_partner_df,
        rounding_partner_df.first,
        cfg,
    )

    rounding_pn = rounding_partner_row["PartnerNumber"] if rounding_partner_row else None
    rounding_sc = rounding_partner_row["ShareClass"] if rounding_partner_row else ""

    # NoCostPartner: lines with no matching HighestPercentPartner
    distinct_lo = lookthrough_output_df.select(
        "EntityID", "ParentEntityID", "LineID", "LineTypeID",
        "AdjustmentTypeID", "TrackingKey", "SuperParentEntityID", "Tag", "AllocationTypeID"
    ).distinct()

    nocost_df = None
    if highest_pct_partner_df is not None:
        nocost_df = distinct_lo.alias("L").join(
            highest_pct_partner_df.alias("HPP"),
            (F.col("L.TrackingKey") == F.col("HPP.TrackingKey")) &
            (F.col("L.LineID") == F.col("HPP.LineID")) &
            (F.col("L.LineTypeID") == F.col("HPP.LineTypeID")),
            "left"
        ).filter(
            F.col("HPP.TrackingKey").isNull()
        ).select(
            F.col("L.EntityID"), F.col("L.ParentEntityID"),
            F.col("L.LineID"), F.col("L.LineTypeID"),
            F.col("L.AdjustmentTypeID"), F.col("L.TrackingKey"),
            F.col("L.SuperParentEntityID"), F.col("L.Tag"),
        ).distinct()

    log_timing("build_nocost_partner", t0)
    return nocost_df, rounding_pn, rounding_sc
