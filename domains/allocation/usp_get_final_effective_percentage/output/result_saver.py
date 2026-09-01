"""
result_saver.py

Converts the result-saving logic from:
  - uspLoadCostEffectivePercentage (modes 1 & 4)
  - uspLoadFootnoteEffectivePercentage (mode 2)
  - usp_SM_LoadCostEffectivePercentage (mode 3)

Each function takes the raw result DataFrame from build_final_output() and
reshapes it into the target table schema with RankForRule, sentinel rows, etc.
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField
import logging

logger = logging.getLogger(__name__)


def _build_system_default_rules(spark: SparkSession, cfg: dict) -> DataFrame:
    """ENU_CustomAllocations WHERE AllocationType IN ('Cost','Asset Class','ProRata')"""
    catalog = cfg["catalog"]
    schema = cfg["schema"]
    return (
        spark.table(f"{catalog}.{schema}.ENU_CustomAllocations")
        .filter(F.col("AllocationType").isin("Cost", "Asset Class", "ProRata"))
        .select("AllocationTypeID", "AllocationType")
    )


def _add_rank_for_rule(df: DataFrame, system_rules: DataFrame) -> DataFrame:
    """Apply RankForRule CASE logic used by all 3 SPs:
    CASE
      WHEN D.AllocationTypeID IS NULL
           AND ISNULL(L.AllocationType,'') NOT IN ('ProRata','Default without Transfer Adj %')
      THEN 1
      WHEN D.AllocationType IN ('Cost','Asset Class') THEN 3
      ELSE 4
    END
    """
    joined = (
        df.alias("L")
        .join(
            system_rules.alias("D"),
            F.col("L.TypeId") == F.col("D.AllocationTypeID"),
            "left",
        )
        .withColumn(
            "RankForRule",
            F.when(
                F.col("D.AllocationTypeID").isNull()
                & (~F.coalesce(F.col("L.AllocationType"), F.lit("")).isin(
                    "ProRata", "Default without Transfer Adj %"
                )),
                F.lit(1),
            )
            .when(
                F.col("D.AllocationType").isin("Cost", "Asset Class"),
                F.lit(3),
            )
            .otherwise(F.lit(4)),
        )
        .select("L.*", "RankForRule")
    )
    return joined


def _ensure_columns(df: DataFrame) -> DataFrame:
    """Ensure EffAmount and other optional columns exist with correct types."""
    if "EffAmount" not in df.columns:
        df = df.withColumn("EffAmount", F.lit(None).cast("double"))
    # TypeId comes as LongType from pipeline but Delta tables have IntegerType
    if "TypeId" in df.columns:
        df = df.withColumn("TypeId", F.col("TypeId").cast("int"))
    return df


def _build_sentinel_row(spark, cfg, parent_df, line_type_id=None,
                        is_exclude_from_transfer=False, sentinel_overrides=None):
    """Build the sentinel row that every SP appends.

    SQL pattern (all load SPs):
      INSERT ... VALUES (@RunID, @EntityID, @EntityID, -1, -1, '-1',
                         0, NULL, NULL, -1, NULL, '', 0/1, NULL, 0, 2)
      EffAmount is NULL in the sentinel for all modes.

    Uses the parent DataFrame's schema to cast F.lit() values, avoiding
    Spark Connect's CANNOT_DETERMINE_TYPE error on None inference.

    Args:
        sentinel_overrides: dict of {column_name: value} to override defaults.
            Used for mode 2 which has non-standard sentinel values.
    """
    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]

    sentinel_values = {
        "RunID": run_id,
        "EntityID": entity_id,
        "InvestmentID": entity_id,
        "SourceLEID": -1,
        "LineID": -1,
        "PartnerNumber": "-1",
        "EffPercentage": 0.0,
        "AllocationType": None,
        "Quarter": None,
        "TypeId": -1,
        "TrackingKey": None,
        "Tag": "",
        "IsExcludefromTransfer": is_exclude_from_transfer,
        "EffAmount": 0.0,
        "AssetClassId": 0,
        "RankForRule": 2,
    }
    if line_type_id is not None:
        sentinel_values["LineTypeID"] = line_type_id

    # Apply mode-specific overrides (e.g., mode 2 sentinel)
    if sentinel_overrides:
        sentinel_values.update(sentinel_overrides)

    # Build a single sentinel row using the parent schema.
    # Use spark.createDataFrame so the sentinel is always produced even when
    # parent_df is empty (the old parent_df.limit(1).select(...) approach
    # silently returns 0 rows when the parent has no data).
    # Make all fields nullable — Spark Connect rejects None for non-nullable
    # IntegerType fields, but sentinel rows legitimately have NULL columns.
    nullable_schema = StructType([
        StructField(f.name, f.dataType, nullable=True) for f in parent_df.schema
    ])
    # Case-insensitive lookup — parent schema may use Trackingkey vs TrackingKey etc.
    sentinel_lower = {k.lower(): v for k, v in sentinel_values.items()}
    row_data = {}
    for field in parent_df.schema:
        row_data[field.name] = sentinel_lower.get(field.name.lower())

    return spark.createDataFrame([row_data], schema=nullable_schema)


def build_mode1_results(
    spark: SparkSession, cfg: dict, result_df: DataFrame,
) -> dict:
    """Mode 1: uspLoadCostEffectivePercentage (@is704c=0)

    Target tables:
      - FinalEffectivePercentages: K1 + Adjustment rows + BoxJKL rows + sentinel

    Returns: dict of {table_name: DataFrame}
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    k1_line_type_id = cfg["k1_line_type_id"]
    adjustment_line_type_id = cfg["adjustment_line_type_id"]
    box_jkl_line_type_id = cfg["box_jkl_line_type_id"]
    system_rules = _build_system_default_rules(spark, cfg)

    # Add RunID, EntityID, SourceLEID columns + ensure optional cols
    base = _ensure_columns(
        result_df
        .withColumn("RunID", F.lit(run_id))
        .withColumn("EntityID", F.lit(entity_id))
        .withColumn("SourceLEID", F.lit(-1))
    )

    # Part 1: K1 + Adjustment LineTypes → FinalEffectivePercentages
    k1_adj = base.filter(
        F.col("LineTypeID").isin(k1_line_type_id, adjustment_line_type_id)
    )

    # Part 2: BoxJKL LineType → FinalEffectivePercentages with RankForRule
    box_jkl = base.filter(F.col("LineTypeID") == box_jkl_line_type_id)
    box_jkl = _add_rank_for_rule(box_jkl, system_rules)

    # K1/Adj rows don't get RankForRule in the K1 insert (no LEFT JOIN)
    # but they keep GPPartnerReceivingCarry
    k1_adj = k1_adj.withColumn("RankForRule", F.lit(None).cast("int"))

    # Combine K1+Adj and BoxJKL
    fep = k1_adj.unionByName(box_jkl, allowMissingColumns=True)

    # Sentinel row for BoxJKL — SQL mode 1 uses 1 (TRUE) for IsExcludefromTransfer
    sentinel = _build_sentinel_row(spark, cfg, fep, box_jkl_line_type_id,
                                   is_exclude_from_transfer=True)
    fep = fep.unionByName(sentinel, allowMissingColumns=True)

    # Select final columns matching FinalEffectivePercentages table schema
    # 704cAllocationTypeID: pass through as-is from pipeline (NULL for non-704c
    # rows, populated for 704c rows). SQL load SP does no COALESCE on this column.
    fep = fep.select(
        "RunID", "EntityID", "InvestmentID", "SourceLEID",
        F.coalesce(F.col("LineID"), F.lit(-1)).alias("LineID"),
        "PartnerNumber", "EffPercentage", "AllocationType", "Quarter",
        F.col("TypeId").cast("int").alias("TypeId"),
        F.col("TrackingKey").alias("Trackingkey"), "Tag",
        "IsExcludefromTransfer", "EffAmount", "AssetClassId", "LineTypeID",
        "RankForRule",
        F.col("`704cAllocationTypeID`").cast("int").alias("704cAllocationTypeID"),
        F.col("`704cPercentageType`").alias("704cPercentageType"),
        "GPPartnerReceivingCarry",
    )

    return {"FinalEffectivePercentages": fep}


def build_mode4_results(
    spark: SparkSession, cfg: dict, result_df: DataFrame,
) -> dict:
    """Mode 4: uspLoadCostEffectivePercentage (@is704c=1)

    Target tables:
      - FinalEffectivePercentages: K1 + Adjustment rows + BoxJKL rows + sentinel
      - FNFinalEffectivePercentages: non-K1/non-Adj/non-BoxJKL where 704cPercentageType != ''

    Returns: dict of {table_name: DataFrame}
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    k1_line_type_id = cfg["k1_line_type_id"]
    adjustment_line_type_id = cfg["adjustment_line_type_id"]
    box_jkl_line_type_id = cfg["box_jkl_line_type_id"]
    system_rules = _build_system_default_rules(spark, cfg)

    base = _ensure_columns(
        result_df
        .withColumn("RunID", F.lit(run_id))
        .withColumn("EntityID", F.lit(entity_id))
        .withColumn("SourceLEID", F.lit(-1))
    )

    # ── FinalEffectivePercentages (same as mode 1) ──
    k1_adj = (
        base.filter(
            F.col("LineTypeID").isin(k1_line_type_id, adjustment_line_type_id)
        )
        .withColumn("RankForRule", F.lit(None).cast("int"))
    )

    box_jkl = base.filter(F.col("LineTypeID") == box_jkl_line_type_id)
    box_jkl = _add_rank_for_rule(box_jkl, system_rules)

    fep = k1_adj.unionByName(box_jkl, allowMissingColumns=True)
    sentinel = _build_sentinel_row(spark, cfg, fep, box_jkl_line_type_id,
                                   is_exclude_from_transfer=True)
    fep = fep.unionByName(sentinel, allowMissingColumns=True)

    # 704cAllocationTypeID: pass through as-is from pipeline. SQL load SP
    # does no COALESCE on this column.
    fep = fep.select(
        "RunID", "EntityID", "InvestmentID", "SourceLEID",
        F.coalesce(F.col("LineID"), F.lit(-1)).alias("LineID"),
        "PartnerNumber", "EffPercentage", "AllocationType", "Quarter",
        F.col("TypeId").cast("int").alias("TypeId"),
        F.col("TrackingKey").alias("Trackingkey"), "Tag",
        "IsExcludefromTransfer", "EffAmount", "AssetClassId", "LineTypeID",
        "RankForRule",
        F.col("`704cAllocationTypeID`").cast("int").alias("704cAllocationTypeID"),
        F.col("`704cPercentageType`").alias("704cPercentageType"),
        "GPPartnerReceivingCarry",
    )

    # ── FNFinalEffectivePercentages: 704c rows not in K1/Adj/BoxJKL ──
    fn_704c = base.filter(
        ~F.col("LineTypeID").isin(
            k1_line_type_id, adjustment_line_type_id, box_jkl_line_type_id
        )
        & (F.coalesce(F.col("`704cPercentageType`"), F.lit("")) != "")
    )

    fn_fep = fn_704c.select(
        "RunID", "EntityID", "InvestmentID", "SourceLEID",
        F.coalesce(F.col("LineID"), F.lit(-1)).alias("LineID"),
        "PartnerNumber", "EffPercentage", "AllocationType", "Quarter",
        F.col("TypeId").cast("int").alias("TypeId"),
        F.col("TrackingKey").alias("Trackingkey"), "Tag",
        "IsExcludefromTransfer", "LineTypeID",
        F.coalesce(F.col("AssetClassId"), F.lit(0)).alias("AssetClassID"),
        F.lit(2).alias("RankForRule"),
        F.col("`704cAllocationTypeID`").alias("704cAllocationTypeID"),
        F.col("`704cPercentageType`").alias("704cPercentageType"),
    )

    return {
        "FinalEffectivePercentages": fep,
        "FNFinalEffectivePercentages": fn_fep,
    }


def build_mode2_results(
    spark: SparkSession, cfg: dict, result_df: DataFrame,
) -> dict:
    """Mode 2: uspLoadFootnoteEffectivePercentage

    Target table:
      - FNFinalEffectivePercentages: all rows with RankForRule + sentinel

    Returns: dict of {table_name: DataFrame}
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    system_rules = _build_system_default_rules(spark, cfg)

    base = _ensure_columns(
        result_df
        .withColumn("RunID", F.lit(run_id))
        .withColumn("EntityID", F.lit(entity_id))
        .withColumn("SourceLEID", F.lit(-1))
    )

    ranked = _add_rank_for_rule(base, system_rules)

    fn_fep = ranked.select(
        "RunID", "EntityID", "InvestmentID", "SourceLEID",
        F.coalesce(F.col("LineID"), F.lit(-1)).alias("LineID"),
        "PartnerNumber", "EffPercentage", "AllocationType", "Quarter",
        F.col("TypeId").cast("int").alias("TypeId"),
        F.col("TrackingKey").alias("Trackingkey"), "Tag",
        "IsExcludefromTransfer", "LineTypeID",
        F.coalesce(F.col("AssetClassId"), F.lit(0)).alias("AssetClassID"),
        "RankForRule",
        F.lit(None).cast("int").alias("704cAllocationTypeID"),
        F.lit(None).cast("string").alias("704cPercentageType"),
    ).distinct()

    # Sentinel row — mode 2 SQL sentinel has non-standard values:
    # EffPercentage=-1, AllocationType='0', TypeId=NULL, TrackingKey='-1',
    # Tag=NULL, LineTypeID=0 ('' cast to INT), IsExcludefromTransfer=False
    sentinel = _build_sentinel_row(
        spark, cfg, fn_fep,
        sentinel_overrides={
            "EffPercentage": -1.0,
            "AllocationType": "0",
            "TypeId": None,
            "TrackingKey": "-1",
            "Tag": None,
            "LineTypeID": 0,
        },
    )
    fn_fep = fn_fep.unionByName(sentinel, allowMissingColumns=True)

    return {"FNFinalEffectivePercentages": fn_fep}


def build_mode3_results(
    spark: SparkSession, cfg: dict, result_df: DataFrame,
) -> dict:
    """Mode 3: usp_SM_LoadCostEffectivePercentage

    Target table:
      - SM_FinalEffectivePercentages: all rows with RankForRule + sentinel

    Returns: dict of {table_name: DataFrame}
    """
    run_id = cfg["run_id"]
    entity_id = cfg["entity_id"]
    system_rules = _build_system_default_rules(spark, cfg)

    base = _ensure_columns(
        result_df
        .withColumn("RunID", F.lit(run_id))
        .withColumn("EntityID", F.lit(entity_id))
        .withColumn("SourceLEID", F.lit(-1))
    )

    ranked = _add_rank_for_rule(base, system_rules)

    sm_fep = ranked.select(
        "RunID", "EntityID", "InvestmentID", "SourceLEID",
        F.coalesce(F.col("LineID"), F.lit(-1)).alias("LineID"),
        "PartnerNumber", "EffPercentage", "AllocationType", "Quarter",
        F.col("TypeId").cast("int").alias("TypeId"),
        F.col("TrackingKey").alias("Trackingkey"), "Tag",
        "IsExcludefromTransfer", "EffAmount",
        F.coalesce(F.col("AssetClassId"), F.lit(0)).alias("AssetClassID"),
        "RankForRule",
    )

    # Sentinel row — mode 3 sentinel has IsExcludefromTransfer=1 (TRUE) per SQL SP.
    sentinel = _build_sentinel_row(spark, cfg, sm_fep, is_exclude_from_transfer=True)
    sm_fep = sm_fep.unionByName(sentinel, allowMissingColumns=True)

    return {"SM_FinalEffectivePercentages": sm_fep}


def _build_sentinel_from_table(spark, cfg, target_table, line_type_id=None,
                               is_exclude_from_transfer=False,
                               sentinel_overrides=None):
    """Build a single sentinel row using the target Delta table's schema.

    Used when a mode is skipped (no input data) but the SQL wrapper SP
    still inserts a sentinel row into the target table.
    """
    catalog = cfg["catalog"]
    schema_name = cfg["schema"]
    table_schema = spark.table(f"{catalog}.{schema_name}.{target_table}").schema

    entity_id = cfg["entity_id"]
    run_id = cfg["run_id"]

    # Case-insensitive lookup dict for sentinel values
    sentinel_values = {
        "runid": run_id,
        "entityid": entity_id,
        "investmentid": entity_id,
        "sourceleid": -1,
        "lineid": -1,
        "partnernumber": "-1",
        "effpercentage": 0.0,
        "allocationtype": None,
        "quarter": None,
        "typeid": -1,
        "trackingkey": None,
        "tag": "",
        "isexcludefromtransfer": is_exclude_from_transfer,
        "effamount": 0.0,
        "assetclassid": 0,
        "rankforrule": 2,
    }
    if line_type_id is not None:
        sentinel_values["linetypeid"] = line_type_id

    # Apply mode-specific overrides (e.g., mode 2 sentinel)
    if sentinel_overrides:
        for k, v in sentinel_overrides.items():
            sentinel_values[k.lower()] = v

    row_data = {}
    for field in table_schema:
        row_data[field.name] = sentinel_values.get(field.name.lower())

    nullable_schema = StructType([
        StructField(f.name, f.dataType, nullable=True) for f in table_schema
    ])
    return spark.createDataFrame([row_data], schema=nullable_schema)


def build_all_results(
    spark: SparkSession, cfg: dict, results: dict,
    all_requested_modes: list = None,
) -> dict:
    """Build result DataFrames for all modes.

    Args:
        results: dict {mode: DataFrame} from run_mode().results
        all_requested_modes: list of all modes that were requested.
            When provided, sentinel rows are inserted for modes whose
            result is None (matching SQL wrapper SP behavior where the
            load SPs always execute regardless of input data).

    Returns:
        dict {table_name: DataFrame} ready for GenericResultStorer
    """
    all_tables = {}

    for mode, df in results.items():
        if df is None:
            continue

        if mode == 1:
            tables = build_mode1_results(spark, cfg, df)
        elif mode == 2:
            tables = build_mode2_results(spark, cfg, df)
        elif mode == 3:
            tables = build_mode3_results(spark, cfg, df)
        elif mode == 4:
            tables = build_mode4_results(spark, cfg, df)
        else:
            continue

        # Merge into all_tables — if same table from multiple modes, union
        for tbl_name, tbl_df in tables.items():
            if tbl_name in all_tables:
                all_tables[tbl_name] = all_tables[tbl_name].unionByName(
                    tbl_df, allowMissingColumns=True,
                )
            else:
                all_tables[tbl_name] = tbl_df

    # Sentinel rows for modes that were requested but had no data.
    # SQL wrapper SP always calls the load SPs, which always insert sentinels.
    if all_requested_modes:
        _sentinel_config = {
            1: ("FinalEffectivePercentages",
                {"line_type_id": cfg.get("box_jkl_line_type_id"),
                 "is_exclude_from_transfer": False}),
            2: ("FNFinalEffectivePercentages",
                {"sentinel_overrides": {
                    "EffPercentage": -1.0,
                    "AllocationType": "0",
                    "TypeId": None,
                    "TrackingKey": "-1",
                    "Tag": None,
                    "LineTypeID": 0,
                }}),
            3: ("SM_FinalEffectivePercentages",
                {"is_exclude_from_transfer": True}),
        }
        for mode in all_requested_modes:
            if results.get(mode) is not None:
                continue  # sentinel already included in the full build
            if mode not in _sentinel_config:
                continue  # mode 4 sentinel is part of its own build
            target_table, kwargs = _sentinel_config[mode]
            logger.info(
                f"[SENTINEL] mode {mode}: inserting sentinel-only row "
                f"into {target_table} (no input data)"
            )
            sentinel = _build_sentinel_from_table(
                spark, cfg, target_table, **kwargs,
            )
            if target_table in all_tables:
                all_tables[target_table] = all_tables[target_table].unionByName(
                    sentinel, allowMissingColumns=True,
                )
            else:
                all_tables[target_table] = sentinel

    return all_tables
