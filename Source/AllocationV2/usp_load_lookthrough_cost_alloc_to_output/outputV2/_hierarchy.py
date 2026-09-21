"""Entity hierarchy building and asset class filtering."""

import logging
import time

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

from Common_V2.core.helpers import read_table, table_prefix, ns, ns0
from Common_V2.core.checkpoint_V2 import checkpoint_V2 as checkpoint
from Common_V2.core.observability import log_section, log_timing

try:
    # get_logger is the project-standard structured logger.
    from Common_V2.core.observability import get_logger
    logger = get_logger(__name__)
except ImportError:  # graceful fallback
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Local checkpoint helper (SparkMigrate Rule 35 / O5)
# ---------------------------------------------------------------------------
# Set _USE_LOCAL_CHECKPOINT = True  to use localCheckpoint (in-memory, no Delta I/O)
# Set _USE_LOCAL_CHECKPOINT = False to use Delta checkpoint   (durable, slower)
_USE_LOCAL_CHECKPOINT = False   # localCheckpoint -- flip to False to revert to Delta


def _checkpoint(spark: SparkSession, df: DataFrame, name: str, cfg: dict) -> DataFrame:
    """In-pipeline materialization that breaks lineage.

    Delta checkpoint costs 3-5s per call (write + read). localCheckpoint(eager=True)
    costs ~0.5-1s. Per SparkMigrate Rule 35 / O5, prefer localCheckpoint for
    intermediate materialization within a single pipeline run. The .toDF(*cp.columns)
    re-wrap strips alias-qualifier metadata so downstream F.col("ALIAS.col")
    references work correctly (lessons_learned §1.2 alias rule).
    """
    if _USE_LOCAL_CHECKPOINT:
        logger.info(f"[CHECKPOINT] {name} (localCheckpoint)")
        cp = df.localCheckpoint(eager=True)
        return cp.toDF(*cp.columns)
    return checkpoint(spark, df, name, cfg)


def build_entity_hierarchy(spark: SparkSession, cfg: dict,
                           cost_percentages: DataFrame) -> DataFrame:
    """Build entity hierarchy using iterative recursion with lineage breaking.
    Replaces SQL recursive CTE with loop + Delta checkpoint.
    ALWAYS-NON-EMPTY for entities with underlying investments.
    """
    log_section("build_entity_hierarchy")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]

    # Cost underlyings: exclude K-1 ONLY (unless InvestmentID = -1)
    cost_underlyings = cost_percentages.filter(
        (F.lower(F.col("EntityUnderlyingType")) != "k-1 only") |
        ((F.lower(F.col("EntityUnderlyingType")) == "k-1 only") &
         (F.col("InvestmentID") == -1))
    ).select(
        "ClientID", "TaxPeriodID", "EntityID", "InvestmentID",
        "AllocationTypeID", "Tag", "TrackingKey", "UnderlyingType",
        "EntityUnderlyingType"
    ).distinct()

    # Materialize cost_underlyings -- consumed by base-case join,
    # entity_total_underlyings join, and asset-class filter functions.
    # Avoids 4-table DAG re-evaluation downstream.
    cost_underlyings = _checkpoint(spark, cost_underlyings, "cost_underlyings", cfg)
    cost_underlyings = cost_underlyings.alias("TC")

    # Entity relationships: filter to (client, period) and prune to the only
    # columns used downstream. Plain Delta scan with predicate pushdown
    # (no broadcast -- relies on Delta data skipping; if profile shows shuffle
    # cost dominates again, re-add F.broadcast on a row-count-bounded subset).
    entity_rel = read_table(spark, "EntityRelationship", cfg).filter(
        (F.col("ClientID") == cfg["client_id"]) &
        (F.col("TaxPeriodID") == cfg["tax_period_id"])
    ).select("LowerTierEntityID", "UpperTierEntityID")
    # Materialize the pruned relationship scan once. Base level and every
    # hierarchy iteration join this frame; without this, each eager
    # hierarchy_lvl_* checkpoint re-scans EntityRelationship.
    entity_rel = _checkpoint(spark, entity_rel, "entity_relationship_pruned", cfg)

    # --- Level 2 (base case) ---
    hierarchy_level_2 = cost_underlyings.alias("TC").join(
        entity_rel.alias("ER"),
        F.col("ER.UpperTierEntityID") ==
        F.when(F.lower(F.col("TC.EntityUnderlyingType")) == "asset class",
               F.col("TC.EntityID"))
        .otherwise(F.col("TC.InvestmentID"))
    ).select(
        F.col("ER.LowerTierEntityID"),
        F.col("ER.UpperTierEntityID").alias("ParentEntityID"),
        F.col("ER.UpperTierEntityID").alias("CurrentEntityId"),
        F.lit(2).alias("HLevel"),
        F.col("TC.AllocationTypeID").alias("AllocationTypeId"),
        F.concat(
            F.lit("~"),
            F.when(F.lower(F.col("TC.EntityUnderlyingType")) == "asset class",
                   F.concat(F.col("ER.LowerTierEntityID").cast("string"), F.lit("~")))
            .otherwise(F.concat(
                F.when(ns(F.col("TC.TrackingKey")) == F.lit(""),
                       F.col("TC.InvestmentID").cast("string"))
                .otherwise(F.col("TC.TrackingKey")),
                F.lit("~")
            ))
        ).alias("TrackingKey"),
        F.col("TC.InvestmentID").alias("AssetClassId"),
        F.col("ER.LowerTierEntityID").alias("ImmediateLowerTierEntityID"),
        F.col("TC.UnderlyingType").alias("Underlyingtype"),
    ).distinct()

    all_levels = [hierarchy_level_2]
    current_level = hierarchy_level_2
    level = 3

    # Iterative recursion -- checkpoint EVERY level so the loop guard
    # (first() is None) reads materialized Delta data instead of forcing a
    # full DAG recomputation each iteration (the old `isEmpty()` cost).
    while True:
        next_level = entity_rel.alias("ER").join(
            current_level.alias("EH"),
            F.col("ER.UpperTierEntityID") == F.col("EH.LowerTierEntityID")
        ).select(
            F.col("ER.LowerTierEntityID"),
            F.col("ER.UpperTierEntityID").alias("ParentEntityID"),
            F.col("EH.CurrentEntityId"),
            F.lit(level).alias("HLevel"),
            F.col("EH.AllocationTypeId"),
            F.col("EH.TrackingKey"),
            F.col("EH.AssetClassId"),
            F.col("EH.ImmediateLowerTierEntityID"),
            F.col("EH.Underlyingtype"),
        )

        # Materialize -- first() on materialized data is O(1).
        # localCheckpoint (~0.5-1s) instead of Delta (~3-5s) per Rule 35.
        next_level = _checkpoint(spark, next_level,
                                 f"hierarchy_lvl_{level}", cfg)

        if next_level.first() is None:
            break

        all_levels.append(next_level)
        current_level = next_level
        level += 1

        # NOTE: the previous "extra lineage break every 3 iterations" was
        # redundant -- next_level was just checkpointed and reassigned to
        # current_level on the line above, so re-checkpointing it materialized
        # the same DataFrame twice (cost ~3-5s per Delta write). Removed.

    # Union all levels
    hierarchy = all_levels[0]
    for lvl_df in all_levels[1:]:
        hierarchy = hierarchy.unionByName(lvl_df)

    # Break lineage after union -- hierarchy feeds the entity_total_underlyings
    # join below; without this the unioned plan re-executes per consumer.
    hierarchy = _checkpoint(spark, hierarchy, "entity_hier_final", cfg)

    # --- Build combined underlyings from hierarchy ---
    entity_total_underlyings = hierarchy.alias("EH").join(
        cost_underlyings.alias("TC"),
        (F.col("EH.CurrentEntityId") ==
         F.when(F.lower(F.col("TC.EntityUnderlyingType")) == "asset class",
                F.col("TC.EntityID"))
         .otherwise(F.col("TC.InvestmentID"))) &
        (F.col("TC.AllocationTypeID") == F.col("EH.AllocationTypeId")) &
        (F.col("TC.InvestmentID") == F.col("EH.AssetClassId"))
    ).select(
        F.col("EH.LowerTierEntityID").alias("UnderlyingEntityId"),
        F.col("EH.CurrentEntityId").alias("EntityId"),
        F.col("EH.HLevel"),
        F.col("TC.UnderlyingType").alias("Underlyingtype"),
        F.col("TC.AllocationTypeID").alias("AllocationTypeId"),
        F.col("EH.TrackingKey"),
        F.col("EH.AssetClassId"),
        F.col("EH.ImmediateLowerTierEntityID"),
    )

    # --- K-1 ONLY cases ---
    k1_only_investment = cost_percentages.filter(
        F.lower(F.col("EntityUnderlyingType")) == "k-1 only"
    ).select(
        F.col("InvestmentID").alias("UnderlyingEntityId"),
        F.col("InvestmentID").alias("EntityId"),
        F.lit(1).alias("HLevel"),
        F.col("UnderlyingType").alias("Underlyingtype"),
        F.col("AllocationTypeID").alias("AllocationTypeId"),
        F.when(ns(F.col("TrackingKey")) == F.lit(""),
               F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")))
        .otherwise(F.col("TrackingKey")).alias("TrackingKey"),
        F.col("InvestmentID").alias("AssetClassId"),
        F.lit(0).cast("int").alias("ImmediateLowerTierEntityID"),
    )

    k1_only_entity = cost_percentages.filter(
        (F.lower(F.col("EntityUnderlyingType")) == "k-1 only") &
        (F.col("InvestmentID") == -1)
    ).select(
        F.lit(entity_id).cast("int").alias("UnderlyingEntityId"),
        F.lit(entity_id).cast("int").alias("EntityId"),
        F.lit(1).alias("HLevel"),
        F.col("UnderlyingType").alias("Underlyingtype"),
        F.col("AllocationTypeID").alias("AllocationTypeId"),
        F.concat(F.lit("~"), F.lit(entity_id).cast("string"), F.lit("~")).alias("TrackingKey"),
        F.lit(entity_id).cast("int").alias("AssetClassId"),
        F.lit(0).cast("int").alias("ImmediateLowerTierEntityID"),
    )

    # --- Asset Class cases ---
    asset_class_underlyings = cost_underlyings.filter(
        F.lower(F.col("EntityUnderlyingType")) == "asset class"
    ).select(
        F.col("EntityID").alias("UnderlyingEntityId"),
        F.col("InvestmentID").alias("EntityId"),
        F.lit(1).alias("HLevel"),
        F.col("UnderlyingType").alias("Underlyingtype"),
        F.col("AllocationTypeID").alias("AllocationTypeId"),
        F.concat(F.lit("~"), F.col("EntityID").cast("string"), F.lit("~")).alias("TrackingKey"),
        F.col("InvestmentID").alias("AssetClassId"),
        F.col("EntityID").alias("ImmediateLowerTierEntityID"),
    )

    # --- Entity Total at source ---
    entity_total_source = cost_underlyings.filter(
        F.lower(F.col("EntityUnderlyingType")) == "entity total"
    ).select(
        F.col("InvestmentID").alias("UnderlyingEntityId"),
        F.col("InvestmentID").alias("EntityId"),
        F.lit(1).alias("HLevel"),
        F.col("UnderlyingType").alias("Underlyingtype"),
        F.col("AllocationTypeID").alias("AllocationTypeId"),
        F.when(ns(F.col("TrackingKey")) == F.lit(""),
               F.concat(F.lit("~"), F.col("InvestmentID").cast("string"), F.lit("~")))
        .otherwise(F.col("TrackingKey")).alias("TrackingKey"),
        F.col("InvestmentID").alias("AssetClassId"),
        F.lit(0).cast("int").alias("ImmediateLowerTierEntityID"),
    )

    # Combine all underlying types
    all_underlyings = (
        entity_total_underlyings
        .unionByName(k1_only_investment)
        .unionByName(k1_only_entity)
        .unionByName(asset_class_underlyings)
        .unionByName(entity_total_source)
        .distinct()
    )

    # Checkpoint: deep lineage (3+ joins) + consumed by rule ordering below
    all_underlyings = _checkpoint(spark, all_underlyings, "all_underlyings", cfg)

    # --- Asset class filtering ---
    # OPT: short-circuit. Both filter functions are no-ops when all_underlyings
    # contains zero rows of type "asset class" (they end with
    # `if all_matching.isEmpty()/rows_to_delete.isEmpty(): return all_underlyings`).
    # The asset class relationship UDF takes ~9s on Spark Connect because it
    # runs a per-entity Python loop with nested Spark queries. Skipping that
    # UDF entirely when there is no asset class data to filter is a >9s win.
    # Asset Class underlying type ID from pre-resolved cfg scalar.
    _ac_id = cfg.get("underlying_type_id_asset_class")
    ac_type_ids = [_ac_id] if _ac_id is not None else []
    has_asset_class_rows = False
    if ac_type_ids:
        has_asset_class_rows = all_underlyings.filter(
            F.col("Underlyingtype").isin(ac_type_ids)
        ).limit(1).first() is not None

    if not has_asset_class_rows:
        logger.info("[SKIP] asset class filter -- no asset-class rows in "
                    "all_underlyings; UDF call avoided")
    else:
        override_config = cfg["override_indirect_lookthrough_assetclass"]
        if override_config != "C":
            all_underlyings = _filter_matching_asset_classes(
                spark, cfg, all_underlyings)
        else:
            all_underlyings = _filter_override_asset_classes(
                spark, cfg, all_underlyings)

    # Ignore asset class for partnership level if configured
    if cfg["ignore_assetclass_partnership"] == "C" and ac_type_ids:
        all_underlyings = all_underlyings.filter(
            ~((F.col("Underlyingtype").isin(ac_type_ids)) &
              (F.col("UnderlyingEntityId") == entity_id))
        )

    log_timing("build_entity_hierarchy", t0)
    return all_underlyings


def _get_asset_class_relationship(spark: SparkSession, cfg: dict) -> DataFrame:
    """Call udfGetAssetClassRelationship from common/udf.
    Returns DataFrame with columns: LowerTierEntityID, AssetClassID, TrackingKey

    Cached in cfg["_asset_class_rel"] + localCheckpointed once per SP run.
    Subsequent .isEmpty() / .filter() reads hit materialized data (~0.1s)
    instead of re-running the UDF DAG (~2-3s each). Per Rule 35 / Rule 37.
    """
    cached = cfg.get("_asset_class_rel")
    if cached is not None:
        return cached

    from Common_V2.domain.udf_asset_class_relationship import (
        udfGetAssetClassRelationship,
    )

    udf_instance = udfGetAssetClassRelationship(
        spark, cfg, entity_ids=str(cfg["entity_id"])
    )
    asset_class_rel = _checkpoint(
        spark, udf_instance.execute(), "asset_class_rel", cfg
    )
    cfg["_asset_class_rel"] = asset_class_rel
    return asset_class_rel


def _filter_matching_asset_classes(spark: SparkSession, cfg: dict,
                                   all_underlyings: DataFrame) -> DataFrame:
    """Filter underlyings to match asset class relationships."""
    prefix = table_prefix(cfg)

    # Asset class relationships from common UDF
    asset_class_rel = _get_asset_class_relationship(spark, cfg)

    if asset_class_rel.isEmpty():
        return all_underlyings

    # Null tracking key matches
    null_tk = asset_class_rel.filter(F.col("TrackingKey").isNull())
    non_null_tk = asset_class_rel.filter(F.col("TrackingKey").isNotNull())

    matching_parts = []

    if not null_tk.isEmpty():
        m1 = all_underlyings.alias("AI").join(
            null_tk.alias("EAR"),
            F.col("AI.UnderlyingEntityId") == F.col("EAR.LowerTierEntityID")
        ).join(
            read_table(spark, "Entity", cfg).alias("E"),
            F.col("AI.UnderlyingEntityId") == F.col("E.EntityID")
        ).join(
            read_table(spark, "ENU_UnderlyingType", cfg).alias("U"),
            F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID")
        ).filter(
            (F.lower(F.col("U.UnderlyingType")) == "asset class") &
            (F.when(ns0(F.col("EAR.AssetClassID")) == 0,
                    F.col("E.AssetClassID"))
             .otherwise(F.col("EAR.AssetClassID")) == F.col("AI.AssetClassId"))
        ).select(
            F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"),
            F.col("AI.HLevel"), F.col("AI.Underlyingtype"),
            F.col("AI.AllocationTypeId"), F.col("AI.TrackingKey"),
            F.col("AI.AssetClassId"), F.col("AI.ImmediateLowerTierEntityID"),
        )
        matching_parts.append(m1)

    if not non_null_tk.isEmpty():
        m2 = all_underlyings.alias("AI").join(
            non_null_tk.alias("EAR"),
            (F.col("AI.UnderlyingEntityId") == F.col("EAR.LowerTierEntityID")) &
            (F.concat(F.lit("~"), F.col("EAR.TrackingKey"), F.lit("~")).like(
                F.concat(F.lit("%"), F.col("AI.TrackingKey"), F.lit("%"))
            ))
        ).join(
            read_table(spark, "Entity", cfg).alias("E"),
            F.col("AI.UnderlyingEntityId") == F.col("E.EntityID")
        ).join(
            read_table(spark, "ENU_UnderlyingType", cfg).alias("U"),
            F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID")
        ).filter(
            (F.lower(F.col("U.UnderlyingType")) == "asset class") &
            (F.when(ns0(F.col("EAR.AssetClassID")) == 0,
                    F.col("E.AssetClassID"))
             .otherwise(F.col("EAR.AssetClassID")) == F.col("AI.AssetClassId"))
        ).select(
            F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"),
            F.col("AI.HLevel"), F.col("AI.Underlyingtype"),
            F.col("AI.AllocationTypeId"), F.col("AI.TrackingKey"),
            F.col("AI.AssetClassId"), F.col("AI.ImmediateLowerTierEntityID"),
        )
        matching_parts.append(m2)

    # Direct entity-asset class match (not already matched above)
    if matching_parts:
        previous_matches = matching_parts[0]
        for mp in matching_parts[1:]:
            previous_matches = previous_matches.unionByName(mp)
    else:
        previous_matches = spark.createDataFrame([], all_underlyings.schema)

    # Direct match on Entity.AssetClassID
    matching_direct = all_underlyings.alias("AI").join(
        read_table(spark, "Entity", cfg).alias("E"),
        (F.col("AI.UnderlyingEntityId") == F.col("E.EntityID")) &
        (F.col("AI.AssetClassId") == F.col("E.AssetClassID"))
    ).join(
        read_table(spark, "ENU_UnderlyingType", cfg).alias("U"),
        F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID")
    ).join(
        previous_matches.alias("M"),
        (F.col("M.UnderlyingEntityId") == F.col("AI.UnderlyingEntityId")) &
        (F.col("AI.TrackingKey") == F.col("M.TrackingKey")) &
        (F.col("AI.ImmediateLowerTierEntityID") == F.col("M.ImmediateLowerTierEntityID")),
        "left"
    ).filter(
        (F.lower(F.col("U.UnderlyingType")) == "asset class") &
        (F.col("M.UnderlyingEntityId").isNull())
    ).select(
        F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"),
        F.col("AI.HLevel"), F.col("AI.Underlyingtype"),
        F.col("AI.AllocationTypeId"), F.col("AI.TrackingKey"),
        F.col("AI.AssetClassId"), F.col("AI.ImmediateLowerTierEntityID"),
    )

    all_matching = previous_matches.unionByName(matching_direct).distinct()

    if all_matching.isEmpty():
        return all_underlyings

    # Remove asset class rows NOT in matching set
    matching_keys = all_matching.select(
        "UnderlyingEntityId", "TrackingKey", "AssetClassId",
        "ImmediateLowerTierEntityID"
    ).distinct()

    filtered = all_underlyings.alias("AU").join(
        matching_keys.alias("MD"),
        (F.col("AU.UnderlyingEntityId") == F.col("MD.UnderlyingEntityId")) &
        (F.col("AU.TrackingKey") == F.col("MD.TrackingKey")) &
        (F.col("AU.AssetClassId") == F.col("MD.AssetClassId")) &
        (F.col("AU.ImmediateLowerTierEntityID") == F.col("MD.ImmediateLowerTierEntityID")),
        "left"
    ).join(
        read_table(spark, "ENU_UnderlyingType", cfg).alias("UT"),
        F.col("AU.Underlyingtype") == F.col("UT.UnderlyingTypeID")
    ).filter(
        (F.lower(F.col("UT.UnderlyingType")) != "asset class") |
        (F.col("MD.UnderlyingEntityId").isNotNull())
    ).select(
        F.col("AU.UnderlyingEntityId"), F.col("AU.EntityId"),
        F.col("AU.HLevel"), F.col("AU.Underlyingtype"),
        F.col("AU.AllocationTypeId"), F.col("AU.TrackingKey"),
        F.col("AU.AssetClassId"), F.col("AU.ImmediateLowerTierEntityID"),
    )

    return filtered


def _filter_override_asset_classes(spark: SparkSession, cfg: dict,
                                   all_underlyings: DataFrame) -> DataFrame:
    """Filter with override indirect lookthrough asset class logic."""

    # Use the same UDF result as _filter_matching_asset_classes
    # (SQL stores UDF output in #EntityAssetClassRelationship temp table and reuses it)
    asset_class_rel = _get_asset_class_relationship(spark, cfg)

    # Find rows where asset class does NOT match → delete them
    rows_to_delete = all_underlyings.alias("AI").join(
        asset_class_rel.alias("EAR"),
        F.col("AI.ImmediateLowerTierEntityID") == F.col("EAR.LowerTierEntityID"),
        "left"
    ).join(
        read_table(spark, "Entity", cfg).alias("E"),
        F.col("AI.ImmediateLowerTierEntityID") == F.col("E.EntityID")
    ).join(
        read_table(spark, "ENU_UnderlyingType", cfg).alias("U"),
        F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID")
    ).filter(
        (F.lower(F.col("U.UnderlyingType")) == "asset class") &
        (F.when(ns0(F.col("EAR.AssetClassID")) == 0,
                F.col("E.AssetClassID"))
         .otherwise(F.col("EAR.AssetClassID")) != F.col("AI.AssetClassId"))
    ).select(
        F.col("AI.UnderlyingEntityId"), F.col("AI.EntityId"),
        F.col("AI.HLevel"), F.col("AI.Underlyingtype"),
        F.col("AI.AllocationTypeId"), F.col("AI.TrackingKey"),
        F.col("AI.AssetClassId"), F.col("AI.ImmediateLowerTierEntityID"),
    ).distinct()

    if rows_to_delete.isEmpty():
        return all_underlyings

    return all_underlyings.join(rows_to_delete,
                                on=all_underlyings.columns,
                                how="left_anti")


def build_rule_ordered_underlyings(spark: SparkSession, cfg: dict,
                                   all_underlyings: DataFrame,
                                   lookthrough_input: DataFrame,
                                   map_rules: DataFrame,
                                   default_rules: DataFrame) -> DataFrame:
    """Build ordered underlyings with rule ranking (waterfall model).
    5-way join with conditional keys + ROW_NUMBER window for rank=1 selection.
    ALWAYS-NON-EMPTY for valid allocation setup.
    """
    log_section("build_rule_ordered_underlyings")
    t0 = time.time()
    prefix = table_prefix(cfg)
    entity_id = cfg["entity_id"]
    k1_line_type_id = cfg["k1_line_type_id"]
    adjustment_line_type_id = cfg["adjustment_line_type_id"]
    override_config = cfg["override_indirect_lookthrough_assetclass"]

    # 5-way join: all_underlyings → lookthrough_input → map_rules → default_rules → ENU_RuleType → ENU_AllocationBy
    ordered = all_underlyings.alias("AI").join(
        read_table(spark, "ENU_UnderlyingType", cfg).alias("U"),
        F.col("AI.Underlyingtype") == F.col("U.UnderlyingTypeID")
    ).join(
        lookthrough_input.alias("L"),
        (F.col("L.EntityID") == F.col("AI.UnderlyingEntityId")) &
        # Conditional tracking key join
        (F.when(
            (F.col("AI.UnderlyingEntityId") == entity_id) |
            ((F.col("AI.EntityId") == entity_id) &
             (F.lower(F.col("U.UnderlyingType")) != "asset class")) |
            ((F.lit(override_config) != "C") &
             (F.lower(F.col("U.UnderlyingType")) == "asset class")),
            F.lit("-1")
        ).otherwise(
            F.concat(F.lit("~"), F.col("L.TrackingKey"), F.lit("~"))
        ).like(
            F.when(
                (F.col("AI.UnderlyingEntityId") == entity_id) |
                ((F.col("AI.EntityId") == entity_id) &
                 (F.lower(F.col("U.UnderlyingType")) != "asset class")) |
                ((F.lit(override_config) != "C") &
                 (F.lower(F.col("U.UnderlyingType")) == "asset class")),
                F.lit("-1")
            ).otherwise(
                F.concat(F.lit("%"), F.col("AI.TrackingKey"), F.lit("%"))
            )
        ))
    ).join(
        map_rules.alias("M"),
        (F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("L.LineID")) ==
         F.when(F.col("M.SelectedMappingID") == -1, F.lit(1))
         .otherwise(F.col("M.SelectedMappingID"))) &
        (F.col("M.RuleID") == F.col("AI.AllocationTypeId")) &
        (F.col("M.SourceID") ==
         F.when(F.col("L.LineTypeID") == adjustment_line_type_id,
                k1_line_type_id)
         .otherwise(F.col("L.LineTypeID")))
    ).join(
        default_rules.alias("D"),
        (F.col("D.RuleID") == F.col("AI.AllocationTypeId")) &
        (F.col("AI.Underlyingtype") == F.col("D.UnderlyingTypeID"))
    ).join(
        read_table(spark, "ENU_RuleType", cfg).alias("R"),
        F.col("D.RuleTypeID") == F.col("R.RuleTypeID")
    ).join(
        read_table(spark, "ENU_AllocationBy", cfg).alias("EA"),
        F.col("D.AllocationByID") == F.col("EA.AllocationByID")
    )

    # Window for rule ranking
    window_spec = Window.partitionBy(
        F.col("AI.UnderlyingEntityId"),
        F.col("L.TrackingKey"),
        F.col("L.LineID"),
        F.when(F.col("L.LineTypeID") == adjustment_line_type_id,
               k1_line_type_id).otherwise(F.col("L.LineTypeID")),
        F.col("EA.DisplayOrder"),
    ).orderBy(
        F.col("AI.HLevel"),
        F.col("R.DisplayOrder").desc(),
        F.col("U.DisplayOrder"),
        F.col("M.SelectedMappingID").desc(),
        F.col("EA.DisplayOrder"),
    )

    ranked = ordered.withColumn(
        "RankForUnderlyingPickup", F.row_number().over(window_spec)
    ).filter(
        F.col("RankForUnderlyingPickup") == F.lit(1)  # INT from ROW_NUMBER, not BIT
    ).select(
        F.col("AI.Underlyingtype"),
        F.col("AI.UnderlyingEntityId"),
        F.col("AI.EntityId"),
        F.col("L.TrackingKey"),
        F.col("AI.TrackingKey").alias("TrackingMatch"),
        F.col("AI.AllocationTypeId"),
        F.col("L.LineID"),
        F.col("M.ExcludeFromTransfers"),
        F.col("RankForUnderlyingPickup"),
        F.when(F.col("L.LineTypeID") == adjustment_line_type_id,
               k1_line_type_id)
        .otherwise(F.col("L.LineTypeID")).alias("LineTypeID"),
        F.col("EA.AllocationBy"),
        F.col("M.StateID"),
    )

    if ranked.isEmpty():
        logger.warning(f"build_rule_ordered_underlyings returned 0 rows for entity_id={entity_id}. "
                       "This may indicate no matching allocation rules.")
    log_timing("build_rule_ordered_underlyings", t0)
    return ranked
