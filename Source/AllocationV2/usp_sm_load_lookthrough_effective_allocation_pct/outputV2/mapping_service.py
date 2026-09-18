"""Mapping builder with the production checkpoint seam routed through V2."""

from __future__ import annotations

import pyspark.sql.functions as F

from Common_V2.core.checkpoint_V2 import checkpoint_V2
from Common_V2.core.helpers import read_table

from .parent import service_module

_production = service_module("mapping_service")
build_parent_k1_mappings = _production.build_parent_k1_mappings

_DISTINCT_MAP_COLS = [
    "StateID", "StateFieldID", "RegisterLineID", "FieldSourceID",
    "MapLineSubType", "OperationType", "SourceTypeID",
]


def build_mapping_data(spark, cfg):
    """Preserve production mapping logic and its tagged-union checkpoint."""
    entity_id = cfg["entity_id"]
    client_id = cfg["client_id"]
    tax_period_id = cfg["tax_period_id"]
    register_type_id = cfg["register_type_id"]

    map_data_reg = (
        read_table(spark, "MAPDataRegister", cfg)
        .filter(
            (F.col("RegisterTypeID") == register_type_id)
            & ((F.col("EntityID") == entity_id) | (F.col("EntityID") == -1))
            & (F.col("ClientID") == client_id)
            & (F.col("TaxPeriodID") == tax_period_id)
        )
        .select(
            "MapRegisterID", "EntityID", "RegisterLineID", "FieldSourceID",
            "MapLineSubType", "OperationType", "SourceTypeID", "MapLineID",
            "CategoryID", "StateID", "ContributionLineClassification",
        )
    )
    sm_state_lines = read_table(spark, "SM_StateLines", cfg)
    enu_sdl_df = (
        read_table(spark, "ENU_StateDataList", cfg)
        .filter(F.lower(F.col("Category")).isin("fieldsource", "statecategories"))
        .select("ID", "Category", "Value")
    )
    enu_state_cat = enu_sdl_df.filter(
        (F.lower(F.col("Category")) == "statecategories")
        & F.lower(F.col("Value")).isin(
            "income", "deduction", "n/a", "apportionment", "credit"
        )
    )

    entity_mapping = (
        map_data_reg.alias("M")
        .join(
            sm_state_lines.alias("S"),
            F.col("M.MapLineID") == F.col("S.StateFieldID"),
        )
        .join(
            F.broadcast(enu_state_cat).alias("enu"),
            F.col("M.CategoryID") == F.col("enu.ID"),
        )
        .filter(F.col("M.EntityID") == entity_id)
        .select(
            F.col("M.MapRegisterID"),
            F.col("M.EntityID"),
            F.col("S.StateID"),
            F.col("S.StateFieldID"),
            F.col("M.RegisterLineID"),
            F.col("M.FieldSourceID"),
            F.col("M.MapLineSubType"),
            F.col("M.OperationType"),
            F.col("M.SourceTypeID"),
            F.col("M.ContributionLineClassification"),
        )
    )
    default_mapping = (
        map_data_reg.alias("M2")
        .join(
            sm_state_lines.alias("S2"),
            F.col("M2.MapLineID") == F.col("S2.StateFieldID"),
        )
        .join(
            F.broadcast(enu_state_cat).alias("enu2"),
            F.col("M2.CategoryID") == F.col("enu2.ID"),
        )
        .join(
            entity_mapping.select(
                F.col("StateID").alias("_EM_StateID"),
                F.col("StateFieldID").alias("_EM_StateFieldID"),
            ).distinct().alias("EM"),
            (F.col("M2.StateID") == F.col("EM._EM_StateID"))
            & (F.col("M2.MapLineID") == F.col("EM._EM_StateFieldID")),
            "left_anti",
        )
        .filter(F.col("M2.EntityID") == -1)
        .select(
            F.col("M2.MapRegisterID"),
            F.col("M2.EntityID"),
            F.col("S2.StateID"),
            F.col("S2.StateFieldID"),
            F.col("M2.RegisterLineID"),
            F.col("M2.FieldSourceID"),
            F.col("M2.MapLineSubType"),
            F.col("M2.OperationType"),
            F.col("M2.SourceTypeID"),
            F.col("M2.ContributionLineClassification"),
        )
    )
    mapping_df = build_parent_k1_mappings(
        spark, cfg, entity_mapping.unionByName(default_mapping)
    )

    federal_ids = [
        value for value in (
            cfg["federal_amount_id"],
            cfg["federal_adj_id"],
            cfg["alloc_only_federal_amount_id"],
        )
        if value is not None
    ]
    ubti_ids = [
        value for value in (
            cfg["federal_ubti_id"],
            cfg["federal_ubti_adj_id"],
            cfg["alloc_only_federal_ubti_id"],
        )
        if value is not None
    ]
    distinct_mappings = (
        mapping_df.filter(F.col("FieldSourceID").isin(federal_ids))
        .select(*_DISTINCT_MAP_COLS)
        .distinct()
    )
    distinct_ubti_mappings = (
        mapping_df.filter(F.col("FieldSourceID").isin(ubti_ids))
        .select(*_DISTINCT_MAP_COLS)
        .distinct()
    )

    if (cfg.get("allocation_type_name") or "").strip().lower() == "pe book allocation":
        k1_line_item = read_table(spark, "K1LineItem", cfg)
        tmp_mapping = (
            distinct_mappings.alias("EM")
            .join(
                sm_state_lines.alias("S3"),
                F.col("EM.StateFieldID") == F.col("S3.StateFieldID"),
            )
            .join(
                k1_line_item.alias("K1"),
                F.col("K1.LineID") == F.col("EM.RegisterLineID"),
            )
            .join(
                k1_line_item.alias("K2"),
                (
                    F.col("K2.LineDescription")
                    == F.concat(F.col("K1.LineDescription"), F.lit(" - Offset"))
                )
                & (F.col("K1.Box") == F.col("K2.Box"))
                & (F.col("K1.LineNumber") == F.col("K2.LineNumber")),
            )
            .filter(F.col("EM.SourceTypeID") == cfg["k1_line_type"])
            .select(
                F.col("EM.StateID"),
                F.col("S3.StateFieldID"),
                F.col("EM.RegisterLineID"),
                F.col("EM.FieldSourceID"),
                F.col("EM.MapLineSubType"),
                F.col("EM.OperationType"),
                F.col("EM.SourceTypeID"),
            )
        )
        distinct_mappings = distinct_mappings.unionByName(tmp_mapping)

    combined = (
        distinct_mappings.withColumn("_tag", F.lit("K1"))
        .unionByName(
            distinct_ubti_mappings.withColumn("_tag", F.lit("UBTI")),
            allowMissingColumns=True,
        )
    )
    combined = checkpoint_V2(
        spark, combined, "mapping_distinct_tagged_union", cfg
    )
    distinct_mappings = combined.filter(F.col("_tag") == "K1").drop("_tag")
    distinct_ubti_mappings = combined.filter(F.col("_tag") == "UBTI").drop("_tag")
    state_mapped_lines = (
        distinct_mappings.select("StateID", "StateFieldID").distinct()
        .unionByName(
            distinct_ubti_mappings.select("StateID", "StateFieldID").distinct()
        )
        .distinct()
    )
    return {
        "distinct_mappings": distinct_mappings,
        "distinct_ubti_mappings": distinct_ubti_mappings,
        "state_mapped_lines": state_mapped_lines,
        "mapping": mapping_df,
    }


__all__ = ["build_mapping_data", "build_parent_k1_mappings"]
