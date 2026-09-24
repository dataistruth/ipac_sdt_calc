"""Parallel outputV3 control flow using unchanged production business helpers.

Only the production ``run_modes`` orchestration is forked here. All DataFrame
business transformations are called from the isolated production module.
"""

from __future__ import annotations

import time
from functools import reduce

from .cfg_isolation import fork_cfg, merge_mode_artifacts


def _union(dfs):
    real = [df for df in dfs if df is not None]
    if not real:
        return None
    return reduce(
        lambda left, right: left.unionByName(
            right, allowMissingColumns=True
        ),
        real,
    )


def run_modes_parallel(
    business,
    run_group,
    production_run_modes,
    spark,
    modes,
    entity_id=None,
    client_id=None,
    tax_period_id=None,
    run_id=None,
    catalog=None,
    schema=None,
    cfg=None,
    verbose=False,
    ResultType="deltalake",
    VolumePath=None,
    ExecutionID=None,
):
    """Production-compatible modes 1/2/3 pipeline with isolated parallel cfgs."""
    if isinstance(modes, int):
        modes = [modes]
    for mode in modes:
        if mode not in (1, 2, 3, 4):
            raise ValueError(f"mode must be 1, 2, 3, or 4 -- got {mode}")

    # Mode 4 mutates catalog metadata in its 704c path and has no independent
    # sibling branch. Mixed mode-4 calls retain the exact production flow.
    if 4 in modes:
        if isinstance(cfg, dict):
            cfg["_output_v3_pipeline_strategy"] = "production_mode4_control_flow"
        return production_run_modes(
            spark,
            modes=modes,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
            cfg=cfg,
            verbose=verbose,
            ResultType=ResultType,
            VolumePath=VolumePath,
            ExecutionID=ExecutionID,
        )

    F = business.F
    checkpoint = business._checkpoint
    started = time.time()
    modes_123 = list(modes)
    if verbose:
        business.logger.setLevel(business.logging.DEBUG)
    if cfg is None:
        cfg = business.load_common_config(
            spark,
            entity_id=entity_id,
            client_id=client_id,
            tax_period_id=tax_period_id,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
        )
    cfg.setdefault("_checkpoint_tables", [])
    cfg.setdefault("_checkpoint_paths", [])
    cfg["_output_v3_pipeline_strategy"] = "parallel_modes_123_control_flow"
    for key, value in {
        "spark.sql.shuffle.partitions": "32",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    }.items():
        try:
            spark.conf.set(key, value)
        except Exception:
            business.logger.info("[AQE] %s unavailable", key)
    cfg.setdefault("result_type", ResultType)
    if VolumePath is not None:
        cfg["volume_path"] = VolumePath
    if ExecutionID is not None:
        cfg["execution_id"] = ExecutionID

    statuses = {}
    save_return_value = None
    try:
        if not cfg.get("_config_loaded"):
            business.load_config(spark, cfg)
            cfg["_config_loaded"] = True

        # Common production stages. Existing outputV3 wrappers overlap only
        # the independently safe read builders in these calls.
        snapshot = business.build_cost_percentage_snapshot_modes123(spark, cfg)
        peb = None
        if 1 in modes_123 and cfg.get("_704c_allocation_type_name"):
            peb_cfg = fork_cfg(cfg, mode=1)
            peb = business.build_mode1_704c_pe_book_allocations(
                spark, peb_cfg, cost_pct_function_df=None
            )
            if peb is not None:
                snapshot = snapshot.unionByName(
                    peb["snapshot_augment"], allowMissingColumns=True
                )
                cfg["has_704c_mappings"] = True
                cfg["_704c_mappings_df"] = peb["mappings"]
            else:
                cfg["has_704c_mappings"] = False
        else:
            cfg["has_704c_mappings"] = False
        snapshot = checkpoint(spark, snapshot, "cost_pct_m123", cfg)

        entity_partners = business.build_entity_partners(spark, cfg)
        cost_underlying_types = business.build_cost_underlying_types(
            spark, cfg, snapshot
        )
        entity_hierarchy = business.build_entity_hierarchy(
            spark, cfg, cost_underlying_types
        )
        asset_class_rel = business.build_asset_class_relationship(spark, cfg)
        underlyings_combined = business.build_underlyings_combined(
            spark,
            cfg,
            cost_underlying_types,
            entity_hierarchy,
            snapshot,
        )
        underlyings_combined = checkpoint(
            spark, underlyings_combined, "underlyings_common", cfg
        )
        dar_setup, map_dar, entity_alloc_rule = (
            business.load_allocation_rules(spark, cfg)
        )
        if peb is not None:
            map_dar = map_dar.unionByName(
                peb["map_dar_704c"], allowMissingColumns=True
            )
            dar_setup = dar_setup.unionByName(
                peb["dar_setup_704c"], allowMissingColumns=True
            )
        line_items = business.load_line_items(spark, cfg)
        book_effective_raw = business.load_book_effective_data(spark, cfg)
        yearly_lines = business.load_yearly_lines(book_effective_raw, cfg)
        quarters = business.load_quarters(spark, cfg)
        yearly_data = business.load_yearly_data(spark, cfg)
        underlyings_filtered = business.filter_asset_class_underlyings(
            spark, cfg, underlyings_combined, asset_class_rel
        )
        underlyings_ordered = business.build_underlyings_hlevel_ordered(
            underlyings_filtered
        )
        underlyings_ordered = checkpoint(
            spark, underlyings_ordered, "uc_ordered_common", cfg
        )
        lt_input_m14 = business.build_lookthrough_input_modes14(spark, cfg)
        footnote_lines = business.build_footnote_lines(spark, cfg)
        book_effective = business.build_footnote_book_effective(
            lt_input_m14, footnote_lines, book_effective_raw, cfg
        )

        common_cfg = fork_cfg(cfg, mode=1, current_mode=0)
        common_temp_cost_pct = business.build_temp_cost_percentage(
            spark, common_cfg, snapshot
        )
        yearly_rows = None
        if not yearly_lines.isEmpty() and not yearly_data.isEmpty():
            yearly_rows = (
                yearly_lines.alias("Y")
                .crossJoin(F.broadcast(yearly_data.alias("YS")))
                .crossJoin(F.broadcast(quarters.alias("Q")))
                .select(
                    F.col("Y.UnderlyingEntityID").alias("DealId"),
                    F.col("YS.PartnerNumber").alias("Partnernumber"),
                    F.col("Q.Quarter"),
                    F.coalesce(
                        F.col("YS.ProRataEffOwnPercent"), F.lit(0.0)
                    ).alias("CommitmentPercent"),
                    F.col("Y.AdjustmentAllocationTypeID").alias("TypeId"),
                    F.lit("").alias("TrackingKey"),
                    F.lit("").alias("Tag"),
                    F.lit(None).cast("int").alias("704cAllocationTypeID"),
                    F.lit(None).cast("string").alias("704cPercentageType"),
                    F.lit(None)
                    .cast("boolean")
                    .alias("GPPartnerReceivingCarry"),
                )
                .distinct()
            )
        if yearly_rows is not None:
            common_temp_cost_pct = common_temp_cost_pct.unionByName(yearly_rows)
            common_temp_cost_pct = checkpoint(
                spark,
                common_temp_cost_pct,
                "tcp_with_yearly_common",
                common_cfg,
            )
        common_underlying_mod = business.build_underlying_mod(
            underlyings_ordered, snapshot
        )

        def build_chain(label, lt_input):
            branch_cfg = fork_cfg(cfg, mode=1, current_mode=0)
            all_underlyings, _ = business.build_all_underlyings_ordered(
                spark,
                branch_cfg,
                common_underlying_mod,
                lt_input,
                book_effective,
                entity_alloc_rule,
                dar_setup,
                map_dar,
                snapshot,
            )
            all_underlyings = checkpoint(
                spark,
                all_underlyings,
                f"all_und_common_{label}",
                branch_cfg,
            )
            input_lines, _, _ = business.build_input_lines(
                spark,
                branch_cfg,
                lt_input,
                line_items,
                book_effective,
                entity_alloc_rule,
                all_underlyings,
            )
            input_lines = checkpoint(
                spark, input_lines, f"input_lines_{label}", branch_cfg
            )
            final_amounts, all_underlyings = (
                business.compute_amount_based_allocation(
                    spark,
                    branch_cfg,
                    all_underlyings,
                    snapshot,
                    lt_input,
                    map_dar,
                )
            )
            non_dated = business.build_non_dated_entities(
                input_lines, line_items, branch_cfg
            )
            dated = business.build_dated_entities(
                spark, branch_cfg, input_lines, line_items
            )
            entity_underlyings = business.build_entity_underlyings(
                spark,
                branch_cfg,
                input_lines,
                underlyings_ordered,
                asset_class_rel,
            )
            entity_underlyings = checkpoint(
                spark,
                entity_underlyings,
                f"entity_und_common_{label}",
                branch_cfg,
            )
            return {
                "cfg": branch_cfg,
                "all_underlyings": all_underlyings,
                "input_lines": input_lines,
                "final_amounts": final_amounts,
                "non_dated_entities": non_dated,
                "dated_entities": dated,
                "entity_underlyings": entity_underlyings,
            }

        chain_tasks = []
        if 1 in modes_123:
            chain_tasks.append(("lt", build_chain, ("lt", lt_input_m14), {}))
        if 2 in modes_123 or 3 in modes_123:
            chain_tasks.append(
                ("nolt", build_chain, ("nolt", lt_input_m14.limit(0)), {})
            )
        chains = run_group("lt_nolt_branches", chain_tasks)

        def prepare_mode(mode):
            mode_started = time.time()
            mode_cfg = fork_cfg(cfg, mode=mode)
            status = {
                "sp_name": "uspGetFinalEffectivePercentage",
                "mode": mode,
                "status": "SUCCESS",
                "error": None,
                "elapsed_seconds": 0,
            }
            alloc_input = (
                business.build_allocation_input(
                    spark, mode_cfg, modes=[mode]
                )
                if mode == 2
                else None
            )
            sm_input = (
                business.build_sm_lookthrough_allocation_input(spark, mode_cfg)
                if mode == 3
                else None
            )
            lt_input = (
                business.build_lookthrough_allocation_input(spark, mode_cfg)
                if mode == 1
                else None
            )
            alloc_empty = alloc_input is None or alloc_input.isEmpty()
            lt_empty = lt_input is None or lt_input.isEmpty()
            sm_empty = sm_input is None or sm_input.isEmpty()
            mode_cfg.update(
                {
                    "_alloc_empty": alloc_empty,
                    "_lt_empty": lt_empty,
                    "_sm_empty": sm_empty,
                    "_inputs_empty": {
                        1: lt_empty,
                        2: alloc_empty,
                        3: sm_empty,
                    }[mode],
                }
            )
            if mode_cfg["_inputs_empty"]:
                status["result"] = None
                status["elapsed_seconds"] = round(
                    time.time() - mode_started, 1
                )
                return {
                    "mode": mode,
                    "cfg": mode_cfg,
                    "status": status,
                    "data": None,
                    "started": mode_started,
                }

            chain = chains["lt" if mode == 1 else "nolt"]
            all_underlyings = chain["all_underlyings"]
            input_lines = chain["input_lines"]
            final_amounts = chain["final_amounts"]
            non_dated = chain["non_dated_entities"]
            dated = chain["dated_entities"]
            entity_underlyings = chain["entity_underlyings"]

            if mode == 2 or (
                mode == 1 and mode_cfg.get("is_pe_model", False)
            ):
                custom_types = business._get_custom_footnote_line_types(
                    spark, mode_cfg
                )
                all_underlyings = (
                    business.build_footnote_underlyings_ordered(
                        spark,
                        mode_cfg,
                        common_underlying_mod,
                        underlyings_ordered,
                        alloc_input,
                        book_effective,
                        all_underlyings,
                        dar_setup,
                        map_dar,
                        custom_types,
                    )
                )
                all_underlyings = checkpoint(
                    spark,
                    all_underlyings,
                    f"all_und_final_m{mode}",
                    mode_cfg,
                )
                footnote_input_lines = business.build_footnote_input_lines(
                    spark,
                    mode_cfg,
                    alloc_input,
                    book_effective,
                    all_underlyings,
                    map_dar,
                )
                footnote_input_lines = checkpoint(
                    spark,
                    footnote_input_lines,
                    f"fn_input_lines_m{mode}",
                    mode_cfg,
                )
                non_dated, dated = (
                    business.build_footnote_dated_entities(
                        spark,
                        mode_cfg,
                        footnote_input_lines,
                        non_dated,
                        dated,
                    )
                )
            non_dated, _ = business.compute_form199a_effective_percentage(
                spark,
                mode_cfg,
                non_dated,
                book_effective,
                input_lines,
                common_temp_cost_pct,
            )
            has_state = (
                mode == 3 and sm_input is not None and not sm_input.isEmpty()
            )
            if has_state:
                all_underlyings, state_lines, state_amounts = (
                    business.build_state_allocation_input(
                        spark,
                        mode_cfg,
                        common_underlying_mod,
                        sm_input,
                        snapshot,
                        all_underlyings,
                        map_dar,
                        dar_setup,
                        entity_partners,
                    )
                )
                non_dated, dated = business.build_state_entities(
                    spark,
                    mode_cfg,
                    state_lines,
                    non_dated,
                    dated,
                )
                if state_amounts is not None:
                    final_amounts = (
                        final_amounts.unionByName(
                            state_amounts, allowMissingColumns=True
                        )
                        if final_amounts is not None
                        else state_amounts
                    )
            if mode == 2 or has_state:
                non_dated = checkpoint(
                    spark,
                    non_dated,
                    f"nde_pre_cpbt_m{mode}",
                    mode_cfg,
                )
                dated = checkpoint(
                    spark, dated, f"de_pre_cpbt_m{mode}", mode_cfg
                )
            transfers = business.load_transfers_adj_cost(
                spark, mode_cfg, all_underlyings, entity_underlyings
            )
            if transfers is not None:
                transfers = checkpoint(
                    spark,
                    transfers,
                    f"txfr_pre_cpbt_m{mode}",
                    mode_cfg,
                )
            data = {
                "temp_cost_pct": common_temp_cost_pct,
                "all_underlyings": all_underlyings,
                "entity_underlyings": entity_underlyings,
                "non_dated_entities": non_dated,
                "dated_entities": dated,
                "transfers_adj": transfers,
                "input_lines": input_lines,
                "final_amounts": final_amounts,
            }
            return {
                "mode": mode,
                "cfg": mode_cfg,
                "status": status,
                "data": data,
                "started": mode_started,
            }

        prepared = run_group(
            "mode_prep",
            [
                (f"mode_{mode}", prepare_mode, (mode,), {})
                for mode in modes_123
            ],
        )
        mode_cfgs = {}
        per_mode = {}
        mode_started = {}
        for mode in modes_123:
            item = prepared[f"mode_{mode}"]
            statuses[mode] = item["status"]
            mode_cfgs[mode] = item["cfg"]
            mode_started[mode] = item["started"]
            if item["data"] is not None:
                per_mode[mode] = item["data"]
        # Match the state left by production's numeric Pass A loop.
        cfg["mode"] = modes_123[-1]
        cfg["_current_mode"] = modes_123[-1]
        artifact_events = merge_mode_artifacts(cfg, mode_cfgs)
        cfg.setdefault("_output_v3_artifact_merges", []).extend(artifact_events)

        valid_modes = sorted(per_mode)
        if valid_modes:
            # Reproduce the production loop's final mode value while using the
            # documented fused sentinel for all fused helper checkpoint names.
            cfg["mode"] = modes_123[-1]
            cfg["_current_mode"] = 0
            tag = lambda df, mode: (
                None if df is None else df.withColumn("_mode", F.lit(mode))
            )
            fused_temp, fused_transfers = (
                business.build_cost_percentage_by_type(
                    spark,
                    cfg,
                    snapshot,
                    _union(
                        [
                            tag(per_mode[m]["temp_cost_pct"], m)
                            for m in valid_modes
                        ]
                    ),
                    _union(
                        [
                            tag(per_mode[m]["all_underlyings"], m)
                            for m in valid_modes
                        ]
                    ),
                    _union(
                        [
                            tag(per_mode[m]["entity_underlyings"], m)
                            for m in valid_modes
                        ]
                    ),
                    _union(
                        [
                            tag(per_mode[m]["non_dated_entities"], m)
                            for m in valid_modes
                        ]
                    ),
                    _union(
                        [
                            tag(per_mode[m]["dated_entities"], m)
                            for m in valid_modes
                        ]
                    ),
                    _union(
                        [
                            tag(per_mode[m]["transfers_adj"], m)
                            for m in valid_modes
                        ]
                    ),
                    checkpoint_fn=checkpoint,
                )
            )
            fused_temp = checkpoint(
                spark, fused_temp, "tcp_by_type_fused", cfg
            )
            fused_transfers = checkpoint(
                spark, fused_transfers, "txfr_adj_fused", cfg
            )
            tagged_non_dated = _union(
                [
                    tag(per_mode[m]["non_dated_entities"], m)
                    for m in valid_modes
                ]
            )
            tagged_dated = _union(
                [
                    tag(per_mode[m]["dated_entities"], m)
                    for m in valid_modes
                ]
            )
            fused_non_dated, fused_dated = (
                business.compute_missing_entities(
                    cfg, tagged_non_dated, tagged_dated, fused_temp
                )
            )
            fused_non_dated = checkpoint(
                spark, fused_non_dated, "nde_post_miss_fused", cfg
            )
            fused_dated = checkpoint(
                spark, fused_dated, "de_post_miss_fused", cfg
            )
            final_cost = business.build_final_cost_percentage(
                fused_temp, entity_partners
            )
            final_cost = checkpoint(
                spark, final_cost, "final_cost_pct_fused", cfg
            )
            if 1 in valid_modes:
                cfg["mode"] = 1
                validation_cfg = fork_cfg(cfg, mode=1)
                if not business.validate_cost_percentage_sum(
                    spark,
                    validation_cfg,
                    final_cost.filter(F.col("_mode") == 1),
                    dar_setup,
                ):
                    raise RuntimeError(
                        "mode 1: Cost percentage does not sum to 100%"
                    )
            cfg["_current_mode"] = 0
            _, min_quarter, fused_dated = business.compute_minimum_quarter(
                spark, cfg, final_cost, fused_dated
            )
            eff_dated, pickup, fused_dated = (
                business.compute_effective_percentage_dated(
                    spark,
                    cfg,
                    fused_dated,
                    final_cost,
                    min_quarter,
                    fused_transfers,
                    entity_partners,
                    line_items,
                    checkpoint_fn=checkpoint,
                )
            )
            if eff_dated is None:
                raise RuntimeError(
                    "compute_effective_percentage_dated returned None"
                )
            eff_dated = checkpoint(
                spark, eff_dated, "eff_dt_fused", cfg
            )
            eff_non_dated = business.compute_effective_percentage_non_dated(
                spark,
                cfg,
                fused_non_dated,
                final_cost,
                min_quarter,
                fused_transfers,
            )
            eff_non_dated = checkpoint(
                spark, eff_non_dated, "eff_nd_fused", cfg
            )
            eff_dated, eff_non_dated = business.apply_plugging(
                spark, cfg, eff_dated, eff_non_dated, dar_setup
            )
            eff_dated = checkpoint(
                spark, eff_dated, "eff_dt_plug_fused", cfg
            )
            eff_non_dated = checkpoint(
                spark, eff_non_dated, "eff_nd_plug_fused", cfg
            )
            eff_dated, eff_non_dated = business.apply_type_id_update(
                cfg,
                eff_dated,
                eff_non_dated,
                cfg.get("_non_dated_entities_cost"),
                cfg.get("_dated_entities_cost"),
            )

            def assemble(mode):
                output_cfg = fork_cfg(cfg, mode=mode)
                data = per_mode[mode]
                result = business.build_final_output(
                    spark,
                    output_cfg,
                    eff_dated.filter(F.col("_mode") == mode).drop("_mode"),
                    eff_non_dated.filter(F.col("_mode") == mode).drop("_mode"),
                    pickup.filter(F.col("_mode") == mode).drop("_mode"),
                    data["entity_underlyings"],
                    data["final_amounts"],
                )
                return result.withColumn("_mode", F.lit(mode))

            assembled = run_group(
                "output_build",
                [
                    (f"mode_{mode}", assemble, (mode,), {})
                    for mode in valid_modes
                ],
            )
            for mode in valid_modes:
                statuses[mode]["result"] = assembled[f"mode_{mode}"]
                statuses[mode]["elapsed_seconds"] = round(
                    time.time() - mode_started[mode], 1
                )
                log_id = cfg.get("log_id")
                if log_id is not None:
                    spark.sql(
                        f"UPDATE {cfg['catalog']}.{cfg['schema']}.AllocationLog "
                        f"SET EndDate = current_timestamp() "
                        f"WHERE LogID = {int(log_id)}"
                    )
            # Match the state left by production's valid-mode Pass C loop.
            cfg["mode"] = valid_modes[-1]
            cfg["_current_mode"] = valid_modes[-1]

        save_return_value = business._save_results(spark, cfg, statuses)
    finally:
        status_out = {
            "statuses": statuses,
            "elapsed_seconds": round(time.time() - started, 1),
            "_checkpoint_tables": list(cfg.get("_checkpoint_tables", [])),
            "_save_return_value": save_return_value,
        }
        business._drop_checkpoints(spark, cfg)
    return status_out


__all__ = ["run_modes_parallel"]
