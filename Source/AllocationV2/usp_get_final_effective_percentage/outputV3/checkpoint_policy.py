"""Name-based checkpoint policy for Final Effective Percentage outputV3."""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
)

from .cfg_isolation import ensure_thread_safe_checkpoint_collections


def _incoming_plan_metrics(df, enabled: bool) -> dict:
    """Inspect logical-plan metadata without starting a Spark action."""
    if not enabled:
        return {
            "incoming_plan_nodes": None,
            "incoming_plan_depth": None,
            "incoming_partitions": None,
        }
    try:
        tree = (
            df._jdf.queryExecution()
            .optimizedPlan()
            .numberedTreeString()
        )
        lines = [line for line in tree.splitlines() if line.strip()]
        depths = [
            max(0, (len(line) - len(line.lstrip(" |:+-"))) // 2)
            for line in lines
        ]
    except Exception:
        lines, depths = [], []
    try:
        partitions = int(
            df._jdf.queryExecution()
            .sparkPlan()
            .outputPartitioning()
            .numPartitions()
        )
    except Exception:
        partitions = None
    return {
        "incoming_plan_nodes": len(lines) or None,
        "incoming_plan_depth": max(depths, default=0) if lines else None,
        "incoming_partitions": partitions,
    }


@dataclass(frozen=True)
class CheckpointDecision:
    backend: str
    reason: str
    stage: str


_DURABLE_RULES = (
    (
        re.compile(r"^(all_und_common|input_lines|entity_und_common)_(lt|nolt)$"),
        "branch join boundary",
        "with_lt_or_no_lt_branch",
    ),
    (
        re.compile(r"^(all_und_final|fn_input_lines|nde_pre_cpbt|de_pre_cpbt)_m\d+$"),
        "mode-prep branch join or reused input",
        "mode_prep",
    ),
    (
        re.compile(r"^txfr_(pre_cpbt_m\d+|adj_fused)$"),
        "alias-sensitive transfer seam",
        "fused_cpbt",
    ),
    (
        re.compile(r"^(all_ent|all_ent_pre_tag|tcp_post_tag)_m\d+$"),
        "expensive reused fused CPBT intermediate",
        "fused_cpbt",
    ),
    (
        re.compile(
            r"^(tcp_by_type_(fused|m4)|nde_post_miss_(fused|m4)|"
            r"de_post_miss_(fused|m4)|final_cost_pct_(fused|m4))$"
        ),
        "reused fused output",
        "fused_cpbt",
    ),
    (
        re.compile(
            r"^(eff_pct_dated_post_transfer|pickup_s4_m\d+|eff_dated_s[56]_m\d+|"
            r"pickup_order_dated_pre_yearly|eff_dt_fused|eff_nd_fused|"
            r"eff_dt_plug_fused|eff_nd_plug_fused)$"
        ),
        "alias-sensitive or reused effective seam",
        "fused_effective",
    ),
)


def decide_checkpoint(name: str) -> CheckpointDecision:
    """Select a backend solely from the semantic checkpoint name."""
    normalized = str(name)
    for pattern, reason, stage in _DURABLE_RULES:
        if pattern.match(normalized):
            if stage == "with_lt_or_no_lt_branch":
                stage = (
                    "no_lt_branch"
                    if normalized.endswith("_nolt")
                    else "with_lt_branch"
                )
            return CheckpointDecision("delta", reason, stage)
    if normalized.startswith(
        (
            "cost_pct_",
            "underlyings_",
            "uc_ordered_",
            "tcp_with_yearly_",
            "all_und_m4",
            "entity_und_m4",
        )
    ):
        return CheckpointDecision("delta", "reused common relation", "common_reads")
    return CheckpointDecision("local", "safe cheap lineage break", "mode_prep")


def initialize_named_checkpoint_policy(
    cfg: dict, checkpoint_mode: int = 1
) -> None:
    """Initialize shared V2 state for modes 1..5."""
    initialize_checkpoint_V2(cfg, checkpoint_mode=checkpoint_mode)
    cfg["_output_v3_checkpoint_mode"] = int(checkpoint_mode)
    cfg["_checkpoint_v2_activity"] = []
    cfg["_checkpoint_policy_activity"] = []
    ensure_thread_safe_checkpoint_collections(cfg)


def named_checkpoint(spark, df, name: str, cfg: dict):
    """Materialize with the configured Common_V2 checkpoint mode."""
    decision = decide_checkpoint(name)
    target_applied = False
    if name == cfg.get("_output_v3_target_checkpoint"):
        strategy = cfg.get("_output_v3_target_partition_strategy", "off")
        partitions = int(cfg.get("_output_v3_target_partitions", 0) or 0)
        if strategy == "coalesce":
            df = df.coalesce(partitions)
            target_applied = True
        elif strategy == "repartition":
            keys = list(cfg.get("_output_v3_target_partition_keys", ()))
            unknown = sorted(set(keys) - set(df.columns))
            if unknown:
                raise ValueError(
                    f"Unknown repartition keys for {name}: {unknown}"
                )
            columns = [df[key] for key in keys]
            df = df.repartition(partitions, *columns)
            target_applied = True
    plan_metrics = _incoming_plan_metrics(
        df, bool(cfg.get("profile_plan", False))
    )
    activity = cfg.setdefault("_checkpoint_v2_activity", [])
    before = len(activity)
    started = time.time()
    started_at = datetime.now().isoformat()
    thread_name = threading.current_thread().name
    checkpoint_mode = int(cfg.get("_output_v3_checkpoint_mode", 1))
    print(
        f"[outputV3 checkpoint] START name={name} "
        f"stage={decision.stage} mode={checkpoint_mode} "
        f"thread={thread_name} at={started_at}",
        flush=True,
    )
    try:
        result = checkpoint_V2(
            spark, df, name, cfg, checkpoint_mode=checkpoint_mode
        )
    except Exception as exc:
        elapsed = time.time() - started
        print(
            f"[outputV3 checkpoint] FAIL name={name} "
            f"stage={decision.stage} mode={checkpoint_mode} "
            f"elapsed={elapsed:.3f}s error={type(exc).__name__}: {exc}",
            flush=True,
        )
        raise
    own_activity = next(
        (
            item
            for item in reversed(activity[before:])
            if item.get("name") == name
        ),
        None,
    )
    actual_backend = (
        own_activity.get("backend") if own_activity else decision.backend
    )
    requested_backend = (
        own_activity.get("requested_backend")
        if own_activity
        else decision.backend
    )
    local_checkpoint_eager = (
        own_activity.get("local_checkpoint_eager")
        if own_activity
        else None
    )
    if actual_backend == "local":
        # Match the qualifier reset of a fresh spark.table relation.
        result = result.toDF(*result.columns)
    ended_at = datetime.now().isoformat()
    materialization = (
        "eager"
        if local_checkpoint_eager
        else "deferred"
        if local_checkpoint_eager is False
        else "durable"
    )
    cfg.setdefault("_checkpoint_policy_activity", []).append(
        {
            "name": name,
            "stage": decision.stage,
            "checkpoint_mode": checkpoint_mode,
            "policy_backend": requested_backend,
            "actual_backend": actual_backend,
            "materialization": materialization,
            "reason": f"checkpoint_V2 mode {checkpoint_mode}; {decision.reason}",
            "thread": thread_name,
            "started_at": started_at,
            "ended_at": ended_at,
            "elapsed_seconds": round(time.time() - started, 3),
            "target_partition_applied": target_applied,
            "target_partition_strategy": (
                cfg.get("_output_v3_target_partition_strategy", "off")
                if target_applied
                else "off"
            ),
            "target_partitions": (
                int(cfg.get("_output_v3_target_partitions", 0) or 0)
                if target_applied
                else None
            ),
            "target_partition_keys": (
                list(cfg.get("_output_v3_target_partition_keys", ()))
                if target_applied
                else []
            ),
            **plan_metrics,
        }
    )
    elapsed = time.time() - started
    print(
        f"[outputV3 checkpoint] DONE name={name} "
        f"stage={decision.stage} mode={checkpoint_mode} "
        f"backend={actual_backend} "
        f"materialization={materialization} "
        f"thread={thread_name} at={ended_at} elapsed={elapsed:.3f}s",
        flush=True,
    )
    return result


def drop_failed_run_checkpoints(spark, cfg: dict) -> None:
    """Clean durable artifacts when no result DataFrame can be returned."""
    drop_checkpoints_V2(spark, cfg)


__all__ = [
    "CheckpointDecision",
    "decide_checkpoint",
    "drop_failed_run_checkpoints",
    "initialize_named_checkpoint_policy",
    "named_checkpoint",
]
