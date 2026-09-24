"""Name-based checkpoint policy for Final Effective Percentage outputV3."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass

from Common_V2.core.checkpoint_V2 import (
    checkpoint_V2,
    drop_checkpoints_V2,
    initialize_checkpoint_V2,
)

from .cfg_isolation import ensure_thread_safe_checkpoint_collections


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


def initialize_named_checkpoint_policy(cfg: dict) -> None:
    """Initialize shared V2 state while disabling sequence-based selection."""
    initialize_checkpoint_V2(cfg, checkpoint_mode=1)
    cfg["_checkpoint_v2_activity"] = []
    cfg["_checkpoint_policy_activity"] = []
    ensure_thread_safe_checkpoint_collections(cfg)


def named_checkpoint(spark, df, name: str, cfg: dict):
    """Materialize with a backend chosen by checkpoint name, never sequence."""
    decision = decide_checkpoint(name)
    activity = cfg.setdefault("_checkpoint_v2_activity", [])
    before = len(activity)
    started = time.time()
    # V2 mode 1 is always Delta; mode 4 is always local. The semantic policy
    # chooses between those primitives and never uses the V2 odd/even modes.
    primitive_mode = 1 if decision.backend == "delta" else 4
    result = checkpoint_V2(
        spark, df, name, cfg, checkpoint_mode=primitive_mode
    )
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
    if actual_backend == "local":
        # Match the qualifier reset of a fresh spark.table relation.
        result = result.toDF(*result.columns)
    cfg.setdefault("_checkpoint_policy_activity", []).append(
        {
            "name": name,
            "stage": decision.stage,
            "policy_backend": decision.backend,
            "actual_backend": actual_backend,
            "reason": decision.reason,
            "elapsed_seconds": round(time.time() - started, 3),
        }
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
