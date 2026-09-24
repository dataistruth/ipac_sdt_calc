"""Configuration isolation and deterministic artifact merging for parallel stages."""

from __future__ import annotations

import threading
from typing import Any

SHARED_CHECKPOINT_KEYS = frozenset(
    {
        "_checkpoint_v2_state",
        "_checkpoint_v2_activity",
        "_checkpoint_policy_activity",
        "_checkpoint_tables",
        "_checkpoint_paths",
    }
)

PRIVATE_BRANCH_KEYS = frozenset(
    {
        "mode",
        "_current_mode",
        "_alloc_empty",
        "_lt_empty",
        "_sm_empty",
        "_inputs_empty",
        "_part_v_quarters_df",
        "_non_dated_entities_cost",
        "_dated_entities_cost",
    }
)


class ThreadSafeList(list):
    """Minimal list-compatible append collection used by Common_V2."""

    def __init__(self, values=()):
        super().__init__(values)
        self._lock = threading.RLock()

    def append(self, value):
        with self._lock:
            return super().append(value)

    def extend(self, values):
        with self._lock:
            return super().extend(values)

    def __len__(self):
        with self._lock:
            return super().__len__()

    def __getitem__(self, item):
        with self._lock:
            return super().__getitem__(item)

    def snapshot(self):
        with self._lock:
            return list(super().__iter__())


def ensure_thread_safe_checkpoint_collections(cfg: dict) -> None:
    """Install list-compatible locked collections before cfg forks exist."""
    for key in (
        "_checkpoint_v2_activity",
        "_checkpoint_policy_activity",
        "_checkpoint_tables",
        "_checkpoint_paths",
    ):
        value = cfg.get(key, ())
        if not isinstance(value, ThreadSafeList):
            cfg[key] = ThreadSafeList(value)


def fork_cfg(cfg: dict, *, mode: int, current_mode: int | None = None) -> dict:
    """Make a shallow branch fork with only checkpoint structures shared.

    DataFrames and immutable configuration values remain shallow references.
    Mutable list/dict/set values are copied unless they belong to the explicit
    thread-safe checkpoint coordination contract.
    """
    fork = {}
    for key, value in cfg.items():
        if key in SHARED_CHECKPOINT_KEYS:
            fork[key] = value
        elif isinstance(value, dict):
            fork[key] = dict(value)
        elif isinstance(value, list):
            fork[key] = list(value)
        elif isinstance(value, set):
            fork[key] = set(value)
        else:
            fork[key] = value
    for key in PRIVATE_BRANCH_KEYS:
        fork.pop(key, None)
    fork["mode"] = mode
    fork["_current_mode"] = mode if current_mode is None else current_mode
    return fork


def _schema_signature(value: Any):
    schema = getattr(value, "schema", None)
    if schema is None:
        return None
    try:
        return schema.simpleString()
    except Exception:
        return str(schema)


def merge_mode_artifacts(
    root_cfg: dict,
    mode_cfgs: dict[int, dict],
) -> list[dict]:
    """Merge the one Pass-B/C artifact with production-order semantics.

    `_part_v_quarters_df` is derived only from RunID and shared source tables.
    Production modes run in numeric order, so mode 2 overwrites mode 1 when
    both produce it. Parallel execution preserves that winner and rejects
    incompatible schemas instead of silently accepting a conflicting value.
    """
    events = []
    producers = [
        (mode, cfg["_part_v_quarters_df"])
        for mode, cfg in sorted(mode_cfgs.items())
        if cfg.get("_part_v_quarters_df") is not None
    ]
    if producers:
        signatures = {
            signature
            for _, value in producers
            if (signature := _schema_signature(value)) is not None
        }
        if len(signatures) > 1:
            raise RuntimeError(
                "Conflicting _part_v_quarters_df schemas from mode forks: "
                f"{[(mode, _schema_signature(value)) for mode, value in producers]}"
            )
        winner_mode, winner = producers[-1]
        root_cfg["_part_v_quarters_df"] = winner
        events.append(
            {
                "artifact": "_part_v_quarters_df",
                "producers": [mode for mode, _ in producers],
                "winner_mode": winner_mode,
                "conflict_check": "schema_match",
            }
        )
    else:
        root_cfg.pop("_part_v_quarters_df", None)
    return events


__all__ = [
    "PRIVATE_BRANCH_KEYS",
    "SHARED_CHECKPOINT_KEYS",
    "ThreadSafeList",
    "ensure_thread_safe_checkpoint_collections",
    "fork_cfg",
    "merge_mode_artifacts",
]
