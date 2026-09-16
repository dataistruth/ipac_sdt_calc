"""Opt-in logical-plan profiling for the updated allocation orchestrator."""

from __future__ import annotations

import functools
import io
import time
from contextlib import redirect_stdout


def _plan_text(df) -> str:
    try:
        return df._jdf.queryExecution().optimizedPlan().treeString()
    except Exception:
        # Spark Connect has no JVM-backed ``_jdf``. Its public explain API
        # requests the plan from the server and prints it to stdout.
        try:
            output = io.StringIO()
            with redirect_stdout(output):
                df.explain(mode="extended")
            return output.getvalue()
        except Exception as exc:
            return f"PLAN_UNAVAILABLE: {type(exc).__name__}: {exc}"


def profile_dataframe(label: str, df, cfg: dict, *, kind: str = "builder"):
    """Record optimized-plan size without triggering a Spark action."""
    if not cfg.get("profile_plan") or not hasattr(df, "columns"):
        return df
    started = time.time()
    text = _plan_text(df)
    lines = [line for line in text.splitlines() if line.strip()]
    available = not text.startswith("PLAN_UNAVAILABLE:")
    lowered = text.lower()
    cfg.setdefault("_plan_profile", []).append(
        {
            "kind": kind,
            "name": label,
            "nodes": len(lines),
            "characters": len(text),
            "inspect_seconds": round(time.time() - started, 4),
            "available": available,
            "broadcast_exchanges": lowered.count("broadcastexchange"),
            "broadcast_hash_joins": lowered.count("broadcasthashjoin"),
            "in_memory_scans": lowered.count("inmemorytablescan"),
        }
    )
    return df


def track_plan(fn):
    """Decorate a DataFrame builder and profile its returned plan when enabled."""
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        result = fn(*args, **kwargs)
        cfg = kwargs.get("cfg")
        if cfg is None:
            cfg = next(
                (arg for arg in args[1:3] if isinstance(arg, dict)),
                {},
            )
        if isinstance(result, tuple):
            for index, value in enumerate(result):
                profile_dataframe(
                    f"{fn.__name__}[{index}]", value, cfg, kind="builder"
                )
        else:
            profile_dataframe(fn.__name__, result, cfg, kind="builder")
        return result

    return wrapped


def plan_profile_report(cfg: dict):
    """Return large plans first and print plans above the configured threshold."""
    rows = sorted(
        cfg.get("_plan_profile", ()),
        key=lambda row: (row["nodes"], row["characters"]),
        reverse=True,
    )
    threshold = int(cfg.get("plan_checkpoint_threshold", 30))
    for row in rows:
        if row["nodes"] >= threshold:
            print(
                f"[plan] {row['kind']}/{row['name']}: "
                f"nodes={row['nodes']} chars={row['characters']}"
            )
    return rows
