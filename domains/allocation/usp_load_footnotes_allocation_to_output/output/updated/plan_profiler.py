"""Plan-size profiler for the updated footnote-allocation pipeline.

Purpose
-------
Measure how much each builder function grows the Spark *logical plan* (the
DAG that Catalyst analyzes and whole-stage-codegens). On small inputs the
dominant cost of this pipeline is planning/codegen of very large logical
plans, not data movement — so knowing *which function* inflates the plan tells
you exactly where a ``checkpoint`` (lineage break) will help most.

Design
------
* ``measure_plan(df)`` is **Spark Connect safe**: it never touches ``_jdf`` on
  Connect. It captures ``df.explain(mode="extended")`` (an analyze-only call —
  no Spark job, no data scan) and parses the *Optimized Logical Plan* section.
  On classic clusters it uses a faster ``_jdf`` tree walk when available.
* ``track_plan`` is a decorator for builder functions. When
  ``cfg['profile_plan']`` is falsy it is a transparent passthrough (zero
  overhead in production). When enabled it records, per function, the output
  plan node-count and the growth (delta) versus its largest DataFrame input.
* ``plan_profile_report(cfg)`` ranks the recorded functions by growth and
  flags checkpoint candidates.

Everything is wrapped in ``try/except`` so profiling can never break a run.
"""

from __future__ import annotations

import contextlib
import functools
import io
import logging
import threading

logger = logging.getLogger(__name__)

# Builders run inside a ThreadPoolExecutor, so guard the shared record list.
_LOCK = threading.Lock()

# Operators worth counting individually in the per-function histogram.
_OPERATORS = (
    "Join",
    "Union",
    "Project",
    "Filter",
    "Aggregate",
    "Window",
    "Generate",
    "Expand",
    "Sort",
)
_SCAN_MARKERS = ("Relation", "Scan", "LogicalRDD", "InMemory")

_DEFAULT_CHECKPOINT_THRESHOLD = 30


def _is_dataframe(obj: object) -> bool:
    """Duck-typed DataFrame check that works on classic *and* Connect."""
    return hasattr(obj, "explain") and hasattr(obj, "schema") and hasattr(
        obj, "columns"
    )


def _first_dataframe(result: object):
    """Return the first DataFrame in a return value (handles tuples/lists)."""
    if _is_dataframe(result):
        return result
    if isinstance(result, (tuple, list)):
        for item in result:
            if _is_dataframe(item):
                return item
    return None


def _find_cfg(args: tuple, kwargs: dict) -> dict | None:
    """Locate the pipeline ``cfg`` dict among a call's arguments."""
    candidate = kwargs.get("cfg")
    if isinstance(candidate, dict):
        return candidate
    for value in (*args, *kwargs.values()):
        if isinstance(value, dict) and (
            "run_id" in value
            or "_checkpoint_tables" in value
            or "profile_plan" in value
        ):
            return value
    return None


def _find_dataframes(values) -> list:
    return [v for v in values if _is_dataframe(v)]


def _extract_section(text: str, header: str) -> str | None:
    """Slice out one ``== <header> ==`` block from an extended explain dump."""
    marker = f"== {header} =="
    start = text.find(marker)
    if start == -1:
        return None
    start += len(marker)
    nxt = text.find("== ", start)
    return text[start:nxt] if nxt != -1 else text[start:]


def _optimized_plan_text(df) -> str | None:
    """Return the optimized-logical-plan text for a DataFrame, or None.

    Classic fast path uses ``_jdf`` when present; otherwise falls back to the
    portable, Connect-safe ``explain(mode="extended")`` capture.
    """
    jdf = getattr(df, "_jdf", None)
    if jdf is not None:
        try:
            return (
                jdf.queryExecution().optimizedPlan().numberedTreeString()
            )
        except Exception:
            pass  # fall through to portable path

    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            df.explain(mode="extended")
    except Exception:
        return None
    full = buffer.getvalue()
    if not full.strip():
        return None
    # Prefer the optimized logical plan; fall back to analyzed, then whole dump.
    return (
        _extract_section(full, "Optimized Logical Plan")
        or _extract_section(full, "Analyzed Logical Plan")
        or full
    )


def _metrics_from_plan_text(text: str) -> dict:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    depth = 0
    for ln in lines:
        prefix_len = len(ln) - len(ln.lstrip(" :+-|"))
        depth = max(depth, prefix_len // 3)

    ops: dict[str, int] = {}
    for kw in _OPERATORS:
        count = sum(1 for ln in lines if kw in ln)
        if count:
            ops[kw] = count
    scans = sum(
        1 for ln in lines if any(marker in ln for marker in _SCAN_MARKERS)
    )
    if scans:
        ops["Scan"] = scans

    return {
        "nodes": len(lines),
        "depth": depth,
        "chars": len(text),
        "ops": ops,
    }


def measure_plan(df) -> dict | None:
    """Measure logical-plan size for ``df``.

    Returns ``{"nodes", "depth", "chars", "ops"}`` or ``None`` if the plan
    could not be inspected. Never raises.
    """
    try:
        text = _optimized_plan_text(df)
        if not text:
            return None
        return _metrics_from_plan_text(text)
    except Exception:
        logger.debug("[PLAN] measure_plan failed", exc_info=True)
        return None


def track_plan(fn):
    """Decorator: record a builder's contribution to logical-plan size.

    No-op unless ``cfg['profile_plan']`` is truthy. Attributes
    ``delta = output_nodes - max(input_nodes)`` to ``fn.__name__``.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        cfg = _find_cfg(args, kwargs)
        if not (isinstance(cfg, dict) and cfg.get("profile_plan")):
            return fn(*args, **kwargs)

        input_nodes = 0
        for df_in in _find_dataframes((*args, *kwargs.values())):
            metrics = measure_plan(df_in)
            if metrics:
                input_nodes = max(input_nodes, metrics["nodes"])

        result = fn(*args, **kwargs)

        out_df = _first_dataframe(result)
        if out_df is not None:
            metrics = measure_plan(out_df)
            if metrics:
                record = {
                    "func": fn.__name__,
                    "nodes": metrics["nodes"],
                    "depth": metrics["depth"],
                    "delta": metrics["nodes"] - input_nodes,
                    "ops": metrics["ops"],
                }
                with _LOCK:
                    cfg.setdefault("_plan_profile", []).append(record)
                logger.info(
                    "[PLAN] %s: nodes=%d depth=%d (+%d)",
                    fn.__name__,
                    record["nodes"],
                    record["depth"],
                    record["delta"],
                )
        return result

    return wrapper


def plan_profile_report(cfg: dict) -> list:
    """Print and return the plan-growth ranking. Empty when nothing recorded."""
    records = list(cfg.get("_plan_profile", [])) if isinstance(cfg, dict) else []
    if not records:
        return []

    threshold = int(
        cfg.get("plan_checkpoint_threshold", _DEFAULT_CHECKPOINT_THRESHOLD)
    )
    ranked = sorted(records, key=lambda r: r["delta"], reverse=True)

    print("[PLAN REPORT] ranked by plan-node growth (delta vs largest input)")
    for r in ranked:
        ops = " ".join(f"{k}={v}" for k, v in sorted(r["ops"].items()))
        flag = "  <-- checkpoint candidate" if r["delta"] >= threshold else ""
        print(
            f"  {r['func']:<34} nodes={r['nodes']:>4} depth={r['depth']:>3} "
            f"(+{r['delta']}){('  ' + ops) if ops else ''}{flag}"
        )
    return ranked
