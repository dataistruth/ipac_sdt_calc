"""Core plan-size measurement + tracking decorator (Connect-safe, opt-in).

This is the single shared implementation used by every optimized Allocation SP.
It supports two enable mechanisms so it fits both orchestrator styles:

* **ContextVar sink** (``start_plan_profile`` / ``finish_plan_profile``) — for
  SPs that delegate to a production orchestrator owning ``cfg`` internally
  (e.g. ``usp_get_final_effective_percentage``). Records are collected through a
  module-level ``ContextVar`` for the duration of one run, mirroring the SP's
  existing timing/checkpoint-activity ContextVars.
* **cfg flag** (``cfg['profile_plan']``) — for SPs whose orchestrator owns and
  threads ``cfg`` directly (e.g. ``usp_load_footnotes_allocation_to_output``).

``track_plan`` is a transparent passthrough (zero overhead) unless a sink is
active or ``cfg['profile_plan']`` is truthy. Everything is wrapped in
``try/except`` so profiling can never break a pipeline.
"""

from __future__ import annotations

import contextlib
import functools
import io
import logging
import threading
from contextvars import ContextVar

logger = logging.getLogger(__name__)

# Builders may run inside a ThreadPoolExecutor, so guard the shared sink.
_LOCK = threading.Lock()

# Active per-run record sink (list) or None when profiling is disabled.
_PLAN_SINK: ContextVar[list | None] = ContextVar(
    "alloc_plan_profile_sink", default=None
)

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


# --------------------------------------------------------------------------- #
# DataFrame / cfg discovery (duck-typed; classic + Connect safe)
# --------------------------------------------------------------------------- #
def _is_dataframe(obj: object) -> bool:
    return (
        hasattr(obj, "explain")
        and hasattr(obj, "schema")
        and hasattr(obj, "columns")
    )


def _first_dataframe(result: object):
    if _is_dataframe(result):
        return result
    if isinstance(result, (tuple, list)):
        for item in result:
            if _is_dataframe(item):
                return item
    return None


def _find_cfg(args: tuple, kwargs: dict) -> dict | None:
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


# --------------------------------------------------------------------------- #
# Plan measurement
# --------------------------------------------------------------------------- #
def _extract_section(text: str, header: str) -> str | None:
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
            return jdf.queryExecution().optimizedPlan().numberedTreeString()
        except Exception:
            pass  # fall through to the portable path

    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            df.explain(mode="extended")
    except Exception:
        return None
    full = buffer.getvalue()
    if not full.strip():
        return None
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
    could not be inspected. Never raises. Uses analyze-only APIs (no Spark job).
    """
    try:
        text = _optimized_plan_text(df)
        if not text:
            return None
        return _metrics_from_plan_text(text)
    except Exception:
        logger.debug("[PLAN] measure_plan failed", exc_info=True)
        return None


# --------------------------------------------------------------------------- #
# Run activation (ContextVar sink) + decorator
# --------------------------------------------------------------------------- #
def start_plan_profile() -> tuple:
    """Activate a fresh record sink for one invocation.

    Returns ``(token, records)`` — pass ``token`` to :func:`finish_plan_profile`
    and read ``records`` for the report.
    """
    records: list = []
    token = _PLAN_SINK.set(records)
    return token, records


def finish_plan_profile(token) -> None:
    if token is None:
        return
    try:
        _PLAN_SINK.reset(token)
    except Exception:
        logger.debug("[PLAN] finish_plan_profile reset failed", exc_info=True)


def track_plan(fn):
    """Decorator: record a builder's contribution to logical-plan size.

    No-op unless a plan sink is active (see :func:`start_plan_profile`) or the
    threaded ``cfg`` has ``profile_plan`` truthy. Attributes
    ``delta = output_nodes - max(input_nodes)`` to ``fn.__name__``.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        sink = _PLAN_SINK.get()
        cfg = _find_cfg(args, kwargs)
        enabled = sink is not None or (
            isinstance(cfg, dict) and cfg.get("profile_plan")
        )
        if not enabled:
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
                target = (
                    sink
                    if sink is not None
                    else cfg.setdefault("_plan_profile", [])
                )
                with _LOCK:
                    target.append(record)
                logger.info(
                    "[PLAN] %s: nodes=%d depth=%d (+%d)",
                    fn.__name__,
                    record["nodes"],
                    record["depth"],
                    record["delta"],
                )
        return result

    return wrapper


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def plan_profile_report(source, threshold: int | None = None) -> list:
    """Print and return the plan-growth ranking.

    ``source`` may be the records ``list`` returned by :func:`start_plan_profile`
    or a ``cfg`` dict carrying ``_plan_profile``. Empty when nothing recorded.
    """
    if isinstance(source, dict):
        records = list(source.get("_plan_profile", []))
        if threshold is None:
            threshold = source.get(
                "plan_checkpoint_threshold", _DEFAULT_CHECKPOINT_THRESHOLD
            )
    else:
        records = list(source or [])
    if threshold is None:
        threshold = _DEFAULT_CHECKPOINT_THRESHOLD
    threshold = int(threshold)

    if not records:
        return []

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
