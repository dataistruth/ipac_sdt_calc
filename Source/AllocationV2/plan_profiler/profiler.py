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
import time
from collections import Counter
from contextvars import ContextVar

logger = logging.getLogger(__name__)

# Builders may run inside a ThreadPoolExecutor, so guard the shared sink.
_LOCK = threading.Lock()

# Active per-run BUILDER record sink (list) or None when profiling is disabled.
_PLAN_SINK: ContextVar[list | None] = ContextVar(
    "alloc_plan_profile_sink", default=None
)

# Active per-run CHECKPOINT record sink (list) or None when disabled. Kept
# SEPARATE from _PLAN_SINK so builder-level growth and checkpoint-level
# truncation are reported independently (a builder profile answers "where does
# the plan grow?"; a checkpoint profile answers "how big is the plan each break
# truncates?").
_CHECKPOINT_SINK: ContextVar[list | None] = ContextVar(
    "alloc_checkpoint_profile_sink", default=None
)

# Active per-run ACTION record sink. Actions must be instrumented explicitly;
# Spark has no safe DataFrame-level hook that identifies the input plan for all
# count/isEmpty/collect/write calls, especially under Spark Connect.
_ACTION_SINK: ContextVar[list | None] = ContextVar(
    "alloc_action_profile_sink", default=None
)

# Master switch. start_* sets this True. disable_plan_profile / finish clears
# it. measure_plan / explain must not run when this is False — leftover
# ContextVar sinks from a prior ProfilePlan=on cell would otherwise add
# explain cost to the next A/B updated run.
_PROFILING_ACTIVE: ContextVar[bool] = ContextVar(
    "alloc_plan_profile_active", default=False
)

# Process-wide sinks. ContextVars do not copy into ThreadPoolExecutor workers,
# and FEP / footnote SPs checkpoint inside those workers. A leftover on-run
# still cannot tax the next off-run because disable_plan_profile() clears these.
_CFG_KEYS = {
    "plan": "_plan_profile",
    "checkpoint": "_checkpoint_plan_profile",
    "action": "_action_plan_profile",
}
_ACTIVE = {
    "profiling": False,
    "plan": None,
    "checkpoint": None,
    "action": None,
}


def _set_active_sink(kind: str, records: list) -> None:
    with _LOCK:
        _ACTIVE["profiling"] = True
        _ACTIVE[kind] = records


def _clear_active_sinks() -> None:
    with _LOCK:
        _ACTIVE["profiling"] = False
        _ACTIVE["plan"] = None
        _ACTIVE["checkpoint"] = None
        _ACTIVE["action"] = None


def _sink_list(kind: str, cfg: dict | None = None):
    with _LOCK:
        sink = _ACTIVE.get(kind)
    if sink is not None:
        return sink
    ctx = {
        "plan": _PLAN_SINK,
        "checkpoint": _CHECKPOINT_SINK,
        "action": _ACTION_SINK,
    }[kind].get()
    if ctx is not None:
        return ctx
    if isinstance(cfg, dict):
        return cfg.setdefault(_CFG_KEYS[kind], [])
    return None


def profiling_enabled(cfg: dict | None = None) -> bool:
    """True only when the notebook ProfilePlan widget armed this invocation.

    ``cfg['profile_plan']`` wins when that key is present so a leftover
    sink cannot force ``df.explain`` after the widget is turned off.
    """
    if isinstance(cfg, dict) and "profile_plan" in cfg:
        return bool(cfg.get("profile_plan"))
    with _LOCK:
        if _ACTIVE["profiling"]:
            return True
    return bool(_PROFILING_ACTIVE.get())


def disable_plan_profile() -> None:
    """Drop leftover sinks. Call at the start of every outputV2 run."""
    global _EXPLAIN_SKIP
    _PROFILING_ACTIVE.set(False)
    _PLAN_SINK.set(None)
    _CHECKPOINT_SINK.set(None)
    _ACTION_SINK.set(None)
    _clear_active_sinks()
    with _EXPLAIN_SKIP_LOCK:
        _EXPLAIN_SKIP = False


def _empty_metrics(reason: str = "explain_unavailable") -> dict:
    return {"nodes": 0, "depth": 0, "chars": 0, "ops": {"explain": reason}}


# After one explain/analyze failure, skip the rest of the run. A failed
# Connect analyze can surface in the notebook even when Python catches it.
_EXPLAIN_SKIP = False
_EXPLAIN_SKIP_LOCK = threading.Lock()


def _explain_skip_armed() -> bool:
    with _EXPLAIN_SKIP_LOCK:
        return _EXPLAIN_SKIP


def _arm_explain_skip(reason: str) -> None:
    global _EXPLAIN_SKIP
    with _EXPLAIN_SKIP_LOCK:
        already = _EXPLAIN_SKIP
        _EXPLAIN_SKIP = True
    if not already:
        print(f"[plan-profile] measure/explain skipped: {reason}")
        logger.info("[PLAN] measure/explain skipped: %s", reason)

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

# Durable copy for A/B last cells. Survives ContextVar reset and is what
# notebooks should read: AllocationV2.plan_profiler.LAST_PLAN_PROFILE
LAST_PLAN_PROFILE = {"builder": [], "checkpoint": [], "action": []}


def store_last_plan_profile(builder=None, checkpoint=None, action=None):
    LAST_PLAN_PROFILE["builder"] = list(builder or [])
    LAST_PLAN_PROFILE["checkpoint"] = list(checkpoint or [])
    LAST_PLAN_PROFILE["action"] = list(action or [])


def _remember(kind: str, record: dict) -> None:
    key = {"plan": "builder", "builder": "builder"}.get(kind, kind)
    if key not in LAST_PLAN_PROFILE:
        key = kind if kind in LAST_PLAN_PROFILE else "builder"
    with _LOCK:
        LAST_PLAN_PROFILE[key].append(dict(record))


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

    Never raises. After one analyze/explain failure, skip the rest of
    the run so Connect ``UNRESOLVED_COLUMN`` cannot fail the A/B cell.
    If ``_explain_string`` exists and fails, do not also call ``df.explain``.
    """
    if _explain_skip_armed():
        return None
    try:
        explainer = getattr(df, "_explain_string", None)
        if callable(explainer):
            for kwargs in (
                {"extended": True},
                {"mode": "extended"},
                {"extended": True, "mode": "extended"},
                {},
            ):
                try:
                    text = explainer(**kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    _arm_explain_skip(f"{type(exc).__name__}: {exc}")
                    return None
                if text and str(text).strip():
                    full = str(text)
                    return (
                        _extract_section(full, "Optimized Logical Plan")
                        or _extract_section(full, "Analyzed Logical Plan")
                        or full
                    )
            return None

        jdf = getattr(df, "_jdf", None)
        if jdf is not None:
            try:
                return jdf.queryExecution().optimizedPlan().numberedTreeString()
            except Exception as exc:
                _arm_explain_skip(f"{type(exc).__name__}: {exc}")
                return None

        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer):
                df.explain(mode="extended")
        except TypeError:
            try:
                with contextlib.redirect_stdout(buffer):
                    df.explain(True)
            except Exception as exc:
                _arm_explain_skip(f"{type(exc).__name__}: {exc}")
                return None
        except Exception as exc:
            _arm_explain_skip(f"{type(exc).__name__}: {exc}")
            return None
        full = buffer.getvalue()
        if not full.strip():
            return None
        return (
            _extract_section(full, "Optimized Logical Plan")
            or _extract_section(full, "Analyzed Logical Plan")
            or full
        )
    except Exception as exc:
        _arm_explain_skip(f"{type(exc).__name__}: {exc}")
        return None


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


def measure_plan(df, cfg: dict | None = None) -> dict | None:
    """Measure logical-plan size for ``df``.

    Returns ``{"nodes", "depth", "chars", "ops"}`` or ``None`` if the plan
    could not be inspected. Never raises. Uses analyze-only APIs (no Spark job).
    Does **not** call ``explain`` unless ProfilePlan armed this run.
    """
    if not profiling_enabled(cfg):
        return None
    if _explain_skip_armed():
        return None
    try:
        text = _optimized_plan_text(df)
        if not text:
            return None
        return _metrics_from_plan_text(text)
    except Exception as exc:
        _arm_explain_skip(f"{type(exc).__name__}: {exc}")
        return None


# --------------------------------------------------------------------------- #
# Run activation (ContextVar sink) + decorator
# --------------------------------------------------------------------------- #
def start_plan_profile() -> tuple:
    """Activate a fresh record sink for one invocation.

    Returns ``(token, records)`` — pass ``token`` to :func:`finish_plan_profile`
    and read ``records`` for the report.
    """
    global _EXPLAIN_SKIP
    records: list = []
    _PROFILING_ACTIVE.set(True)
    _set_active_sink("plan", records)
    LAST_PLAN_PROFILE["builder"] = []
    LAST_PLAN_PROFILE["checkpoint"] = []
    LAST_PLAN_PROFILE["action"] = []
    with _EXPLAIN_SKIP_LOCK:
        _EXPLAIN_SKIP = False
    token = _PLAN_SINK.set(records)
    return token, records


def finish_plan_profile(token) -> None:
    if token is None:
        return
    try:
        _PLAN_SINK.reset(token)
    except Exception:
        logger.debug("[PLAN] finish_plan_profile reset failed", exc_info=True)


# --------------------------------------------------------------------------- #
# Checkpoint-level plan profile (separate sink)
# --------------------------------------------------------------------------- #
def start_checkpoint_plan_profile() -> tuple:
    """Activate a fresh CHECKPOINT record sink for one invocation.

    Returns ``(token, records)`` — pass ``token`` to
    :func:`finish_checkpoint_plan_profile`. Independent of the builder sink.
    """
    records: list = []
    _PROFILING_ACTIVE.set(True)
    _set_active_sink("checkpoint", records)
    token = _CHECKPOINT_SINK.set(records)
    return token, records


def finish_checkpoint_plan_profile(token) -> None:
    if token is None:
        return
    try:
        _CHECKPOINT_SINK.reset(token)
    except Exception:
        logger.debug(
            "[PLAN] finish_checkpoint_plan_profile reset failed", exc_info=True
        )


def start_action_profile() -> tuple:
    """Activate a fresh ACTION record sink for one invocation."""
    records: list = []
    _PROFILING_ACTIVE.set(True)
    _set_active_sink("action", records)
    token = _ACTION_SINK.set(records)
    return token, records


def finish_action_profile(token) -> None:
    if token is None:
        return
    try:
        _ACTION_SINK.reset(token)
    except Exception:
        logger.debug("[PLAN] finish_action_profile reset failed", exc_info=True)


def _record_action(
    name: str,
    df,
    cfg: dict | None,
    elapsed_seconds: float | None,
) -> None:
    if not profiling_enabled(cfg):
        return
    try:
        metrics = measure_plan(df, cfg)
        if not metrics:
            return
        record = {
            "func": name,
            "nodes": metrics["nodes"],
            "depth": metrics["depth"],
            "delta": metrics["nodes"],
            "ops": metrics["ops"],
            "elapsed_seconds": (
                round(float(elapsed_seconds), 3)
                if elapsed_seconds is not None
                else None
            ),
        }
        target = _sink_list("action", cfg)
        if target is not None:
            with _LOCK:
                target.append(record)
        _remember("action", record)
    except Exception:
        logger.debug("[PLAN-ACTION] record failed", exc_info=True)


def track_action_plan(
    name: str,
    df,
    cfg: dict | None = None,
    elapsed_seconds: float | None = None,
) -> None:
    """Record a known action's input plan without executing the action."""
    _record_action(name, df, cfg, elapsed_seconds)


def profile_action(name: str, df, action, cfg: dict | None = None):
    """Execute a zero-argument Spark action and profile its input plan + time.

    Example: ``profile_action("warnings.isEmpty", df, df.isEmpty, cfg)``.
    Profiling is a transparent pass-through when disabled. Exceptions from the
    action are propagated unchanged.
    """
    if not profiling_enabled(cfg):
        return action()
    metrics = measure_plan(df, cfg)
    started = time.perf_counter()
    try:
        return action()
    finally:
        elapsed = time.perf_counter() - started
        if metrics:
            record = {
                "func": name,
                "nodes": metrics["nodes"],
                "depth": metrics["depth"],
                "delta": metrics["nodes"],
                "ops": metrics["ops"],
                "elapsed_seconds": round(elapsed, 3),
            }
            target = _sink_list("action", cfg)
            if target is not None:
                with _LOCK:
                    target.append(record)
            _remember("action", record)


def track_checkpoint_plan(name: str, df, cfg=None) -> None:
    """Record the plan a checkpoint truncates — measured BEFORE the lineage break.

    Call this inside ``checkpoint()`` on the *incoming* DataFrame (pre
    materialization), so the record reflects the full plan that the break
    collapses. ``delta`` is set to ``nodes`` (the whole plan is truncated).

    Enabled only when ProfilePlan armed this run (``cfg['profile_plan']``
    or an active start_* from the widget). Never raises. Never ``explain``
    when the widget is off.
    """
    if not profiling_enabled(cfg):
        return
    try:
        metrics = measure_plan(df, cfg)
        if not metrics:
            return
        record = {
            "func": name,
            "nodes": metrics["nodes"],
            "depth": metrics["depth"],
            "delta": metrics["nodes"],
            "ops": metrics["ops"],
        }
        target = _sink_list("checkpoint", cfg)
        if target is not None:
            with _LOCK:
                target.append(record)
        _remember("checkpoint", record)
        ops = " ".join(f"{k}={v}" for k, v in sorted(record["ops"].items()))
        logger.info(
            "[PLAN-CKPT] %s: nodes=%d depth=%d%s",
            name,
            record["nodes"],
            record["depth"],
            f"  {ops}" if ops else "",
        )
    except Exception:
        logger.debug("[PLAN] track_checkpoint_plan failed", exc_info=True)


def track_plan(fn):
    """Decorator: record a builder's contribution to logical-plan size.

    No-op unless a plan sink is active (see :func:`start_plan_profile`) or the
    threaded ``cfg`` has ``profile_plan`` truthy. Attributes
    ``delta = output_nodes - max(input_nodes)`` to ``fn.__name__``.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        cfg = _find_cfg(args, kwargs)
        if not profiling_enabled(cfg):
            return fn(*args, **kwargs)

        input_nodes = 0
        try:
            for df_in in _find_dataframes((*args, *kwargs.values())):
                metrics = measure_plan(df_in, cfg)
                if metrics:
                    input_nodes = max(input_nodes, metrics["nodes"])
        except Exception as exc:
            _arm_explain_skip(f"{type(exc).__name__}: {exc}")

        result = fn(*args, **kwargs)

        out_df = _first_dataframe(result)
        if out_df is not None:
            try:
                metrics = measure_plan(out_df, cfg)
            except Exception as exc:
                _arm_explain_skip(f"{type(exc).__name__}: {exc}")
                return result
            if not metrics:
                return result
            record = {
                "func": fn.__name__,
                "nodes": metrics["nodes"],
                "depth": metrics["depth"],
                "delta": metrics["nodes"] - input_nodes,
                "ops": metrics["ops"],
            }
            target = _sink_list("plan", cfg)
            if target is not None:
                with _LOCK:
                    target.append(record)
            _remember("builder", record)
            ops = " ".join(
                f"{k}={v}" for k, v in sorted(record["ops"].items())
            )
            logger.info(
                "[PLAN] %s: nodes=%d depth=%d (+%d)%s",
                fn.__name__,
                record["nodes"],
                record["depth"],
                record["delta"],
                f"  {ops}" if ops else "",
            )
        return result

    return wrapper


# --------------------------------------------------------------------------- #
# Recommendation (printed; does not insert or drop checkpoints)
# --------------------------------------------------------------------------- #
def _report_kind(label: str) -> str:
    normalized = str(label or "").upper()
    if "ACTION" in normalized:
        return "action"
    if "CHECKPOINT" in normalized:
        return "checkpoint"
    return "builder"


def classify_plan_recommendation(
    record: dict, threshold: int, kind: str = "builder"
) -> str:
    """Label one profile row: add / keep / measure / collapse / remove.

    BUILDER (where the plan grows):
      * add      — ``delta >= threshold``; consider a new lineage break
      * measure  — plan already large/deep but this step added little
      * collapse — small growth; do not add a checkpoint here

    CHECKPOINT (what an existing break truncates; ``delta`` is incoming nodes):
      * keep     — truncates a large plan
      * measure  — mid-size; need wall time / fan-out
      * remove   — low-value seam; consider dropping the break

    ACTION (where Spark materializes a plan):
      * add      — oversized plan or repeated action name
      * measure  — action exists, but plan/reuse evidence is insufficient
    """
    nodes = int(record.get("nodes", 0) or 0)
    delta = int(record.get("delta", 0) or 0)
    depth = int(record.get("depth", 0) or 0)
    limit = max(1, int(threshold))
    if kind == "action":
        if nodes >= limit or int(record.get("action_count", 1) or 1) > 1:
            return "add"
        return "measure"
    if kind == "checkpoint":
        size = max(nodes, delta)
        if size >= limit:
            return "keep"
        if size >= max(8, limit // 2):
            return "measure"
        return "remove"
    if delta >= limit:
        return "add"
    if nodes >= limit or depth >= 20:
        return "measure"
    return "collapse"


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def plan_profile_report(
    source, threshold: int | None = None, label: str = ""
) -> list:
    """Print + LOG and return the plan-growth ranking.

    ``source`` may be the records ``list`` returned by :func:`start_plan_profile`
    or a ``cfg`` dict carrying ``_plan_profile``. Empty when nothing recorded.
    ``label`` (e.g. "BUILDER" / "CHECKPOINT") is included in the header so the
    two reports are distinguishable in the driver run log. Each row is emitted
    via both ``print`` (notebook cell) and ``logger.info`` (driver run log), so
    node/depth/ops land in the run log too — not just the notebook display.
    """
    if isinstance(source, dict):
        if not str(label or "").strip():
            if threshold is None:
                threshold = source.get(
                    "plan_checkpoint_threshold", _DEFAULT_CHECKPOINT_THRESHOLD
                )
            builders = plan_profile_report(
                source.get("_plan_profile"), threshold, label="BUILDER"
            )
            checkpoints = plan_profile_report(
                source.get("_checkpoint_plan_profile"),
                threshold,
                label="CHECKPOINT",
            )
            actions = plan_profile_report(
                source.get("_action_plan_profile"),
                threshold,
                label="ACTION",
            )
            source["_checkpoint_plan_profile_report"] = checkpoints
            source["_action_plan_profile_report"] = actions
            return builders
        records = list(
            source.get(
                {
                    "checkpoint": "_checkpoint_plan_profile",
                    "action": "_action_plan_profile",
                }.get(_report_kind(label), "_plan_profile"),
                [],
            )
        )
        if threshold is None:
            threshold = source.get(
                "plan_checkpoint_threshold", _DEFAULT_CHECKPOINT_THRESHOLD
            )
    else:
        records = list(source or [])
    if threshold is None:
        threshold = _DEFAULT_CHECKPOINT_THRESHOLD
    threshold = int(threshold)

    tag = f" {label}" if label else ""
    if not records:
        empty = f"[PLAN REPORT{tag}] no records captured"
        print(empty)
        logger.info(empty)
        return []

    kind = _report_kind(label)
    ranked = sorted(records, key=lambda r: r["delta"], reverse=True)
    action_counts = Counter(str(r.get("func")) for r in records)
    if kind == "action":
        header = (
            f"[PLAN REPORT{tag}] Spark actions ranked by input plan size; "
            "add / measure (candidate only)"
        )
        legend = (
            f"[PLAN REPORT{tag}] add=nodes>={threshold} or repeated action name; "
            "measure=insufficient checkpoint evidence"
        )
    elif kind == "checkpoint":
        header = (
            f"[PLAN REPORT{tag}] ranked by truncated plan size "
            "(delta=incoming nodes); keep / measure / remove"
        )
        legend = (
            f"[PLAN REPORT{tag}] keep=nodes>={threshold}  "
            f"measure=mid  remove=low-value existing break"
        )
    else:
        header = (
            f"[PLAN REPORT{tag}] ranked by plan-node growth "
            "(delta vs largest input); add / measure / collapse"
        )
        legend = (
            f"[PLAN REPORT{tag}] add=delta>={threshold}  "
            f"measure=fat/low-growth  collapse=do not add"
        )
    print(header)
    print(legend)
    logger.info(header)
    logger.info(legend)
    annotated = []
    for r in ranked:
        rec = dict(r)
        if kind == "action":
            rec["action_count"] = action_counts[str(rec.get("func"))]
        rec["recommendation"] = classify_plan_recommendation(rec, threshold, kind)
        ops = " ".join(
            f"{k}={v}" for k, v in sorted((rec.get("ops") or {}).items())
        )
        action_detail = ""
        if kind == "action":
            elapsed = rec.get("elapsed_seconds")
            elapsed_text = (
                f"{float(elapsed):.3f}s" if elapsed is not None else "unknown"
            )
            action_detail = (
                f"  action={elapsed_text} calls={rec['action_count']}"
            )
        flag = f"  <-- {rec['recommendation']}"
        line = (
            f"  {rec['func']:<34} nodes={rec['nodes']:>4} depth={rec['depth']:>3} "
            f"(+{rec['delta']}){action_detail}"
            f"{('  ' + ops) if ops else ''}{flag}"
        )
        print(line)
        logger.info(line)
        annotated.append(rec)
    return annotated
