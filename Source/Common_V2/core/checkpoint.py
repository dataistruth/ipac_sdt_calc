"""Delta checkpointing — breaks DAG lineage for deep pipelines.

Budget: 0 for Easy/Medium SPs, 0-2 for Hard.
Only checkpoint when lineage has 3+ raw joins AND the DataFrame
has multiple downstream consumers.

Two modes:
    DELTA_CHECKPOINT_MODE (1, default) - temp Delta table, read back.
    LOCAL_CHECKPOINT_MODE (2)          - localCheckpoint(eager=True).
The mode comes from ``checkpoint_mode``, else ``cfg["checkpoint_mode"]``, else
DEFAULT_CHECKPOINT_MODE (Delta), so SPs that set neither keep the Delta behavior.
"""

from pyspark.sql import SparkSession, DataFrame
from Common_V2.core.helpers import table_prefix
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import os
import uuid

logger = logging.getLogger(__name__)

_CHECKPOINT_MAX_RETRIES = 3
_CHECKPOINT_RETRY_DELAY = 5  # seconds

DELTA_CHECKPOINT_MODE = 1
LOCAL_CHECKPOINT_MODE = 2
DEFAULT_CHECKPOINT_MODE = DELTA_CHECKPOINT_MODE


def checkpoint(spark: SparkSession, df: DataFrame, name: str, cfg: dict, checkpoint_mode=None) -> DataFrame:
    """Write DataFrame to a temp Delta table and read back.

    Breaks Spark DAG lineage for deep pipelines. Thread-safe via a unique
    per-call suffix so parallel SPs never share a temp table name.
    Table name stored in cfg['_checkpoint_tables'] for cleanup.
    Retries up to 3 times on transient Unity Catalog staging errors.

    In LOCAL_CHECKPOINT_MODE the frame is materialized with
    ``localCheckpoint(eager=True)`` instead (falling back to Delta if that is
    unavailable) and column qualifiers are reset with ``toDF``.
    """
    mode = checkpoint_mode if checkpoint_mode is not None else cfg.get("checkpoint_mode", DEFAULT_CHECKPOINT_MODE)
    if str(mode).strip() == str(LOCAL_CHECKPOINT_MODE):
        try:
            _t0 = time.time()
            result = df.localCheckpoint(eager=True)
            logger.info(f"[CHECKPOINT] {time.time() - _t0:7.2f}s  {name}  (local)")
            return result.toDF(*result.columns)
        except Exception as e:
            logger.warning(f"[CHECKPOINT] localCheckpoint failed for {name}, using Delta: {e}")

    run_id = cfg.get("run_id", "0")
    # Unique suffix per call so SPs sharing the same run_id (parallel execution)
    # never collide on the same temp table name.
    uniq = uuid.uuid4().hex[:8]
    fqn = f"{table_prefix(cfg)}._tmp_{name}_{run_id}_{uniq}"
    cfg.setdefault("_checkpoint_tables", []).append(fqn)

    last_err = None
    for attempt in range(1, _CHECKPOINT_MAX_RETRIES + 1):
        try:
            # No pre-DROP on the happy path: fqn carries a per-call uuid suffix, so the table
            # cannot pre-exist. mode("overwrite") covers the retry case, and the retry branch
            # below force-drops before re-attempting. Skipping it removes one Unity Catalog
            # metadata round-trip per checkpoint.
            _t0 = time.time()
            df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("delta.dataSkippingNumIndexedCols", "0") \
                .saveAsTable(fqn)
            logger.info(f"[CHECKPOINT] {time.time() - _t0:7.2f}s  {name}  ({fqn})")
            return spark.table(fqn)
        except Exception as e:
            last_err = e
            err_str = str(e)
            # Retry on transient Unity Catalog staging table errors and metadata
            # latency (TABLE_OR_VIEW_ALREADY_EXISTS can occur when a prior DROP has
            # not yet propagated through UC metadata before saveAsTable analyses
            # the plan).
            if (
                "RESOURCE_DOES_NOT_EXIST" in err_str
                or "Staging Table" in err_str
                or "TABLE_OR_VIEW_ALREADY_EXISTS" in err_str
                or "TableAlreadyExistsException" in err_str
            ):
                logger.warning(
                    f"[CHECKPOINT] Transient UC error on attempt {attempt}/{_CHECKPOINT_MAX_RETRIES} "
                    f"for {fqn}: {err_str[:200]}"
                )
                if attempt < _CHECKPOINT_MAX_RETRIES:
                    # Force-drop again and wait for UC metadata to propagate
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                    except Exception:
                        pass
                    time.sleep(_CHECKPOINT_RETRY_DELAY * attempt)
                    continue
            # Non-transient error — raise immediately
            raise

    # All retries exhausted
    raise last_err


def drop_checkpoints(spark: SparkSession, cfg: dict) -> None:
    """Drop all checkpoint tables. Call in finally block — ALWAYS.

    Each DROP is an independent Unity Catalog metadata round-trip, so they are issued
    concurrently. A deep pipeline registers dozens of scratch tables and this runs inside
    the job's measured wall time.
    """
    tables = cfg.get("_checkpoint_tables", [])
    if not tables:
        cfg["_checkpoint_tables"] = []
        return

    def _drop(fqn):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            logger.debug(f"[CHECKPOINT] Dropped: {fqn}")
        except Exception:
            logger.warning(f"[CHECKPOINT] Failed to drop: {fqn}")

    # Each DROP is an independent metadata round-trip of ~1-2s, and a deep pipeline now
    # registers dozens of scratch tables, so the width here is what bounds the teardown
    # that runs inside the job's measured wall time.
    with ThreadPoolExecutor(max_workers=min(32, len(tables))) as pool:
        list(pool.map(_drop, tables))
    cfg["_checkpoint_tables"] = []
