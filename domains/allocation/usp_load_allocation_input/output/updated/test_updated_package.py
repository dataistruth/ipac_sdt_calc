"""
Smoke test: syntax, monolith-style import path, and stubbed pipeline run.

Run from anywhere:
  python3 test_updated_package.py
"""

from __future__ import annotations

import ast
import importlib
import os
import sys
import tempfile
import types
from pathlib import Path

UPDATED_DIR = Path(__file__).resolve().parent
SKIP_FILES = {"_ai_pfic_flowup_service_base.py", "_build_ai_pfic_flowup.py"}


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"OK: {msg}")


def test_syntax() -> None:
    py_files = sorted(
        p for p in UPDATED_DIR.glob("*.py")
        if p.name not in SKIP_FILES and p.name != Path(__file__).name
    )
    for path in py_files:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    _ok(f"syntax parse ({len(py_files)} modules in updated/)")


def _install_common_v2_stubs() -> None:
    common = types.ModuleType("Common_V2")
    core = types.ModuleType("Common_V2.core")
    helpers = types.ModuleType("Common_V2.core.helpers")
    config = types.ModuleType("Common_V2.core.config")
    storer = types.ModuleType("Common_V2.core.generic_result_storer")

    def table_prefix(cfg: dict) -> str:
        return f"{cfg.get('catalog', 'cat')}.{cfg.get('schema', 'sch')}"

    def read_table(spark, name: str, cfg: dict):
        return spark.createDataFrame([], "id INT")

    def log_section(title: str) -> None:
        print(f"[section] {title}")

    def log_timing(label: str, t0: float) -> None:
        print(f"[timing] {label}")

    helpers.table_prefix = table_prefix
    helpers.read_table = read_table
    helpers.log_section = log_section
    helpers.log_timing = log_timing

    def load_common_config(spark, **kwargs):
        return {
            "entity_id": kwargs.get("entity_id"),
            "client_id": kwargs.get("client_id"),
            "tax_period_id": kwargs.get("tax_period_id"),
            "run_id": kwargs.get("run_id"),
            "catalog": kwargs.get("catalog"),
            "schema": kwargs.get("schema"),
            "run_status": "OK",
            "investment_tag_workflow_id": 0,
            "fx_rate_transaction_id": 0,
        }

    config.load_common_config = load_common_config

    class GenericResultStorer:
        def __init__(self, *args, **kwargs):
            pass

        def store(self, *args, **kwargs):
            return None

    storer.GenericResultStorer = GenericResultStorer

    sys.modules["Common_V2"] = common
    sys.modules["Common_V2.core"] = core
    sys.modules["Common_V2.core.helpers"] = helpers
    sys.modules["Common_V2.core.config"] = config
    sys.modules["Common_V2.core.generic_result_storer"] = storer
    common.core = core
    core.helpers = helpers
    core.config = config
    core.generic_result_storer = storer


def _write_ai_stubs(output_dir: Path) -> None:
    empty_df_src = """
def _empty(spark):
    return spark.createDataFrame([], "RunID INT, EntityID INT")
"""

    stubs = {
        "ai_config_service": """
def load_config(spark, cfg):
    return cfg
""",
        "ai_validation_service": """
def run_validations(spark, cfg, lower_tier_df):
    return True
""",
        "ai_hierarchy_service": """
def _empty(spark):
    return spark.createDataFrame([], "RunID INT, EntityID INT")

def build_entity_hierarchy(spark, cfg):
    return _empty(spark)

def build_lower_tier_funds(spark, cfg):
    return _empty(spark)

def build_workflows(spark, cfg):
    return {}
""",
        "ai_k1_service": """
def build_k1_and_related_inputs(spark, cfg, workflows):
    return spark.createDataFrame([], "RunID INT, EntityID INT")
""",
        "ai_form_service": """
def build_all_form_inputs(spark, cfg):
    return spark.createDataFrame([], "RunID INT, EntityID INT")
""",
        "ai_pfic_service": """
def _empty(spark):
    return spark.createDataFrame([], "RunID INT, EntityID INT")

def build_pfic_snapshot(spark, cfg):
    return _empty(spark)

def build_pfic_elections(spark, cfg, pfic_snapshot_df):
    return _empty(spark)

def build_pfic_allocation_input(spark, cfg, pfic_snapshot_df, pfic_elections):
    return _empty(spark)

def apply_pfic_election_deletes(spark, cfg, allocation_input_df, pfic_flowup_df, pfic_elections, lower_tier_df):
    return allocation_input_df, pfic_flowup_df

def apply_part_v_vii_flags(spark, cfg, pfic_flowup_df):
    return pfic_flowup_df
""",
        "ai_finalization_service": """
def _empty(spark):
    return spark.createDataFrame([], "RunID INT, EntityID INT")

def apply_tag_percentages(spark, cfg, df):
    return df

def write_allocation_input(spark, cfg, df):
    return None

def write_pfic_flowup(spark, cfg, df):
    return None

def apply_master_feed_override(spark, cfg, df):
    return df

def apply_blocker_entity_cleanup(spark, cfg, df):
    return df

def apply_distribution_line_suppression(spark, cfg, df):
    return df

def write_form_flowups(spark, cfg):
    return None

def purge_output_tables(spark, cfg):
    return None
""",
        "ai_shared_views": """
def register_shared_views(spark, cfg):
    return None
""",
    }

    for name, body in stubs.items():
        (output_dir / f"{name}.py").write_text(body.strip() + "\n", encoding="utf-8")


def _clear_allocation_modules() -> None:
    prefix = "AllocationV2.usp_load_allocation_input"
    for name in list(sys.modules):
        if name == prefix or name.startswith(prefix + "."):
            del sys.modules[name]


def test_imports(source_dir: Path) -> dict[str, types.ModuleType]:
    _clear_allocation_modules()
    modules = {}
    stems = [
        "updated",
        "updated.load_allocation_input",
        "updated.load_allocation_input_updated",
        "updated.ai_pfic_flowup_service",
        "updated.checkpoint",
        "updated.parent",
    ]
    base = "AllocationV2.usp_load_allocation_input.output"
    for stem in stems:
        full = f"{base}.{stem}"
        mod = importlib.import_module(full)
        modules[stem] = mod
        _ok(f"import {full}")
    return modules


def test_stubbed_run(modules: dict[str, types.ModuleType]) -> None:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_updated_package")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    runner = modules["updated.load_allocation_input"]
    pfic_mod = modules["updated.ai_pfic_flowup_service"]

    def stub_flowup(spark, cfg, pfic_snapshot_df, pfic_elections, lower_tier_df):
        pfic_mod._log("stub build_pfic_flowup_pipeline (smoke test)")
        return spark.createDataFrame([], "RunID INT, EntityID INT")

    runner.build_pfic_flowup_pipeline = stub_flowup
    runner.build_custom_footnote_input = lambda spark, cfg: spark.createDataFrame(
        [], "RunID INT, EntityID INT"
    )
    runner.check_pfic_xml_override_alert = lambda *a, **k: None
    runner.apply_pfic_election_deletes = lambda spark, cfg, a, p, e, l: (a, p)
    runner.apply_part_v_vii_flags = lambda spark, cfg, df: df

    result = runner.run_load_allocation_input(
        spark,
        EntityID=115,
        ClientID=15348,
        TaxPeriodID=1,
        RunID=16560,
        CatalogName="QA7",
        SchemaName="IPC_2025_QA7_15348",
        VolumePath="/tmp/alloc_checkpoint_test",
        CheckpointLevel="default",
        parallel_write_workers=1,
        parallel_config_workers=1,
    )

    if not isinstance(result, dict):
        _fail(f"run returned non-dict: {type(result)}")
    if result.get("status") not in ("OK", "SUCCESS"):
        _fail(f"run status={result.get('status')} reason={result.get('reason')}")
    if result.get("implementation") != "updated.load_allocation_input":
        _fail(f"unexpected implementation: {result.get('implementation')}")
    _ok(f"stubbed run status={result.get('status')} elapsed={result.get('elapsed_seconds')}")

    spark.stop()


def main() -> None:
    print(f"updated dir: {UPDATED_DIR}")
    test_syntax()

    _install_common_v2_stubs()

    with tempfile.TemporaryDirectory(prefix="alloc_import_test_") as tmp:
        source = Path(tmp) / "Source"
        output = source / "AllocationV2" / "usp_load_allocation_input" / "output"
        output.mkdir(parents=True)
        os.symlink(UPDATED_DIR, output / "updated", target_is_directory=True)
        _write_ai_stubs(output)

        if str(source) not in sys.path:
            sys.path.insert(0, str(source))

        modules = test_imports(source)
        test_stubbed_run(modules)

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
