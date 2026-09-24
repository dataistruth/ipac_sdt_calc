"""Local structure tests; these never import PySpark-dependent modules."""

from __future__ import annotations

import ast
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def source(name):
    return (ROOT / name).read_text(encoding="utf-8")


def tree(name):
    return ast.parse(source(name), filename=name)


def function_names(name):
    return {
        node.name
        for node in ast.walk(tree(name))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


class OutputV3StructureTests(unittest.TestCase):
    def test_public_api_is_preserved(self):
        names = function_names("orchestrator.py")
        self.assertTrue(
            {
                "run_final_effective_percentages",
                "run_mode",
                "run_modes",
                "get_last_run_profile",
            }.issubset(names | {"run_mode"})
        )
        init_text = source("__init__.py")
        for api in (
            "run_final_effective_percentages",
            "run_mode",
            "run_modes",
            "get_last_run_profile",
        ):
            self.assertIn(f'"{api}"', init_text)

    def test_no_disallowed_package_dependency(self):
        for path in ROOT.glob("*.py"):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn(".outputV2", text, path.name)
            self.assertNotIn(".output.updated", text, path.name)
            self.assertNotIn("output/updated", text, path.name)

    def test_named_stage_contracts_are_explicit(self):
        text = source("stages.py")
        for name in (
            "COMMON_READS",
            "WITH_LT_BRANCH",
            "NO_LT_BRANCH",
            "MODE_PREP",
            "FUSED_CPBT",
            "FUSED_EFFECTIVE",
            "OUTPUT_BUILD",
            "OUTPUT_WRITE",
        ):
            self.assertIn(name, text)

    def test_checkpoint_policy_uses_existing_v2_modes(self):
        text = source("checkpoint_policy.py")
        self.assertIn("def decide_checkpoint(name:", text)
        self.assertIn("alias-sensitive transfer seam", text)
        self.assertIn("alias-sensitive or reused effective seam", text)
        self.assertIn("expensive reused fused CPBT intermediate", text)
        self.assertIn("all_ent_pre_tag", text)
        self.assertIn("tcp_post_tag", text)
        self.assertIn("safe cheap lineage break", text)
        self.assertIn("checkpoint_mode=checkpoint_mode", text)
        self.assertIn('cfg["_output_v3_checkpoint_mode"]', text)
        self.assertIn('"checkpoint_mode": checkpoint_mode', text)
        self.assertIn("drop_failed_run_checkpoints", text)
        self.assertIn("drop_checkpoints_V2", text)
        self.assertIn("[outputV3 checkpoint] START", text)
        self.assertIn("[outputV3 checkpoint] DONE", text)
        self.assertIn("[outputV3 checkpoint] FAIL", text)

    def test_parallelism_is_bounded_and_staged(self):
        text = source("orchestrator.py")
        self.assertIn("min(int(value), 4)", text)
        self.assertIn('"pass_a_strategy": (', text)
        self.assertIn('"branch_strategy": (', text)
        self.assertIn('"parallel_isolated_cfg"', text)
        self.assertIn('"ParallelGroups"', text)
        self.assertIn("_ALL_PARALLEL_GROUPS", text)
        pipeline = source("pipeline.py")
        for group in ("lt_nolt_branches", "mode_prep", "output_build"):
            self.assertIn(f'"{group}"', pipeline)
        self.assertIn('"output_writes"', text)

    def test_cfg_forks_share_only_checkpoint_coordination(self):
        text = source("cfg_isolation.py")
        for key in (
            "_checkpoint_v2_state",
            "_checkpoint_v2_activity",
            "_checkpoint_policy_activity",
            "_checkpoint_tables",
            "_checkpoint_paths",
        ):
            self.assertIn(f'"{key}"', text)
        for key in (
            "mode",
            "_current_mode",
            "_inputs_empty",
            "_part_v_quarters_df",
        ):
            self.assertIn(f'"{key}"', text)
        self.assertIn("Conflicting _part_v_quarters_df schemas", text)

    def test_cfg_fork_runtime_identity_contract(self):
        spec = importlib.util.spec_from_file_location(
            "output_v3_cfg_isolation", ROOT / "cfg_isolation.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        state = {"lock": object()}
        activity = []
        source_cfg = {
            "mode": 3,
            "_current_mode": 3,
            "_inputs_empty": True,
            "_part_v_quarters_df": object(),
            "_checkpoint_v2_state": state,
            "_checkpoint_v2_activity": activity,
            "_checkpoint_policy_activity": [],
            "_checkpoint_tables": [],
            "_checkpoint_paths": [],
            "ordinary_list": [1],
        }
        module.ensure_thread_safe_checkpoint_collections(source_cfg)
        fork = module.fork_cfg(source_cfg, mode=2)
        for key in module.SHARED_CHECKPOINT_KEYS:
            self.assertIs(fork[key], source_cfg[key])
        for key in (
            "_checkpoint_v2_activity",
            "_checkpoint_policy_activity",
            "_checkpoint_tables",
            "_checkpoint_paths",
        ):
            self.assertIsInstance(source_cfg[key], module.ThreadSafeList)
        self.assertIsNot(fork["ordinary_list"], source_cfg["ordinary_list"])
        self.assertEqual(fork["mode"], 2)
        self.assertEqual(fork["_current_mode"], 2)
        self.assertNotIn("_inputs_empty", fork)
        self.assertNotIn("_part_v_quarters_df", fork)

    def test_pipeline_copies_control_flow_not_business_helpers(self):
        names = function_names("pipeline.py")
        self.assertIn("run_modes_parallel", names)
        self.assertFalse(
            {
                "build_cost_percentage_by_type",
                "compute_effective_percentage_dated",
                "build_final_output",
                "build_footnote_dated_entities",
            }.intersection(names)
        )
        text = source("pipeline.py")
        for key in (
            '"statuses"',
            '"elapsed_seconds"',
            '"_checkpoint_tables"',
            '"_save_return_value"',
        ):
            self.assertIn(key, text)
        self.assertIn('status["result"] = None', text)
        self.assertIn('if 4 in modes:', text)

    def test_fresh_relation_behavior_is_retained(self):
        self.assertIn(
            "result.toDF(*result.columns)", source("checkpoint_policy.py")
        )

    def test_benchmark_runs_one_side_by_side_with_result_tables(self):
        text = source("notebook/benchmark_final_effective_percentage.py")
        self.assertIn('"wall_seconds": 181.893', text)
        self.assertIn('"reported_seconds": 172.0', text)
        self.assertIn('"rows": 79', text)
        self.assertIn('"ParallelGroups"', text)
        self.assertIn('"CheckpointMode"', text)
        self.assertIn('["1", "2", "4", "5"]', text)
        self.assertNotIn("[FEP_BENCHMARK]", text)
        self.assertNotIn("def _banner(", text)
        self.assertIn('"local/deferred"', text)
        self.assertIn("RUNTIME_SCHEMA", text)
        self.assertIn("COMPARE_SCHEMA", text)
        self.assertIn("CHECKPOINT_TIMING_SCHEMA", text)
        self.assertIn("spark.createDataFrame(runtime_rows", text)
        self.assertIn("display(spark.createDataFrame(compare_rows", text)
        # Multi-pass benchmark controlled by a single Passes widget.
        self.assertIn('"Passes"', text)
        self.assertIn("range(1, passes + 1)", text)
        self.assertIn("pass INT", text)
        # Experiment-only widgets were removed from the simple benchmark.
        self.assertNotIn('"ProfilePlan"', text)
        self.assertNotIn('"WarningProbeRemoval"', text)
        self.assertNotIn('"CpbtInputBreak"', text)
        self.assertNotIn('"TargetCheckpoint"', text)
        self.assertNotIn('"BusinessOptimization"', text)
        # No stdout logging helpers remain in the run cell.
        self.assertNotIn("def _log(", text)
        self.assertNotIn("def _clock(", text)

    def test_runtime_logging_is_present(self):
        text = source("orchestrator.py")
        self.assertIn("def _print_process(", text)
        self.assertIn("[outputV3 process]", text)
        self.assertIn('"START", "helper"', text)
        self.assertIn('"START", "task"', text)
        self.assertIn('"START", "run"', text)
        self.assertIn('"helper",\n                operation', text)
        self.assertIn('"task",\n                f"{group}.{name}"', text)
        self.assertIn('"run",\n            run_name', text)
        self.assertIn("[outputV3 timing] wall=", text)
        self.assertIn("[outputV3 timing] stage=", text)
        self.assertIn("start_plan_profile()", text)
        self.assertIn("start_checkpoint_plan_profile()", text)
        self.assertIn("start_action_profile()", text)
        self.assertIn('"plan_profile": reports["builder"]', text)
        self.assertIn(
            '"checkpoint_plan_profile": reports["checkpoint"]', text
        )
        self.assertIn('"action_profile": reports["action"]', text)
        self.assertIn('"performance_summary": performance', text)
        self.assertIn("[outputV3 budget]", text)

    def test_deep_baseline_experiments_are_isolated_and_reported(self):
        orchestrator = source("orchestrator.py")
        pipeline = source("pipeline.py")
        checkpoint = source("checkpoint_policy.py")
        notebook = source(
            "notebook/benchmark_final_effective_percentage.py"
        )
        self.assertIn('"SqlShufflePartitions"', orchestrator)
        self.assertIn('"_output_v3_effective_spark_config"', pipeline)
        self.assertNotIn('"spark.sql.shuffle.partitions": "32"', pipeline)
        self.assertIn('"WarningProbeRemoval"', orchestrator)
        self.assertIn('"_output_v3_missing_entity_identity"', pipeline)
        self.assertIn('"CpbtInputBreak"', orchestrator)
        self.assertIn('"cpbt_input_non_dated_fused"', pipeline)
        self.assertIn('"cpbt_input_dated_fused"', pipeline)
        self.assertIn('"_output_v3_target_checkpoint"', checkpoint)
        self.assertIn('"started_at": started_at', checkpoint)
        self.assertIn('"ended_at": ended_at', checkpoint)
        self.assertIn("CHECKPOINT_TIMING_SCHEMA", notebook)
        self.assertIn('"materialization"', notebook)
        self.assertIn('"registration only"', notebook)
        self.assertIn("shuffle_verified", notebook)
        self.assertIn('"parallel_wave_critical_path"', orchestrator)
        self.assertIn('"critical_actions"', orchestrator)
        self.assertNotIn("_emit(", notebook)

    def test_mode_five_and_shared_state_boundary_are_enabled(self):
        orchestrator = source("orchestrator.py")
        pipeline = source("pipeline.py")
        checkpoint = source("checkpoint_policy.py")
        checkpoint_v2 = (
            ROOT.parents[2] / "Common_V2/core/checkpoint_V2.py"
        ).read_text(encoding="utf-8")
        self.assertIn("checkpoint_mode\", 5", orchestrator)
        self.assertIn("sql_shuffle_partitions\", 8", orchestrator)
        self.assertIn("missing_entity_identity\", True", orchestrator)
        self.assertNotIn("checkpoint_mode != 5", checkpoint)
        self.assertIn("checkpoint_mode=checkpoint_mode", checkpoint)
        self.assertIn('f"state_lines_m{mode}"', pipeline)
        self.assertIn("frozenset({1, 2, 3, 4, 5})", checkpoint_v2)
        self.assertIn("local_checkpoint_eager = mode != 5", checkpoint_v2)
        self.assertIn(
            "df.localCheckpoint(eager=local_checkpoint_eager)",
            checkpoint_v2,
        )

    def test_warning_probe_experiments_are_output_v3_local(self):
        text = source("orchestrator.py")
        self.assertIn("_line_items_without_warning_probe", text)
        self.assertIn("_quarters_without_warning_probe", text)
        self.assertIn("_lookthrough_without_warning_probe", text)
        self.assertNotIn(".output.updated", text)

    def test_python_files_parse_without_importing_pyspark(self):
        for path in ROOT.rglob("*.py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


if __name__ == "__main__":
    unittest.main()
