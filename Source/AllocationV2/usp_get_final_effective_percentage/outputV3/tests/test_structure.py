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

    def test_checkpoint_policy_is_name_based(self):
        text = source("checkpoint_policy.py")
        self.assertIn("def decide_checkpoint(name:", text)
        self.assertIn("alias-sensitive transfer seam", text)
        self.assertIn("alias-sensitive or reused effective seam", text)
        self.assertIn("expensive reused fused CPBT intermediate", text)
        self.assertIn("all_ent_pre_tag", text)
        self.assertIn("tcp_post_tag", text)
        self.assertIn("safe cheap lineage break", text)
        self.assertIn("checkpoint_mode=primitive_mode", text)
        self.assertNotIn("sequence %", text)
        self.assertIn("drop_failed_run_checkpoints", text)
        self.assertIn("drop_checkpoints_V2", text)

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

    def test_benchmark_defaults_to_two_alternating_passes(self):
        text = source("notebook/benchmark_final_effective_percentage.py")
        self.assertIn('"number_of_runs", "2"', text)
        self.assertIn('"alternate"', text)
        self.assertIn('"wall_seconds": 181.893', text)
        self.assertIn('"reported_seconds": 172.0', text)
        self.assertIn('"rows": 79', text)
        self.assertIn('"pipeline_strategy"', text)
        self.assertIn('"artifact_merges"', text)
        self.assertIn('"ParallelGroups"', text)

    def test_python_files_parse_without_importing_pyspark(self):
        for path in ROOT.rglob("*.py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


if __name__ == "__main__":
    unittest.main()
