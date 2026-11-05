import importlib.util, sys, types
sys.path.insert(0, "Source")
for name, path in (("AllocationV2", "Source/AllocationV2"), ("AllocationV2.plan_profiler", "Source/AllocationV2/plan_profiler")):
    m = types.ModuleType(name); m.__path__ = [path]; sys.modules[name] = m
def load(name, path):
    s = importlib.util.spec_from_file_location(name, path); m = importlib.util.module_from_spec(s); sys.modules[name] = m; s.loader.exec_module(m); return m
P = load("AllocationV2.plan_profiler.profiler", "Source/AllocationV2/plan_profiler/profiler.py")
D = load("AllocationV2.plan_profiler.display", "Source/AllocationV2/plan_profiler/display.py")

class DF:
    columns = ["a"]; schema = None
    def __init__(s, plan): s.plan = plan
    def explain(s): pass
    def _explain_string(s, mode="extended"): return "== Optimized Logical Plan ==\n" + s.plan + "\n== Physical Plan ==\nx"

A_leaf = "LogicalRDD [a#1, b#2L], false"
B_leaf = "LogicalRDD [c#5, d#6], false"
D_leaf = "Relation spark_catalog.qa7.s._tmp_fep_cost_pct_m123_17376_ab12cd34[x#9] parquet"
cfg = {"profile_plan": True}
P.start_plan_profile(); _, ck = P.start_checkpoint_plan_profile()
def cp(name, incoming, leaf):
    P.track_checkpoint_plan(name, DF(incoming), cfg)
    P.track_checkpoint_result(name, DF(leaf), cfg)
big = "\n".join(["Project [a]"] + [f"+- Filter x{i}" for i in range(40)])
cp("A_small_read_twice", "Aggregate [a]\n+- Join Inner\n   :- Scan t\n   +- Scan u", A_leaf)
cp("B_small_read_once", "Project [c]\n+- Filter c\n   +- Scan v", B_leaf)
cp("D_delta_read_twice", "Project [x]\n+- Scan w", D_leaf)
cp("E_consumer", f"Join Inner\n:- {A_leaf}\n+- Join Inner\n   :- LogicalRDD [a#20, b#21L], false\n   +- {B_leaf}", "LogicalRDD [e#30], false")
cp("F_consumer", f"Union\n:- {D_leaf}\n+- Project [x#9]\n   +- {D_leaf}", "LogicalRDD [f#31], false")
cp("G_big_never_read", big, "LogicalRDD [g#32], false")
P.track_checkpoint_plan("H_no_result_tracked", DF("Project [h]\n+- Scan z"), cfg)
B2_leaf = "LogicalRDD [m#40], false"
cp("B2_read_once", "Project [m]\n+- Filter m\n   +- Scan q", B2_leaf)
cp("I_recheckpoint_of_B2", B2_leaf, "LogicalRDD [m#41], false")
cp("J_empty_local", "LocalRelation <empty>, [k#50]", "LogicalRDD [k#51], false")
cp("K_filtered_table", "Filter (x#60 > 0)\n+- Relation spark_catalog.qa7.s.src[x#60] parquet", "LogicalRDD [x#61], false")
rows = P.plan_profile_report(ck, 30, label="CHECKPOINT")
got = {r["func"]: (r.get("reads"), r["recommendation"]) for r in rows}
exp = {"A_small_read_twice": (2, "keep"), "B_small_read_once": (1, "remove"), "D_delta_read_twice": (2, "keep"),
       "E_consumer": (0, "measure"), "F_consumer": (0, "remove"), "G_big_never_read": (0, "keep"), "H_no_result_tracked": (None, "measure"),
       "B2_read_once": (1, "remove"), "I_recheckpoint_of_B2": (0, "remove"), "J_empty_local": (0, "remove"), "K_filtered_table": (0, "measure")}
bad = 0
for k, v in exp.items():
    ok = got[k] == v; bad += not ok
    print(("PASS " if ok else "FAIL ") + f"{k}: got {got[k]} expected {v}")
print("LAST_PLAN_PROFILE reads:", [r.get("reads") for r in P.LAST_PLAN_PROFILE["checkpoint"]])
print("display row:", D._profile_rows(ck, 30, "checkpoint")[-1])
P.disable_plan_profile(); print("leaves cleared:", not P._CHECKPOINT_LEAVES)
sys.exit(bad)
