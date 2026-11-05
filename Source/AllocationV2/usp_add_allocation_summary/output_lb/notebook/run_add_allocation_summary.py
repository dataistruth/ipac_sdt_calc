# Databricks notebook source
# MAGIC %md
# MAGIC # Run uspAddAllocationSummary (Lakebase / PostgreSQL)
# MAGIC Widgets 1–5 match the production runner. The `lakebase_*` widgets carry the
# MAGIC Postgres connection: role auth reads host/user/password from the secret scope
# MAGIC (or the explicit widgets), oauth auth uses `lakebase_instance` or `lakebase_endpoint`.

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]>=3.1"

# COMMAND ----------

# Uncomment to reset widgets to notebook defaults.
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/usa-mukessingh@deloitte.com/iPACSCore_SDT_Databricks/Source",
    "1. Source root",
)
dbutils.widgets.text("EntityID", "4137", "2. EntityID")
dbutils.widgets.text("ClientID", "15348", "3. ClientID")
dbutils.widgets.text("TaxPeriodID", "1", "4. TaxPeriodID")
dbutils.widgets.text("RunID", "17376", "5. RunID")
dbutils.widgets.text("lakebase_host", "", "lb01. Host (blank = secret / oauth lookup)")
dbutils.widgets.text("lakebase_port", "5432", "lb02. Port")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "lb03. Database")
dbutils.widgets.text("lakebase_schema", "", "lb04. Schema")
dbutils.widgets.dropdown("lakebase_auth", "role", ["role", "oauth"], "lb05. Auth")
dbutils.widgets.text("lakebase_secret_scope", "", "lb06. Secret scope")
dbutils.widgets.text("lakebase_host_key", "lakebase_host", "lb07. Host secret key")
dbutils.widgets.text("lakebase_user_key", "lakebase_user", "lb08. User secret key")
dbutils.widgets.text("lakebase_password_key", "lakebase_password", "lb09. Password secret key")
dbutils.widgets.text("lakebase_instance", "", "lb10. OAuth instance name")
dbutils.widgets.text("lakebase_endpoint", "", "lb11. OAuth endpoint name")
dbutils.widgets.dropdown("lakebase_deploy", "auto", ["auto", "always", "never"], "lb12. Deploy SQL")
dbutils.widgets.dropdown("lakebase_write_log", "true", ["true", "false"], "lb13. Write AllocationLog")

# COMMAND ----------

import importlib
import sys
import time

source_path = dbutils.widgets.get("source_path").strip().rstrip("/")
entity_id = int(dbutils.widgets.get("EntityID"))
client_id = int(dbutils.widgets.get("ClientID"))
tax_period_id = int(dbutils.widgets.get("TaxPeriodID"))
run_id = int(dbutils.widgets.get("RunID"))
LAKEBASE_KEYS = (
    "lakebase_host", "lakebase_port", "lakebase_database", "lakebase_schema", "lakebase_auth",
    "lakebase_secret_scope", "lakebase_host_key", "lakebase_user_key", "lakebase_password_key",
    "lakebase_instance", "lakebase_endpoint", "lakebase_deploy", "lakebase_write_log",
)
lakebase_params = {key: dbutils.widgets.get(key).strip() for key in LAKEBASE_KEYS}
PACKAGE = "AllocationV2.usp_add_allocation_summary"
MODULE = "AllocationV2.usp_add_allocation_summary.output_lb.add_allocation_summary"
ENTRY = "run_add_allocation_summary"
OUTPUT_TABLES = ('K1AllocationSummary', 'M1AdjAllocationSummary', 'BoxJKLAllocationSummary', 'Form926AllocationSummary', 'Form8865AllocationSummary', 'Form199AAllocationSummary', 'PFICFootnoteAllocationSummary', 'PFICFootnoteAllocationText', 'CustomFootnoteAllocationSummary', 'Line18AAllocationSummary', 'UBTIAllocationSummary', 'PassiveIncomeAllocationSummary', 'Form200616AllocationSummary', 'Form8886AllocationSummary', 'AtRiskAllocationSummary', 'GAAPToTaxAllocation', 'AdjustmentAllocationSummary')

while source_path in sys.path:
    sys.path.remove(source_path)
sys.path.insert(0, source_path)


def _evict():
    roots = (f"{PACKAGE}.output_lb", "Common_V2")
    for name in list(sys.modules):
        if any(name == root or name.startswith(root + ".") for root in roots):
            del sys.modules[name]
    importlib.invalidate_caches()


_evict()
lakebase = importlib.import_module("Common_V2.core.lakebase")
runner = importlib.import_module(MODULE)
print(f"[run] module={runner.__file__}", flush=True)


def _row_counts():
    settings = lakebase.settings_from_params(lakebase_params, spark=spark, dbutils=dbutils)
    schema = lakebase.quote_ident(settings.schema)
    counts = {}
    total = 0
    conn = lakebase.connect(settings, application_name="ipac_output_lb.row_counts")
    try:
        with conn.cursor() as cur:
            for table in OUTPUT_TABLES:
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (f'{schema}.{lakebase.quote_ident(table)}',),
                )
                if not cur.fetchone()[0]:
                    counts[table] = None
                    continue
                cur.execute(
                    f'SELECT count(*) FROM {schema}.{lakebase.quote_ident(table)} WHERE "RunID" = %s',
                    (run_id,),
                )
                counts[table] = cur.fetchone()[0]
                total += counts[table]
        conn.rollback()
    finally:
        conn.close()
    return counts, total


kwargs = dict(
    EntityID=entity_id,
    ClientID=client_id,
    TaxPeriodID=tax_period_id,
    RunID=run_id,
    **lakebase_params,
)

started = time.perf_counter()
result = getattr(runner, ENTRY)(spark, **kwargs)
wall = round(time.perf_counter() - started, 3)
counts, total_rows = _row_counts()
print(
    f"[run] lakebase wall={wall:.3f}s reported={result.get('elapsed_seconds')} "
    f"status={result.get('status')} deployed={result.get('deployed')} rows={total_rows}",
    flush=True,
)
for table in OUTPUT_TABLES:
    print(f"[run] {table} rows={counts[table]}", flush=True)
display({"result": result, "wall_seconds": wall, "rows": counts})
