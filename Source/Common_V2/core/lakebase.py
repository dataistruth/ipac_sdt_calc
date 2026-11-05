"""Lakebase (PostgreSQL) helpers for the AllocationV2 output_lb packages.

Every output_lb entry receives its connection settings as ``lakebase_*``
parameters (runner notebook widgets or job task parameters), deploys its
PL/pgSQL objects into ``lakebase_schema`` when needed, and CALLs the procedure.

Parameters (names match the ingestion notebooks):
    lakebase_schema        target schema holding the SQL Server tables (required)
    lakebase_host          Postgres host; falls back to the secret scope / OAuth lookup
    lakebase_port          default 5432
    lakebase_database      default databricks_postgres
    lakebase_auth          "role" (user/password) or "oauth" (Databricks token)
    lakebase_secret_scope  scope holding host/user/password for role auth
    lakebase_host_key, lakebase_user_key, lakebase_password_key
                           secret keys (defaults lakebase_host/lakebase_user/lakebase_password)
    lakebase_user, lakebase_password
                           explicit role credentials (override the secret scope)
    lakebase_instance      provisioned database instance name (oauth)
    lakebase_endpoint      autoscaling endpoint name (oauth)
    lakebase_sslmode       default require
    lakebase_deploy        auto (default) | always | never
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

SCHEMA_TOKEN = "__LB_SCHEMA__"
COMMON_SQL_DIR = Path(__file__).with_name("lakebase_sql")
COMMON_SQL_FILES: tuple[Path, ...] = tuple(sorted(COMMON_SQL_DIR.glob("*.sql")))
DEPLOY_MODES = ("auto", "always", "never")
AUTH_MODES = ("role", "oauth")
_FINGERPRINT_PREFIX = "output_lb sha256:"
_DEPLOY_LOCK_KEY = "AllocationV2.output_lb.deploy"
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


@dataclass(frozen=True)
class LakebaseSettings:
    schema: str
    host: str | None = None
    port: int = 5432
    database: str = "databricks_postgres"
    auth: str = "role"
    user: str | None = None
    password: str | None = field(default=None, repr=False)
    instance: str | None = None
    endpoint: str | None = None
    sslmode: str = "require"
    deploy: str = "auto"


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def validate_identifier(name: str, what: str = "identifier") -> str:
    if not name or not _IDENT.match(name):
        raise ValueError(f"unsafe Lakebase {what}: {name!r}")
    return name


def quote_ident(name: str) -> str:
    return '"' + validate_identifier(name) + '"'


def resolve_dbutils(spark=None, dbutils=None):
    if dbutils is not None:
        return dbutils
    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils

            return DBUtils(spark)
        except Exception:
            pass
    try:
        import IPython

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:
        pass
    return None


def settings_from_params(params: Mapping[str, Any], spark=None, dbutils=None) -> LakebaseSettings:
    """Build settings from ``lakebase_*`` parameters, reading secrets for role auth."""
    get = lambda key, default=None: _text(params.get(key)) or default  # noqa: E731

    schema = get("lakebase_schema")
    if not schema:
        raise ValueError("lakebase_schema is required for output_lb")
    validate_identifier(schema, "schema")

    auth = get("lakebase_auth", "role").lower()
    if auth not in AUTH_MODES:
        raise ValueError(f"lakebase_auth must be one of {AUTH_MODES}, got {auth!r}")
    deploy = get("lakebase_deploy", "auto").lower()
    if deploy not in DEPLOY_MODES:
        raise ValueError(f"lakebase_deploy must be one of {DEPLOY_MODES}, got {deploy!r}")

    host = get("lakebase_host")
    user = get("lakebase_user")
    password = get("lakebase_password")
    scope = get("lakebase_secret_scope")
    if scope and (not host or (auth == "role" and (not user or not password))):
        dbu = resolve_dbutils(spark, dbutils)
        if dbu is None:
            raise RuntimeError("lakebase_secret_scope is set but dbutils is unavailable")

        def secret(key_param: str, default_key: str) -> str | None:
            return _text(dbu.secrets.get(scope, get(key_param, default_key)))

        host = host or secret("lakebase_host_key", "lakebase_host")
        if auth == "role":
            user = user or secret("lakebase_user_key", "lakebase_user")
            password = password or secret("lakebase_password_key", "lakebase_password")

    if auth == "role" and not (host and user and password):
        raise ValueError(
            "role auth needs lakebase_host/lakebase_user/lakebase_password "
            "(directly or through lakebase_secret_scope)"
        )
    if auth == "oauth" and not (get("lakebase_instance") or get("lakebase_endpoint")):
        raise ValueError("oauth auth needs lakebase_instance or lakebase_endpoint")

    return LakebaseSettings(
        schema=schema,
        host=host,
        port=int(get("lakebase_port", "5432")),
        database=get("lakebase_database", "databricks_postgres"),
        auth=auth,
        user=user,
        password=password,
        instance=get("lakebase_instance"),
        endpoint=get("lakebase_endpoint"),
        sslmode=get("lakebase_sslmode", "require"),
        deploy=deploy,
    )


def _oauth_login(settings: LakebaseSettings) -> tuple[str, str, str]:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    user = settings.user or w.current_user.me().user_name
    host = settings.host
    if settings.instance:
        if not host:
            host = w.database.get_database_instance(name=settings.instance).read_write_dns
        token = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[settings.instance]
        ).token
    else:
        if not host:
            host = w.postgres.get_endpoint(name=settings.endpoint).status.hosts.host
        token = w.postgres.generate_database_credential(endpoint=settings.endpoint).token
    return host, user, token


def connect(settings: LakebaseSettings, application_name: str = "ipac_output_lb"):
    """Open a non-autocommit DB-API connection (psycopg 3, else psycopg2)."""
    if settings.auth == "oauth":
        host, user, password = _oauth_login(settings)
    else:
        host, user, password = settings.host, settings.user, settings.password
    kwargs = dict(
        host=host,
        port=settings.port,
        dbname=settings.database,
        user=user,
        password=password,
        sslmode=settings.sslmode,
        application_name=application_name,
    )
    try:
        import psycopg

        return psycopg.connect(**kwargs)
    except ImportError:
        import psycopg2

        return psycopg2.connect(**kwargs)


def render_sql(paths: Iterable[Path], schema: str) -> str:
    validate_identifier(schema, "schema")
    parts = []
    for path in paths:
        text = Path(path).read_text(encoding="utf-8-sig")
        parts.append(f"-- source: {Path(path).name}\n{text.replace(SCHEMA_TOKEN, schema)}")
    return "\n\n".join(parts)


def _procedure_signature(cur, schema: str, procedure: str) -> tuple[str, str | None] | None:
    cur.execute(
        "SELECT p.oid::regprocedure::text, obj_description(p.oid, 'pg_proc') "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = %s AND p.proname = %s AND p.prokind = 'p' "
        "ORDER BY p.oid DESC LIMIT 1",
        (schema, procedure),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def deploy(conn, settings: LakebaseSettings, procedure: str, sql_files: Sequence[Path]) -> bool:
    """Create/replace the SQL objects; with ``auto`` only when the SQL changed.

    Returns True when the SQL was (re)deployed. The fingerprint of the rendered
    SQL is stored as the procedure's COMMENT.
    """
    validate_identifier(procedure, "procedure")
    if settings.deploy == "never":
        return False
    sql = render_sql(sql_files, settings.schema)
    fingerprint = _FINGERPRINT_PREFIX + hashlib.sha256(sql.encode("utf-8")).hexdigest()
    try:
        with conn.cursor() as cur:
            if settings.deploy == "auto":
                current = _procedure_signature(cur, settings.schema, procedure)
                if current and current[1] == fingerprint:
                    conn.rollback()
                    return False
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (_DEPLOY_LOCK_KEY,))
            if settings.deploy == "auto":
                current = _procedure_signature(cur, settings.schema, procedure)
                if current and current[1] == fingerprint:
                    conn.rollback()
                    return False
            cur.execute(sql)
            current = _procedure_signature(cur, settings.schema, procedure)
            if current is None:
                raise RuntimeError(f"{settings.schema}.{procedure} was not created by its SQL files")
            cur.execute(f"COMMENT ON PROCEDURE {current[0]} IS '{fingerprint}'")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info("Deployed %s.%s (%s files)", settings.schema, procedure, len(sql_files))
    return True


def call_procedure(conn, schema: str, procedure: str, args: Sequence[tuple[Any, str]]):
    """CALL schema.procedure(args) in one transaction and return the INOUT row.

    ``args`` are ``(value, pg_type)`` pairs; the explicit casts keep procedure
    resolution independent of how the driver types Python ints and None.
    """
    for _, pg_type in args:
        if not re.match(r"^[a-z][a-z0-9_ ]*(\(\d+(,\d+)?\))?$", pg_type):
            raise ValueError(f"unsafe pg type: {pg_type!r}")
    placeholders = ", ".join(f"%s::{pg_type}" for _, pg_type in args)
    statement = f"CALL {quote_ident(schema)}.{quote_ident(procedure)}({placeholders})"
    try:
        with conn.cursor() as cur:
            cur.execute(statement, [value for value, _ in args])
            row = cur.fetchone() if cur.description else None
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return row


def run_procedure(
    params: Mapping[str, Any],
    procedure: str,
    args: Sequence[tuple[Any, str]],
    sql_files: Sequence[Path],
    spark=None,
    dbutils=None,
) -> dict:
    """Connect, deploy if needed, CALL; returns ``{"row", "deployed", "schema"}``."""
    settings = settings_from_params(params, spark=spark, dbutils=dbutils)
    conn = connect(settings, application_name=f"ipac_output_lb.{procedure}")
    try:
        deployed = deploy(conn, settings, procedure, sql_files)
        row = call_procedure(conn, settings.schema, procedure, args)
    finally:
        conn.close()
    return {"row": row, "deployed": deployed, "schema": settings.schema}
