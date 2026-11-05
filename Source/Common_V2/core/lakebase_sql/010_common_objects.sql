-- Shared Lakebase (PostgreSQL) objects for the AllocationV2 output_lb procedures.
--
-- Ports of the SQL Server helpers the summary/allocation procedures call:
--   dbo.Split, dbo.udfGetPhaseID, dbo.udfGetLastTransactionID_Phase,
--   dbo.udfGetLatestCustomFootnoteTransactionIDs, dbo.uspAddAllocationLog,
--   dbo.uspUpdateAllocationLog
-- plus "udfLbTrackingKey", the tracking-key CASE expression repeated across
-- every *AllocationSummary insert.
--
-- Identifiers are quoted with their SQL Server spelling, so the Lakebase tables
-- must keep the SQL Server table/column case. __LB_SCHEMA__ is replaced with the
-- runner's schema at deploy time. SQL Server's default collation is
-- case-insensitive, so name lookups compare lower(...) on both sides.

CREATE OR REPLACE FUNCTION "__LB_SCHEMA__"."Split"(p_string text, p_delimiter text)
RETURNS TABLE ("TIds" varchar(8000))
LANGUAGE sql
IMMUTABLE
AS $fn$
    SELECT s::varchar(8000)
    FROM unnest(string_to_array(p_string, p_delimiter)) AS s
    WHERE length(rtrim(s)) > 0
$fn$;


CREATE OR REPLACE FUNCTION "__LB_SCHEMA__"."udfLbTrackingKey"(
    p_is_tracking_key text,
    p_tracking_key    text,
    p_source_entity   bigint,
    p_local_entity    bigint
)
RETURNS varchar(4000)
LANGUAGE sql
IMMUTABLE
AS $fn$
    SELECT CASE
               WHEN upper(p_is_tracking_key) = 'C' THEN
                   CASE
                       WHEN p_tracking_key IS NULL THEN p_source_entity::varchar(4000)
                       ELSE (p_tracking_key || '~' || p_local_entity::varchar)::varchar(4000)
                   END
               ELSE NULL
           END
$fn$;


CREATE OR REPLACE FUNCTION "__LB_SCHEMA__"."udfGetPhaseID"(
    p_client_id     integer,
    p_tax_period_id integer
)
RETURNS integer
LANGUAGE sql
STABLE
SET search_path = "__LB_SCHEMA__"
AS $fn$
    SELECT ph."PhaseID"
    FROM "Phase" ph
    WHERE ph."ClientID" = p_client_id
      AND ph."TaxPeriodID" = p_tax_period_id
      AND ph."EndDate" IS NULL
    LIMIT 1
$fn$;


CREATE OR REPLACE FUNCTION "__LB_SCHEMA__"."udfGetLastTransactionID_Phase"(
    p_client_id      integer,
    p_tax_period_id  integer,
    p_include_failed boolean,
    p_event_type_id  integer,
    p_entity_id      integer DEFAULT -1,
    p_phase_id       integer DEFAULT -1
)
RETURNS integer
LANGUAGE plpgsql
STABLE
SET search_path = "__LB_SCHEMA__"
AS $fn$
DECLARE
    v_exclude_status integer := 0;
    v_phase_id       integer := p_phase_id;
    v_use_entity_id  boolean;
    v_transaction_id integer;
BEGIN
    IF v_phase_id = -1 THEN
        v_phase_id := "udfGetPhaseID"(p_client_id, p_tax_period_id);
    END IF;

    v_use_entity_id := NOT EXISTS (
        SELECT 1
        FROM "ENU_Event" ev
        WHERE ev."EventTypeID" = p_event_type_id
          AND lower(ev."EventName") IN (
              'import_entityrelationship', 'import_historic', 'import_mastertaxableincome',
              'datafeed_byentityinvestment', 'datafeed_entities', 'datafeed_deals-specific',
              'datafeed_investors-specific', 'datafeed_investors', 'datafeed_deals',
              'datafeed_chart of accounts', 'datafeed_financial',
              'import_compositewithholdingbridge', 'import_whpaymentallocation',
              'import_entityconfiguration'
          )
    );

    IF NOT p_include_failed THEN
        IF NOT v_use_entity_id THEN
            SELECT max(tl."TransactionID") INTO v_transaction_id
            FROM "TransactionLog" tl
            WHERE tl."ClientID" = p_client_id
              AND tl."EventTypeID" = p_event_type_id
              AND tl."TaxPeriodID" = p_tax_period_id
              AND tl."StatusID" NOT IN (
                  SELECT ws."StatusID"
                  FROM "WorkflowStatus" ws
                  WHERE lower(ws."EnumerationName") IN ('rejected', 'err_critical', 'err_noncritical')
                  UNION
                  SELECT v_exclude_status
              )
              AND tl."PhaseID" = v_phase_id;
        ELSE
            SELECT max(tl."TransactionID") INTO v_transaction_id
            FROM "TransactionLog" tl
            JOIN "Entity" e ON tl."EntityID" = e."EntityID"
            WHERE tl."ClientID" = p_client_id
              AND tl."EventTypeID" = p_event_type_id
              AND tl."TaxPeriodID" = p_tax_period_id
              AND tl."StatusID" NOT IN (
                  SELECT ws."StatusID"
                  FROM "WorkflowStatus" ws
                  WHERE lower(ws."EnumerationName") IN ('rejected', 'err_critical', 'err_noncritical')
                  UNION
                  SELECT v_exclude_status
              )
              AND e."EntityID" = p_entity_id
              AND tl."PhaseID" = v_phase_id;
        END IF;
    ELSE
        IF NOT v_use_entity_id THEN
            SELECT max(tl."TransactionID") INTO v_transaction_id
            FROM "TransactionLog" tl
            WHERE tl."ClientID" = p_client_id
              AND tl."TaxPeriodID" = p_tax_period_id
              AND tl."EventTypeID" = p_event_type_id
              AND tl."StatusID" <> v_exclude_status
              AND tl."PhaseID" = v_phase_id;
        ELSE
            SELECT max(tl."TransactionID") INTO v_transaction_id
            FROM "TransactionLog" tl
            JOIN "Entity" e ON tl."EntityID" = p_entity_id
            WHERE tl."ClientID" = p_client_id
              AND tl."TaxPeriodID" = p_tax_period_id
              AND tl."EventTypeID" = p_event_type_id
              AND e."EntityID" = p_entity_id
              AND tl."StatusID" <> v_exclude_status
              AND tl."PhaseID" = v_phase_id;
        END IF;
    END IF;

    RETURN v_transaction_id;
END;
$fn$;


CREATE OR REPLACE FUNCTION "__LB_SCHEMA__"."udfGetLatestCustomFootnoteTransactionIDs"(
    p_client_id                       integer,
    p_tax_period_id                   integer,
    p_entity_list_csv                 text,
    p_phase_id                        integer,
    p_register_type_id                integer DEFAULT -1,
    p_is_call_from_reports            integer DEFAULT 0,
    p_is_look_through_entity_list_csv integer DEFAULT 0
)
RETURNS TABLE (
    "EntityID"       integer,
    "TransactionID"  integer,
    "LineTypeID"     integer,
    "EventTypeid"    integer,
    "RegisterTypeID" integer,
    "K1PackageID"    integer
)
LANGUAGE plpgsql
STABLE
SET search_path = "__LB_SCHEMA__"
AS $fn$
#variable_conflict use_column
DECLARE
    v_phase_id           integer := p_phase_id;
    v_investment_type_id integer;
BEGIN
    SELECT et."EntityTypeID" INTO v_investment_type_id
    FROM "ENU_EntityType" et
    WHERE lower(et."EntityTypeName") = 'investment'
      AND et."ClientID" = p_client_id
    LIMIT 1;

    IF v_phase_id IS NULL THEN
        v_phase_id := "udfGetPhaseID"(p_client_id, p_tax_period_id);
    END IF;

    RETURN QUERY
    WITH tmp_entity AS (
        SELECT trim(sp."TIds")::integer AS entity_id
        FROM "Split"(p_entity_list_csv, ',') sp
    ),
    all_entity AS (
        SELECT te.entity_id FROM tmp_entity te
        UNION ALL
        SELECT e."EntityID"
        FROM "EntityRelationship" er
        JOIN tmp_entity te ON er."UpperTierEntityID" = te.entity_id
        JOIN "Entity" e ON er."LowerTierEntityID" = e."EntityID"
        WHERE p_is_look_through_entity_list_csv = 0
          AND e."FundOrInvestmentID" = v_investment_type_id
    ),
    custom_events AS (
        SELECT ee."EventTypeID" AS event_type_id,
               el."LineTypeID"  AS line_type_id,
               cd."GlobalMenuID" AS register_type_id
        FROM "CustomImportDetail" cd
        JOIN "ENU_Event" ee ON lower(cd."ImportName") = lower(ee."EventName")
        JOIN "ENU_LineType" el ON lower(el."LineType") = lower(cd."ImportName")
        WHERE cd."IsCustomFootnote"::int = 1
          AND (p_is_call_from_reports <> 1 OR cd."GlobalMenuID" = p_register_type_id)
    )
    SELECT ae.entity_id,
           "udfGetLastTransactionID_Phase"(
               p_client_id, p_tax_period_id, false, tf.event_type_id, ae.entity_id, v_phase_id
           ),
           tf.line_type_id,
           tf.event_type_id,
           tf.register_type_id,
           COALESCE(k."K1PackageID", 0)
    FROM all_entity ae
    JOIN "K1Package" k ON ae.entity_id = k."UpperTierEntityID"
    CROSS JOIN custom_events tf;
END;
$fn$;


CREATE OR REPLACE PROCEDURE "__LB_SCHEMA__"."uspAddAllocationLog"(
    p_client_id       integer,
    p_tax_period_id   integer,
    p_run_id          integer,
    p_category        varchar(50),
    p_process_name    varchar(100),
    p_start_date      timestamp,
    p_end_date        timestamp,
    p_log_description varchar(1000),
    INOUT p_log_id    integer DEFAULT 0
)
LANGUAGE plpgsql
SET search_path = "__LB_SCHEMA__"
AS $proc$
BEGIN
    INSERT INTO "AllocationLog" (
        "ClientID", "TaxPeriodID", "Category", "LogDescription",
        "ProcessName", "StartDate", "EndDate", "RunID"
    )
    VALUES (
        p_client_id, p_tax_period_id, p_category, p_log_description,
        p_process_name, p_start_date, p_end_date, p_run_id
    )
    RETURNING "LogID" INTO p_log_id;
END;
$proc$;


CREATE OR REPLACE PROCEDURE "__LB_SCHEMA__"."uspUpdateAllocationLog"(
    p_log_id   integer,
    p_end_date timestamp DEFAULT NULL
)
LANGUAGE plpgsql
SET search_path = "__LB_SCHEMA__"
AS $proc$
BEGIN
    UPDATE "AllocationLog"
    SET "EndDate" = COALESCE(p_end_date, clock_timestamp()::timestamp)
    WHERE "LogID" = p_log_id;
END;
$proc$;
