-- Lakebase (PostgreSQL) port of dbo.uspAddAllocationSummary.
--
-- Requires Common_V2/core/lakebase_sql/010_common_objects.sql (udfLbTrackingKey,
-- udfGetLatestCustomFootnoteTransactionIDs, uspAddAllocationLog,
-- uspUpdateAllocationLog). __LB_SCHEMA__ is replaced at deploy time.
--
-- Differences from the T-SQL that do not change results:
--   * #AllocationOutput is only read for the periodic K1/M1 inserts, so the
--     snapshot keeps just those rows (PeriodID NOT NULL, K1/M1 line types).
--   * Variables and temp tables the T-SQL computes but never reads
--     (#AtRiskWorkflow, #QuicklinkID, @InvLineID, @TaxClassID, @IsPFIC,
--     event/entity-type ids) are not ported.
--   * The whole CALL runs in one transaction: a failure rolls back every
--     summary insert instead of leaving a partial load.
--
-- SQL Server semantics reproduced explicitly:
--   * case-insensitive collation: name/flag comparisons use lower()/upper();
--   * CHAR(1) variables truncate GlobalMenu.State ('CG' -> 'C'): left(State, 1);
--   * SELECT @var = ... keeps the previous value when no row matches: FOUND.
--   * GlobalMenu / PFICFootnoteLineItem lookups that the T-SQL leaves unscoped
--     are scoped to the run's client and tax period (same as the PySpark port)
--     so they resolve to a single deterministic row.

CREATE OR REPLACE PROCEDURE "__LB_SCHEMA__"."uspAddAllocationSummary"(
    p_entity_id     integer,
    p_client_id     bigint,
    p_tax_period_id integer,
    p_run_id        bigint,
    p_write_log     boolean DEFAULT true,
    INOUT p_status  text DEFAULT NULL
)
LANGUAGE plpgsql
SET search_path = "__LB_SCHEMA__"
AS $proc$
DECLARE
    v_entity_id      integer := p_entity_id;
    v_client_id      integer := p_client_id;
    v_tax_period_id  integer := p_tax_period_id;
    v_run_id         bigint  := p_run_id;
    v_start_date     timestamp := clock_timestamp()::timestamp;
    v_log_id         integer := 0;

    v_k1_lt          integer;
    v_m1_lt          integer;
    v_boxjkl_lt      integer;
    v_form926_lt     integer;
    v_form8865_lt    integer;
    v_form199a_lt    integer;
    v_form8886_lt    integer;
    v_pfic_fn_lt     integer;
    v_line18a_lt     integer;
    v_ubti_lt        integer;
    v_passive_lt     integer;
    v_gaap_lt        integer;
    v_atrisk_lt      integer;
    v_book_k1_adj_lt integer;

    v_cum_qef_line       integer;
    v_cum_qef_dist_line  integer;
    v_rev_py_qef_line    integer;
    v_cfc_income_line    integer;
    v_cfc_loss_line      integer;
    v_election_d_line    integer;
    v_type_of_pfic_line  integer;

    v_is_tracking_key    text := 'C';
    v_is_k1_intl         boolean;
    v_is_domestic_blocker boolean;
    v_is_auto_elec_d     boolean;
    v_alloc_type_name    text;
    v_inv_level_rounding text;
    v_allow_pe_book      boolean;

    v_run_status         text;
    v_phase_id           integer;
    v_partner_workflow_id    integer;
    v_partner_transaction_id integer;
    v_fx_rate_txn_id         integer;
BEGIN
    IF p_write_log THEN
        CALL "uspAddAllocationLog"(
            v_client_id, v_tax_period_id, v_run_id::integer, 'Add Allocaton Summary',
            'uspAddAllocationSummary', v_start_date, NULL, 'Loading the summary tables', v_log_id
        );
    END IF;

    SELECT left(gm."State", 1) INTO v_is_tracking_key
    FROM "GlobalMenu" gm
    WHERE lower(trim(gm."MenuName")) = 'keep tracking keys'
      AND gm."ClientID" = v_client_id
      AND gm."TaxPeriodID" = v_tax_period_id
    LIMIT 1;
    IF NOT FOUND THEN
        v_is_tracking_key := 'C';
    END IF;

    SELECT ar."RunStatus", ar."PhaseID", ar."PartnerWorkflowID",
           ar."PartnerTransactionID", ar."ForeignCurrencyRateTransactionID"
    INTO v_run_status, v_phase_id, v_partner_workflow_id,
         v_partner_transaction_id, v_fx_rate_txn_id
    FROM "AllocationRun" ar
    WHERE ar."RunID" = v_run_id
      AND ar."ClientID" = v_client_id
      AND ar."TaxPeriodID" = v_tax_period_id
    LIMIT 1;

    IF upper(v_run_status) = 'FAIL' THEN
        IF p_write_log THEN
            CALL "uspUpdateAllocationLog"(v_log_id, clock_timestamp()::timestamp);
        END IF;
        p_status := 'SKIPPED';
        RETURN;
    END IF;

    SELECT
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'k1'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'm1-adjustment'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'boxjkl'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'form926'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'form8865'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'form199a'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'form8886'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'pfic footnote'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'line 18a'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'ubti'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'passive income'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'gaap to tax'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'at risk'),
        max(lt."LineTypeID") FILTER (WHERE lower(trim(lt."LineType")) = 'book k-1 adjustments')
    INTO v_k1_lt, v_m1_lt, v_boxjkl_lt, v_form926_lt, v_form8865_lt, v_form199a_lt,
         v_form8886_lt, v_pfic_fn_lt, v_line18a_lt, v_ubti_lt, v_passive_lt, v_gaap_lt,
         v_atrisk_lt, v_book_k1_adj_lt
    FROM "ENU_LineType" lt
    WHERE lt."ClientID" = v_client_id
      AND lt."TaxPeriodID" = v_tax_period_id;

    SELECT
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'cumulativeqefinclusion'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'cumulativeqefdistributions'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'reversalpyqefinclusion'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'cfctestedincome'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'cfctestedloss'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'rdelectionddeemed'),
        max(li."LineID") FILTER (WHERE lower(trim(li."ShortName")) = 'typeofpfic')
    INTO v_cum_qef_line, v_cum_qef_dist_line, v_rev_py_qef_line, v_cfc_income_line,
         v_cfc_loss_line, v_election_d_line, v_type_of_pfic_line
    FROM "PFICFootnoteLineItem" li
    WHERE li."ClientID" = v_client_id
      AND li."TaxPeriodID" = v_tax_period_id;

    SELECT COALESCE(e."IsDomesticBlocker"::int, 0) = 1 INTO v_is_domestic_blocker
    FROM "Entity" e
    WHERE e."EntityID" = v_entity_id
    LIMIT 1;
    v_is_domestic_blocker := COALESCE(v_is_domestic_blocker, false);

    v_is_k1_intl := EXISTS (
        SELECT 1
        FROM "GlobalMenu" gm
        JOIN "ENU_GlobalMenuGroup" gmg ON gm."GlobalMenuGroupID" = gmg."GlobalMenuGroupID"
        WHERE lower(trim(gmg."GroupName")) = 'other configuration'
          AND lower(trim(gm."MenuName")) = 'separate international sign off'
          AND upper(gm."State") = 'C'
          AND gm."ClientID" = v_client_id
          AND gm."TaxPeriodID" = v_tax_period_id
    );

    SELECT al."AllocationTypeName" INTO v_alloc_type_name
    FROM "ENU_AllocationLogic" al
    JOIN "Entity" e ON al."AllocationTypeID" = e."AllocationTypeID"
    WHERE e."EntityID" = v_entity_id
      AND e."ClientID" = v_client_id
    LIMIT 1;

    SELECT left(gm."State", 1) INTO v_inv_level_rounding
    FROM "GlobalMenu" gm
    JOIN "ENU_GlobalMenuGroup" eg
      ON eg."GlobalMenuGroupID" = gm."GlobalMenuGroupID"
     AND lower(trim(eg."GroupName")) = 'other configuration'
    WHERE lower(trim(gm."MenuName")) = 'investment level rounding logic'
      AND gm."ClientID" = v_client_id
      AND gm."TaxPeriodID" = v_tax_period_id
    LIMIT 1;

    SELECT upper(COALESCE(gm."State", 'U')) IN ('C', 'CG') INTO v_is_auto_elec_d
    FROM "GlobalMenu" gm
    WHERE lower(trim(gm."MenuName")) = 'automate deemed sale election'
      AND gm."ClientID" = v_client_id
      AND gm."TaxPeriodID" = v_tax_period_id
    LIMIT 1;
    v_is_auto_elec_d := COALESCE(v_is_auto_elec_d, false);

    -- ((name = 'PE Book Allocation' AND rounding = 'U') OR name <> 'PE Book Allocation');
    -- a NULL allocation type name makes the T-SQL predicate false.
    v_allow_pe_book := CASE
        WHEN v_alloc_type_name IS NULL THEN false
        WHEN lower(trim(v_alloc_type_name)) = 'pe book allocation'
            THEN COALESCE(upper(v_inv_level_rounding) = 'U', false)
        ELSE true
    END;

    DROP TABLE IF EXISTS tmp_aas_k1_workflow, tmp_aas_lower_tier_funds, tmp_aas_ao_periodic,
        tmp_aas_aos, tmp_aas_partner_pfic, tmp_aas_reclass_pfic, tmp_aas_pfic_text,
        tmp_aas_pfic_aos, tmp_aas_pfic_flowup_tk, tmp_aas_cf_line_types,
        tmp_aas_cf_line_item, tmp_aas_cf_ids, tmp_aas_alloc_pct;

    CREATE TEMP TABLE tmp_aas_k1_workflow ("EntityID" integer, "WorkflowID" integer);

    INSERT INTO tmp_aas_k1_workflow ("EntityID", "WorkflowID")
    SELECT aiw."EntityID", aiw."K1WorkflowID"
    FROM "AllocationInputWorkflow" aiw
    WHERE aiw."RunID" = v_run_id;

    IF v_is_k1_intl THEN
        INSERT INTO tmp_aas_k1_workflow ("EntityID", "WorkflowID")
        SELECT aiw."EntityID", aiw."K1InternationalWorkflowID"
        FROM "AllocationInputWorkflow" aiw
        WHERE aiw."RunID" = v_run_id;
    END IF;

    DELETE FROM tmp_aas_k1_workflow WHERE "WorkflowID" IS NULL;

    CREATE TEMP TABLE tmp_aas_lower_tier_funds AS
    SELECT ltf."EntityID", ltf."PartnerNumber", ltf."LTRunID" AS "RunID"
    FROM "LowerTierFunds" ltf
    WHERE ltf."RunID" = v_run_id;

    CREATE TEMP TABLE tmp_aas_ao_periodic AS
    SELECT ao."ShareClass", ao."PartnerNumber", ao."LineTypeID", ao."LineID",
           ao."Amount", ao."PeriodID", ao."LineCode"
    FROM "AllocationOutput" ao
    WHERE ao."RunID" = v_run_id
      AND ao."PeriodID" IS NOT NULL
      AND ao."LineTypeID" IN (v_k1_lt, v_m1_lt);

    CREATE TEMP TABLE tmp_aas_aos AS
    SELECT aos."RunID", aos."ClientID", aos."EntityID", aos."ShareClass", aos."PartnerNumber",
           aos."LineTypeID", aos."LineID", aos."Amount", aos."QuicklinkID", aos."Amount704b",
           aos."CategoryID", aos."LineCode", aos."AdjustmentTypeID", aos."Tag",
           aos."OriginalParentEntityID", aos."ParentEntityId", aos."TrackingKey", aos."SchID"
    FROM "AllocationOutputSummary" aos
    WHERE aos."RunID" = v_run_id;

    CREATE INDEX ON tmp_aas_aos ("LineTypeID");
    ANALYZE tmp_aas_aos;

    -- K1
    IF v_allow_pe_book THEN
        INSERT INTO "K1AllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "LineID", "Amount", "FlowupAmount"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
               ao."LineID", ao."Amount", ao."Amount"
        FROM tmp_aas_aos ao
        WHERE ao."LineTypeID" = v_k1_lt;
    END IF;

    -- M1 Adjustments
    INSERT INTO "M1AdjAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "LineID", "Amount", "FlowupAmount", "WorkPaperCode"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, COALESCE(ao."ShareClass", ''),
           ao."PartnerNumber", ao."LineID", COALESCE(ao."Amount", 0), COALESCE(ao."Amount", 0),
           ao."LineCode"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_m1_lt;

    -- Periodic K1 / M1
    IF EXISTS (
        SELECT 1 FROM "AllocationOutput" ao
        WHERE ao."RunID" = v_run_id AND ao."PeriodID" IS NOT NULL
    ) THEN
        INSERT INTO "K1AllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "LineID", "Amount", "FlowupAmount", "PeriodID"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, COALESCE(ao."ShareClass", ''),
               ao."PartnerNumber", ao."LineID", sum(COALESCE(ao."Amount", 0)),
               sum(COALESCE(ao."Amount", 0)), ao."PeriodID"
        FROM tmp_aas_ao_periodic ao
        WHERE ao."LineTypeID" = v_k1_lt
        GROUP BY COALESCE(ao."ShareClass", ''), ao."PartnerNumber", ao."LineID", ao."PeriodID";

        INSERT INTO "M1AdjAllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "LineID", "Amount", "FlowupAmount", "PeriodID", "WorkPaperCode"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, COALESCE(ao."ShareClass", ''),
               ao."PartnerNumber", ao."LineID", sum(COALESCE(ao."Amount", 0)),
               sum(COALESCE(ao."Amount", 0)), ao."PeriodID", ao."LineCode"
        FROM tmp_aas_ao_periodic ao
        WHERE ao."LineTypeID" = v_m1_lt
        GROUP BY COALESCE(ao."ShareClass", ''), ao."PartnerNumber", ao."LineID", ao."PeriodID", ao."LineCode";
    END IF;

    -- Box JKL
    INSERT INTO "BoxJKLAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "LineID", "Amount", "FlowupAmount"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."LineID", ao."Amount", ao."Amount"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_boxjkl_lt;

    -- Form 926
    INSERT INTO "Form926AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form926ID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_form926_lt;

    -- Form 8865
    INSERT INTO "Form8865AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form8865ID", "LineID", "Amount", "FlowupAmount", "TextValue", "SchID",
        "ParentEntityID", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           COALESCE(ao."SchID", 0), ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_form8865_lt;

    -- Form 199A
    INSERT INTO "Form199AAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form199AID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_form199a_lt;

    -- Partners of the run (TFS#36641)
    CREATE TEMP TABLE tmp_aas_partner_pfic AS
    SELECT DISTINCT COALESCE(ps."ShareClass", '') AS "ShareClass", ps."PartnerNumber"
    FROM "Partner_Snapshot" ps
    WHERE COALESCE(ps."WorkFlowID", ps."Transactionid")
              = COALESCE(v_partner_workflow_id, v_partner_transaction_id)
      AND ps."EntityID" = v_entity_id
      AND ps."Clientid" = v_client_id;

    -- PFIC Footnote
    CREATE TEMP TABLE tmp_aas_reclass_pfic (
        "SourceEntityID"         integer,
        "PFICFootnoteID"         integer,
        "LineID"                 integer,
        "Amount"                 double precision,
        "FlowupAmount"           double precision,
        "TextValue"              varchar(4000),
        "ParentEntityId"         integer,
        "TrackingKey"            varchar(4000),
        "OriginalParentEntityID" integer
    );

    CREATE TEMP TABLE tmp_aas_pfic_text AS
    SELECT p."RunID", p."PFICFootnoteID", p."LineID"
    FROM "PFICFootnoteAllocationText" p
    WHERE p."RunID" = v_run_id;

    CREATE TEMP TABLE tmp_aas_pfic_aos AS
    SELECT DISTINCT ao."EntityID", ao."QuicklinkID", ao."TrackingKey"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_pfic_fn_lt;

    INSERT INTO tmp_aas_reclass_pfic (
        "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "TextValue",
        "ParentEntityId", "TrackingKey"
    )
    SELECT kw."EntityID", pfic."PFICFootnoteID", pfic."LineID",
           CASE
               WHEN pfic."LineID" IN (v_cfc_income_line, v_cfc_loss_line)
                   THEN COALESCE(pfic."Amount", 0) / COALESCE(r."AverageRate", 1)
               ELSE COALESCE(pfic."Amount", 0)
           END,
           pfic."TextValue", 0,
           kw."EntityID"::varchar || '~' || v_entity_id::varchar
    FROM "PFICFootnoteInput_Snapshot" pfic
    JOIN "PFICFootnoteLineItem" fl
      ON pfic."LineID" = fl."LineID"
     AND fl."ClientID" = pfic."ClientID"
     AND fl."TaxPeriodID" = pfic."TaxPeriodID"
     AND COALESCE(fl."IsAllocated"::int, 0) = 0
     AND fl."LineID" NOT IN (v_cum_qef_line, v_cum_qef_dist_line, v_rev_py_qef_line)
    JOIN tmp_aas_k1_workflow kw ON pfic."WorkflowID" = kw."WorkflowID"
    JOIN "Entity" e ON e."EntityID" = kw."EntityID"
    JOIN tmp_aas_pfic_aos ao
      ON ao."EntityID" = e."EntityID"
     AND ao."QuicklinkID" = pfic."PFICFootnoteID"
    LEFT JOIN "ForeignCurrencyAverageRate" r
      ON fl."ClientID" = r."ClientID"
     AND r."CurrencyCode" = e."CurrencyCode"
     AND r."TransactionID" = v_fx_rate_txn_id
    LEFT JOIN tmp_aas_pfic_text p
      ON p."RunID" = v_run_id
     AND p."PFICFootnoteID" = pfic."PFICFootnoteID"
     AND p."LineID" = fl."LineID"
    WHERE fl."ClientID" = v_client_id
      AND fl."TaxPeriodID" = v_tax_period_id
      AND p."PFICFootnoteID" IS NULL;

    INSERT INTO "PFICFootnoteAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_pfic_fn_lt;

    INSERT INTO tmp_aas_reclass_pfic (
        "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT pfic."SourceEntityID", pfic."FootnoteID", pfic."LineID", pfic."Amount",
           pfic."FlowupAmount", pfic."TextValue",
           CASE
               WHEN COALESCE(pfic."ParentEntityID", 0) = 0 THEN
                   CASE WHEN pfic."LTEntityID" = k."LowerTierEntityID" THEN 0 ELSE pfic."LTEntityID" END
               ELSE pfic."ParentEntityID"
           END,
           "udfLbTrackingKey"(v_is_tracking_key, pfic."TrackingKey", pfic."EntityID", v_entity_id),
           pfic."OriginalParentEntityID"
    FROM "ReclassFootnoteAllocationData" pfic
    JOIN "PFICFootnoteLineItem" fl
      ON pfic."LineID" = fl."LineID"
     AND fl."ClientID" = pfic."ClientID"
     AND fl."TaxPeriodID" = pfic."TaxPeriodID"
     AND COALESCE(fl."IsAllocated"::int, 0) = 0
    JOIN "PFICFootnotePackage" p ON p."PFICFootnoteID" = pfic."FootnoteID"
    JOIN "K1Package" k ON k."K1PackageID" = p."K1PackageID"
    JOIN tmp_aas_pfic_aos ao
      ON ao."QuicklinkID" = pfic."FootnoteID"
     AND ao."EntityID" = pfic."SourceEntityID"
     AND ao."TrackingKey" = pfic."TrackingKey"
    WHERE fl."ClientID" = v_client_id
      AND fl."TaxPeriodID" = v_tax_period_id
      AND pfic."RunID" = v_run_id
      AND pfic."LineTypeID" = v_pfic_fn_lt;

    -- Part 5 / Part 7 values derived from PFIC Matrix Automation
    UPDATE tmp_aas_reclass_pfic pfa
    SET "TextValue" = pf."TextValue"
    FROM "PFICfootnoteflowup" pf, "PFICFootnoteLineItem" l
    WHERE pfa."PFICFootnoteID" = pf."PFICFootnoteID"
      AND pf."LineID" = pfa."LineID"
      AND pf."SourceEntityID" = pfa."SourceEntityID"
      AND lower(l."ShortName") IN ('ispart_7', 'ispart_5')
      AND pfa."LineID" = l."LineID"
      AND pf."RunID" = v_run_id;

    IF v_is_auto_elec_d THEN
        CREATE TEMP TABLE tmp_aas_pfic_flowup_tk AS
        SELECT f."RunID", f."ClientID", f."TaxPeriodID", f."EntityID", f."SourceEntityID",
               f."PFICFootnoteID", f."LineID", f."Amount", f."TextValue", f."TrackingKey"
        FROM "PFICFootnoteFlowupWithTrackingKey" f
        WHERE f."RunID" = v_run_id
          AND f."LineID" = v_election_d_line;

        UPDATE tmp_aas_reclass_pfic pfa
        SET "TextValue" = pf."TextValue"
        FROM tmp_aas_pfic_flowup_tk pf
        WHERE COALESCE(pfa."PFICFootnoteID", 0) = COALESCE(pf."PFICFootnoteID", 0)
          AND COALESCE(pfa."SourceEntityID", 0) = COALESCE(pf."SourceEntityID", 0)
          AND pfa."LineID" = pf."LineID"
          AND COALESCE(pfa."TrackingKey", '') = COALESCE(pf."TrackingKey", '');

        INSERT INTO tmp_aas_reclass_pfic (
            "SourceEntityID", "PFICFootnoteID", "LineID", "Amount", "FlowupAmount", "TextValue",
            "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
        )
        SELECT pfa."SourceEntityID", pfa."PFICFootnoteID", pfa."LineID", NULL, NULL, pfa."TextValue",
               0, pfa."TrackingKey", NULL
        FROM tmp_aas_reclass_pfic pf
        JOIN tmp_aas_pfic_flowup_tk pfa
          ON COALESCE(pfa."SourceEntityID", 0) = COALESCE(pf."SourceEntityID", 0)
         AND pfa."PFICFootnoteID" = pf."PFICFootnoteID"
         AND COALESCE(pfa."TrackingKey", '') = COALESCE(pf."TrackingKey", '')
         AND pfa."LineID" = pf."LineID"
        LEFT JOIN tmp_aas_reclass_pfic tpf
          ON pf."PFICFootnoteID" = tpf."PFICFootnoteID"
         AND pf."SourceEntityID" = tpf."SourceEntityID"
         AND pf."LineID" = tpf."LineID"
         AND pf."TrackingKey" = tpf."TrackingKey"
        WHERE tpf."LineID" IS NULL;

        DROP TABLE IF EXISTS tmp_aas_pfic_flowup_tk;
    END IF;

    -- Domestic blocker: only 1293 deemed / 1291 any-distribution PFICs flow up
    IF v_is_domestic_blocker THEN
        DELETE FROM tmp_aas_reclass_pfic s
        WHERE s."PFICFootnoteID" IN (
                SELECT d."PFICFootnoteID"
                FROM tmp_aas_reclass_pfic d
                WHERE d."SourceEntityID" <> v_entity_id
            )
          AND s."PFICFootnoteID" NOT IN (
                SELECT d."PFICFootnoteID"
                FROM tmp_aas_reclass_pfic d
                WHERE d."SourceEntityID" = v_entity_id
                  AND d."LineID" = v_type_of_pfic_line
                  AND upper(d."TextValue") NOT IN ('IS1293ELIGIBLEDEEMED', 'IS1291ANYDISTRIBUTION')
            );
    END IF;

    INSERT INTO "PFICFootnoteAllocationText" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "SourceEntityID", "PFICFootnoteID",
        "LineID", "Amount", "TextValue", "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, t."SourceEntityID",
           t."PFICFootnoteID", t."LineID", t."Amount", t."TextValue", t."ParentEntityId",
           t."TrackingKey", t."OriginalParentEntityID"
    FROM tmp_aas_reclass_pfic t;

    -- Custom Footnotes. Line types come from every latest-transaction row,
    -- including rows whose TransactionID is NULL (the T-SQL deletes those
    -- only after taking the line types).
    CREATE TEMP TABLE tmp_aas_cf_line_types AS
    SELECT DISTINCT cft."LineTypeID"
    FROM "udfGetLatestCustomFootnoteTransactionIDs"(
        v_client_id, v_tax_period_id, v_entity_id::varchar(10), v_phase_id, -1, 0, 0
    ) cft;

    CREATE TEMP TABLE tmp_aas_cf_line_item AS
    SELECT cfl."LineID"
    FROM "CustomFootnoteLineItem" cfl
    WHERE cfl."ClientID" = v_client_id
      AND cfl."TaxPeriodID" = v_tax_period_id
      AND COALESCE(cfl."IsAllocable"::int, 0) = 0
    UNION ALL
    SELECT -1;

    CREATE TEMP TABLE tmp_aas_cf_ids AS
    SELECT DISTINCT ao."QuicklinkID" AS "CustomFootnoteID"
    FROM tmp_aas_aos ao
    JOIN tmp_aas_cf_line_types cft ON cft."LineTypeID" = ao."LineTypeID";

    INSERT INTO "CustomFootnoteAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "CustomFootnoteID", "LineID", "LineTypeID", "Amount", "FlowupAmount",
        "TextValue", "ParentEntityId", "TrackingKey"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           cfi."SourceEntityID", cfi."CustomFootnoteID", cfi."LineID", cfi."LineTypeID",
           cfi."Amount", cfi."Amount", cfi."TextValue", 0, v_entity_id::varchar
    FROM "CustomFootnoteFlowup" cfi
    JOIN tmp_aas_cf_line_item cfl ON cfi."LineID" = cfl."LineID"
    JOIN tmp_aas_cf_ids cf ON cfi."CustomFootnoteID" = cf."CustomFootnoteID"
    CROSS JOIN tmp_aas_partner_pfic ao
    WHERE cfi."RunID" = v_run_id;

    INSERT INTO "CustomFootnoteAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "CustomFootnoteID", "LineID", "LineTypeID", "Amount", "FlowupAmount",
        "TextValue", "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."EntityID", ao."QuicklinkID", ao."LineID", ao."LineTypeID", ao."Amount", ao."Amount",
           NULL::varchar, ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    JOIN tmp_aas_cf_line_types cft ON cft."LineTypeID" = ao."LineTypeID";

    -- Line 18A
    INSERT INTO "Line18AAllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "LocationID", "Amount", "FlowupAmount"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           ao."LineID", ao."Amount", ao."Amount"
    FROM tmp_aas_aos ao
    WHERE ao."LineTypeID" = v_line18a_lt;

    IF v_allow_pe_book THEN
        -- UBTI
        INSERT INTO "UBTIAllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "UBTIType", "LineID", "Amount", "FlowupAmount"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
               CASE WHEN ao."QuicklinkID" = 1 THEN 'Qualified'
                    WHEN ao."QuicklinkID" = 2 THEN 'Non-Qualified'
               END,
               ao."LineID", ao."Amount", ao."Amount"
        FROM tmp_aas_aos ao
        WHERE ao."LineTypeID" = v_ubti_lt;

        -- Passive Income
        INSERT INTO "PassiveIncomeAllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "LineID", "Amount", "FlowupAmount"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
               ao."LineID", ao."Amount", ao."Amount"
        FROM tmp_aas_aos ao
        WHERE ao."LineTypeID" = v_passive_lt;
    END IF;

    -- Form 2006-16
    CREATE TEMP TABLE tmp_aas_alloc_pct AS
    SELECT DISTINCT ap."PartnerNumber", COALESCE(ap."ShareClass", '') AS "ShareClass"
    FROM "AllocationPercentage" ap
    WHERE ap."RunID" = v_run_id
      AND ap."ClientID" = v_client_id;

    INSERT INTO "Form200616AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form2006EntityID"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ap."ShareClass", ap."PartnerNumber",
           kw."EntityID", f2006."Form2006EntityID"
    FROM "Form200616_Snapshot" f2006
    JOIN tmp_aas_k1_workflow kw ON f2006."WorkFlowID" = kw."WorkflowID"
    CROSS JOIN tmp_aas_alloc_pct ap
    WHERE f2006."ClientID" = v_client_id
      AND f2006."TaxPeriodID" = v_tax_period_id;

    INSERT INTO "Form200616AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form2006EntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ap."ShareClass", ap."PartnerNumber",
           f2006."SourceEntityID", f2006."Form2006EntityID"
    FROM "Form200616AllocationSummary" f2006
    JOIN tmp_aas_lower_tier_funds lt
      ON f2006."RunID" = lt."RunID"
     AND f2006."EntityID" = lt."EntityID"
     AND f2006."PartnerNumber" = lt."PartnerNumber"
    CROSS JOIN tmp_aas_alloc_pct ap
    WHERE f2006."ClientID" = v_client_id
      AND f2006."TaxPeriodID" = v_tax_period_id;

    -- Form 8886
    INSERT INTO "Form8886AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form8886ID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "TransactionName", "TransactionEntityID", "Comments", "SecIIComments",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           kw."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           f8886."TransactionName", f8886."EntityID", left(f8886."Comments", 1000),
           left(f8886."SecIIComments", 1000), ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    JOIN "Form8886Input_Snapshot" f8886
      ON ao."QuicklinkID" = f8886."Form8886ID"
     AND ao."LineID" = f8886."LineID"
    JOIN tmp_aas_k1_workflow kw ON f8886."WorkFlowID" = kw."WorkflowID"
    JOIN "Form8886LineItem" f ON f."LineID" = f8886."LineID"
    WHERE ao."LineTypeID" = v_form8886_lt
      AND COALESCE(f."IsActive"::int, 0) = 1;

    INSERT INTO "Form8886AllocationSummary" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
        "SourceEntityID", "Form8886ID", "LineID", "Amount", "FlowupAmount", "TextValue",
        "TransactionName", "TransactionEntityID", "Comments", "SecIIComments",
        "ParentEntityId", "TrackingKey", "OriginalParentEntityID"
    )
    SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
           f8886."SourceEntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount", NULL::varchar,
           f8886."TransactionName", f8886."TransactionEntityID", left(f8886."Comments", 1000),
           left(f8886."SecIIComments", 1000), ao."ParentEntityId",
           "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
           ao."OriginalParentEntityID"
    FROM tmp_aas_aos ao
    JOIN "Form8886LineItem" f
      ON f."LineID" = ao."LineID"
     AND COALESCE(f."IsActive"::int, 0) = 1
    JOIN (
        SELECT DISTINCT rf."SourceEntityID", rf."FootnoteID" AS "Form8886ID", rf."TransactionName",
               rf."TransactionEntityID", rf."LineID", rf."Comments", rf."SecIIComments"
        FROM "ReclassFootnoteAllocationData" rf
        WHERE rf."RunID" = v_run_id
          AND rf."LineTypeID" = v_form8886_lt
    ) f8886
      ON ao."QuicklinkID" = f8886."Form8886ID"
     AND ao."LineID" = f8886."LineID"
    WHERE ao."LineTypeID" = v_form8886_lt;

    -- At Risk
    IF EXISTS (
        SELECT 1 FROM tmp_aas_aos ao
        WHERE ao."ClientID" = v_client_id AND ao."LineTypeID" = v_atrisk_lt
    ) THEN
        INSERT INTO "AtRiskAllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "SourceEntityID", "AtRiskID", "LineID", "Amount", "FlowupAmount",
            "OriginalParentEntityID", "TrackingKey", "ParentEntityId"
        )
        SELECT DISTINCT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
               ao."EntityID", ao."QuicklinkID", ao."LineID", ao."Amount", ao."Amount",
               ao."OriginalParentEntityID",
               "udfLbTrackingKey"(v_is_tracking_key, ao."TrackingKey", ao."EntityID", v_entity_id),
               ao."ParentEntityId"
        FROM tmp_aas_aos ao
        WHERE ao."LineTypeID" = v_atrisk_lt;
    END IF;

    -- GAAP To Tax
    INSERT INTO "GAAPToTaxAllocation" (
        "RunID", "ClientID", "TaxPeriodID", "EntityID", "PartnerNumber", "ShareClass",
        "K1LineID", "GaapToTaxLineID", "CategoryID", "Amount"
    )
    SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."PartnerNumber", ao."ShareClass",
           ao."LineID", ao."QuicklinkID", ao."CategoryID", ao."Amount"
    FROM tmp_aas_aos ao
    WHERE ao."EntityID" = v_entity_id
      AND ao."LineTypeID" = v_gaap_lt;

    -- Book K-1 Adjustments
    IF v_allow_pe_book THEN
        INSERT INTO "AdjustmentAllocationSummary" (
            "RunID", "ClientID", "TaxPeriodID", "EntityID", "ShareClass", "PartnerNumber",
            "LineID", "LineTypeID", "Amount", "FlowupAmount", "AdjustmentTypeID"
        )
        SELECT v_run_id, v_client_id, v_tax_period_id, v_entity_id, ao."ShareClass", ao."PartnerNumber",
               ao."LineID", ao."LineTypeID", ao."Amount", ao."Amount", ao."AdjustmentTypeID"
        FROM tmp_aas_aos ao
        WHERE ao."LineTypeID" = v_book_k1_adj_lt;
    END IF;

    DROP TABLE IF EXISTS tmp_aas_k1_workflow, tmp_aas_lower_tier_funds, tmp_aas_ao_periodic,
        tmp_aas_aos, tmp_aas_partner_pfic, tmp_aas_reclass_pfic, tmp_aas_pfic_text,
        tmp_aas_pfic_aos, tmp_aas_pfic_flowup_tk, tmp_aas_cf_line_types,
        tmp_aas_cf_line_item, tmp_aas_cf_ids, tmp_aas_alloc_pct;

    IF p_write_log THEN
        CALL "uspUpdateAllocationLog"(v_log_id, clock_timestamp()::timestamp);
    END IF;

    p_status := 'SUCCESS';
END;
$proc$;
