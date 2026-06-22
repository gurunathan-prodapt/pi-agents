--
-- BigQuery Stored Procedure: your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref
-- Migrates content from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
--
CREATE OR REPLACE PROCEDURE your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref(
    OUT p_record_count INT64
)
BEGIN
    -- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql
    --
    -- This procedure migrates the core business logic from the original Oracle SQL script.
    -- Oracle-specific syntax has been converted to BigQuery SQL where possible.
    -- Manual review for exact data type mappings, function equivalents, and table
    -- availability in BigQuery is required, as the original Oracle table schemas
    -- (e.g., dwtk_meldungen, cds$ta_bp_ref, sof$ta_bp_ref) are not fully known.

    DECLARE v_datum DATE;
    DECLARE records_inserted INT64;

    -- Determine 'v_datum' from dwtk_meldungen table, similar to Oracle's 's_datum' variable
    -- Assumption: isbert_schema.dwtk_meldungen is migrated to your_gcp_project.your_bq_dataset.dwtk_meldungen
    -- NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    SET v_datum = COALESCE(
        (SELECT MAX(DATE(m.timecreated)) FROM your_gcp_project.your_bq_dataset.dwtk_meldungen AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'),
        PARSE_DATE('%Y%m%d', '19000101')
    );

    -- Original script had 'prompt tracing und settings', 'START ../trace.sql.cfg', 'SPOOL', 'WHENEVER SQLERROR', 'SET TIMING ON', 'SET SERVEROUTPUT ON'.
    -- These are SQL*Plus client commands and are replaced by BigQuery's native logging and error handling.

    -- Original script: 'tabelle von vorherigem lauf loeschen'
    -- begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bp_ref'); end; /
    -- Assumption: sof$ta_bp_ref is migrated to your_gcp_project.your_bq_dataset.sof_ta_bp_ref
    -- TRUNCATE TABLE is a DDL statement, cannot be directly run within a BEGIN...END block in some SQL dialects.
    -- BigQuery supports TRUNCATE in a procedure.
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.sof_ta_bp_ref;

    -- Original script: 'zieltabelle anlegen: lokale kopie der carmen-bp_ref-tabelle'
    -- INSERT INTO sof$ta_bp_ref(...) SELECT ... FROM cds$ta_bp_ref &v_carmen br ...
    -- Assumption: cds$ta_bp_ref is migrated to your_gcp_project.your_bq_dataset.cds_ta_bp_ref
    -- The '&v_carmen' (DB-Link) is removed as BigQuery handles cross-dataset/project access directly.
    INSERT INTO your_gcp_project.your_bq_dataset.sof_ta_bp_ref (
        cntrct_cp2_id,
        bp_id
    )
    SELECT
        br.cntrct_cp2_id,
        br.bp_id
    FROM
        your_gcp_project.your_bq_dataset.cds_ta_bp_ref AS br
    WHERE
        -- Original: br.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
        DATE(br.insert_at) <= v_datum
    AND
        (   br.modified_at IS NULL
         OR DATE(br.modified_at) > v_datum     )
    AND
        DATE(br.valid_from) <= v_datum
    AND
        (   br.valid_to IS NULL
         OR DATE(br.valid_to) > v_datum       )
    AND     br.is_production = 1
    AND     br.bp_ref_ty = 4;

    SET p_record_count = @@row_count; -- Get the number of rows inserted by the last DML statement.

    -- Original script had 'commit;'. BigQuery DML operations are typically auto-committed.
    -- Explicit transactions might be needed for complex multi-statement logic if atomicity is critical.

    -- Original: 'prompt Verarbeitung fehlerfrei beendet.'
    -- Original: 'spool off'
    -- These are SQL*Plus specific and handled by BigQuery's execution environment.

END;