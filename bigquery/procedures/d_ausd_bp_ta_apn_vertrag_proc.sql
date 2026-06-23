-- BigQuery Stored Procedure: project.dataset.d_ausd_bp_ta_apn_vertrag_proc
-- Replaces the logic from d_ausd_bp_ta_apn_vertrag.sql, orchestrated by k_ausd_bp_ta_apn_vertrag.ksh
-- NOTE: The original SQL content of d_ausd_bp_ta_apn_vertrag.sql was not provided.
-- This is a placeholder procedure. You must replace the content with the actual
-- translated SQL logic from d_ausd_bp_ta_apn_vertrag.sql.

CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_bp_ta_apn_vertrag_proc(
    p_EintragsNr STRING,
    p_JobKennung STRING,
    p_Stichtag DATE,
    p_wiederanlaufWert STRING,
    OUT processed_rows INT64
)
BEGIN
    -- DECLARE variables if needed
    -- e.g., DECLARE v_some_var STRING;

    -- Placeholder for the actual SQL logic from d_ausd_bp_ta_apn_vertrag.sql
    -- This section should contain CREATE, MERGE, INSERT, UPDATE, DELETE statements
    -- that perform the core data extraction and transformation.

    -- Example: Insert into a hypothetical target table
    -- REPLACE WITH YOUR ACTUAL LOGIC
    CREATE SCHEMA IF NOT EXISTS project.dataset;
    CREATE TABLE IF NOT EXISTS project.dataset.poolbasisprodukt (
        col1 STRING,
        col2 DATE,
        job_kennung STRING,
        eintragsnr STRING,
        stichtag DATE
    );

    INSERT INTO project.dataset.poolbasisprodukt (col1, col2, job_kennung, eintragsnr, stichtag)
    SELECT
        'example_data_' || GENERATE_UUID(),
        CURRENT_DATE(),
        p_JobKennung,
        p_EintragsNr,
        p_Stichtag
    FROM UNNEST(GENERATE_ARRAY(1, 10)) -- Simulate some rows being processed
    LIMIT 10;

    SET processed_rows = @@row_count; -- Get the count of rows affected by the last DML statement

    -- If the logic involves multiple DMLs, you might need to sum row counts or
    -- query the final target table to get the true processed_rows count.
    -- Example: SET processed_rows = (SELECT COUNT(*) FROM project.dataset.poolbasisprodukt WHERE stichtag = p_Stichtag);

EXCEPTION WHEN ERROR THEN
    INSERT INTO project.dataset.error_log (log_timestamp, job_name, error_message, severity, additional_info)
    VALUES (
        CURRENT_TIMESTAMP(),
        'd_ausd_bp_ta_apn_vertrag_proc',
        @@error.message,
        'ERROR',
        TO_JSON(STRUCT(p_EintragsNr, p_JobKennung, p_Stichtag, p_wiederanlaufWert AS wiederanlaufWert_param))
    );
    RAISE; -- Re-raise the error to propagate it to the calling procedure
END;