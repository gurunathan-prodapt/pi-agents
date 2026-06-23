-- Target: BigQuery Stored Procedure
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- Description: BigQuery Stored Procedure migrating the orchestration logic of the KornShell script.
-- It handles parameter validation, date derivation, and invokes the main data processing logic.

CREATE OR REPLACE PROCEDURE `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Expected format: DDMMYYYY
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_Stichtag_DATE DATE;
    DECLARE v_heute DATE;
    DECLARE v_gestern DATE;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_job_status STRING DEFAULT 'SUCCESS';
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID(); -- Unique ID for this specific run

    -- Local variable for p_wiederanlaufWert to handle potential NULL input as 0 (mimicking shell default)
    DECLARE v_local_wiederanlaufWert INT64 DEFAULT 0;
    IF p_wiederanlaufWert IS NOT NULL THEN
        SET v_local_wiederanlaufWert = p_wiederanlaufWert;
    END IF;

    -- Parameter Validation
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_JobKennung must be provided and not empty.';
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'Parameter Validation');
        RAISE USING MESSAGE = v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_EintragsNr must be provided and not empty.';
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'Parameter Validation');
        RAISE USING MESSAGE = v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
        SET v_error_message = 'ERROR: Parameter p_Stichtag must be provided and not empty.';
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'Parameter Validation');
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Date Validation for p_Stichtag (expected DDMMYYYY)
    IF NOT REGEXP_CONTAINS(p_Stichtag, r'^[0-9]{8}$') THEN
        SET v_error_message = 'ERROR: Parameter p_Stichtag has an invalid format. Expected DDMMYYYY.';
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'Date Validation');
        RAISE USING MESSAGE = v_error_message;
    END IF;

    SET v_Stichtag_DATE = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_Stichtag_DATE IS NULL THEN
        SET v_error_message = 'ERROR: Parameter p_Stichtag could not be parsed as a valid date (DDMMYYYY).';
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'Date Validation');
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Date Derivation (replacing gestern.ksh)
    SET v_heute = CURRENT_DATE();
    SET v_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Main SQL Logic Execution (content from 'd_ausd_bp_ta_bpr_basis_his.bq.sql')
    BEGIN
        -- The core data processing logic from 'd_ausd_bp_ta_bpr_basis_his.bq.sql' would be embedded or called here.
        -- For demonstration, we'll use a MERGE statement placeholder.
        -- Parameters for the SQL logic would typically be passed as query parameters or variables.
        -- In this example, we're using variables within the procedure.

        -- Placeholder for actual data processing DML.
        -- Example of how the SQL might be directly embedded:
        MERGE `project.dataset.PoolBasisprodukt` AS T
        USING (
            SELECT
                src.product_key,
                src.product_name,
                src.start_date,
                src.end_date,
                src.status,
                v_Stichtag_DATE AS processing_date -- Using derived date
            FROM
                `project.dataset.source_pool_basisprodukt_data` AS src -- Placeholder for actual source table
            WHERE
                CAST(FORMAT_DATE('%Y%m%d', src.record_date) AS STRING) = p_Stichtag
                AND (v_local_wiederanlaufWert = 0 OR src.version >= v_local_wiederanlaufWert)
        ) AS S
        ON T.product_key = S.product_key AND T.processing_date = S.processing_date
        WHEN MATCHED THEN
            UPDATE SET
                T.product_name = S.product_name,
                T.start_date = S.start_date,
                T.end_date = S.end_date,
                T.status = S.status,
                T.last_updated_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (product_key, product_name, start_date, end_date, status, processing_date, last_updated_timestamp)
            VALUES (S.product_key, S.product_name, S.start_date, S.end_date, S.status, S.processing_date, CURRENT_TIMESTAMP());

        -- After the MERGE/INSERT, capture the number of processed records.
        -- This count would typically be more precise, reflecting rows inserted/updated by the DML.
        -- For this placeholder, we simulate a count based on the merge operation.
        SET v_records_processed = @@row_count; -- Captures rows affected by the last DML statement

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = FORMAT("SQL Execution Error: %s", @@error.message);
        INSERT INTO `project.dataset.error_log` (job_id, run_id, timestamp, error_message, component, stack_trace)
        VALUES ('k_ausd_bp_ta_bpr_basis_his', v_run_id, CURRENT_TIMESTAMP(), v_error_message, 'SQL Execution', @@error.stack_trace);
        SET v_job_status = 'FAILED';
        RAISE USING MESSAGE = v_error_message;
    END;

    -- Job Audit Logging
    INSERT INTO `project.dataset.job_audit` (job_id, run_id, start_timestamp, end_timestamp, status, input_params, records_processed, notes)
    VALUES (
        'k_ausd_bp_ta_bpr_basis_his',
        v_run_id,
        v_start_time,
        CURRENT_TIMESTAMP(),
        v_job_status,
        TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, v_local_wiederanlaufWert AS wiederanlaufWert)),
        v_records_processed,
        'Main data processing completed for Stichtag: ' || p_Stichtag || '. Records processed: ' || CAST(v_records_processed AS STRING)
    );

END;