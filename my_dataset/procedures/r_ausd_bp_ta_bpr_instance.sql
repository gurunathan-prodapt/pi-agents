-- BigQuery Stored Procedure: r_ausd_bp_ta_bpr_instance
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This procedure orchestrates the data preparation for 'PoolBasisprodukt' instances.
-- It handles parameter parsing, validation, date derivation, and execution of the core SQL logic.
--
-- Please replace `project.dataset` with your actual GCP Project ID and BigQuery Dataset ID.
-- Also, update table names like `target_bpr_instance_table` and their column names (`processing_date_col`)
-- to match your actual BigQuery implementation.

CREATE OR REPLACE PROCEDURE `project.dataset`.r_ausd_bp_ta_bpr_instance(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING,      -- Expected format: DDMMYYYY
    p_wiederanlaufWert INT64 DEFAULT 0
)
BEGIN
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_records INT64;
    DECLARE v_error_message STRING;
    DECLARE v_stichtag_date DATE;

    -- 1. Date Derivation (replacing gestern.ksh)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- 2. Parameter Validation (replacing h_alis_parameter.ksh and h_alis_date.ksh)

    -- Check for required parameters
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'JobKennung parameter is missing or empty.';
        INSERT INTO `project.dataset.error_log` (process_name, error_nr, error_arg, created_at)
        VALUES ('r_ausd_bp_ta_bpr_instance', 1001, v_error_message, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'EintragsNr parameter is missing or empty.';
        INSERT INTO `project.dataset.error_log` (process_name, error_nr, error_arg, created_at)
        VALUES ('r_ausd_bp_ta_bpr_instance', 1002, v_error_message, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        SET v_error_message = 'Stichtag parameter is missing or empty.';
        INSERT INTO `project.dataset.error_log` (process_name, error_nr, error_arg, created_at)
        VALUES ('r_ausd_bp_ta_bpr_instance', 1003, v_error_message, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Validate Stichtag format (DDMMYYYY)
    BEGIN
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = 'Stichtag parameter has invalid format. Expected DDMMYYYY.';
        INSERT INTO `project.dataset.error_log` (process_name, error_nr, error_arg, created_at)
        VALUES ('r_ausd_bp_ta_bpr_instance', 1004, v_error_message, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE = v_error_message;
    END;

    -- 3. SQL Logic Execution (calling the migrated d_ausd_bp_ta_bpr_instance procedure)
    -- This procedure encapsulates the logic from the original d_ausd_bp_ta_bpr_instance.sql
    CALL `project.dataset.d_ausd_bp_ta_bpr_instance`(
        p_JobKennung,
        p_EintragsNr,
        p_Stichtag,        -- Raw Stichtag string
        v_stichtag_date,   -- Parsed Stichtag date
        p_wiederanlaufWert,
        v_datum_heute,
        v_datum_gestern
    );

    -- 4. Record Count (replacing temporary file read)
    -- This section needs to be updated to accurately count records inserted/updated
    -- by the `d_ausd_bp_ta_bpr_instance` procedure.
    -- Assume `d_ausd_bp_ta_bpr_instance` inserts into `project.dataset.target_bpr_instance_table`.
    -- Replace `target_bpr_instance_table` and `processing_date_col` with actual table and column names.
    SET v_records = (
        SELECT COUNT(*)
        FROM `project.dataset.target_bpr_instance_table`
        WHERE DATE(processing_date_col) = v_stichtag_date
    );
    -- If the target table's date column is not directly `stichtag_date`, adjust the WHERE clause accordingly.
    -- Alternatively, the `d_ausd_bp_ta_bpr_instance` procedure could return the record count as an OUT parameter.

    -- 5. Audit Logging
    INSERT INTO `project.dataset.job_audit` (job_kennung, eintrags_nr, stichtag, records, created_at, tab_name)
    VALUES (p_JobKennung, p_EintragsNr, p_Stichtag, v_records, CURRENT_TIMESTAMP(), 'bert_bp_ta_bpr_instance_target'); -- 'bert_bp_ta_bpr_instance_target' is a placeholder for the actual target table name

    -- 6. Optional: Job Control (if reactivated from original ksh)
    -- UNCOMMENT AND ADAPT THE FOLLOWING BLOCK IF JOB MANAGEMENT IS REACTIVATED
    /*
    INSERT INTO `project.dataset.job_control` (tab_name, status, mode, from_date, to_date, job_type, restart_flag, records, description)
    VALUES (
        'bert_bp_ta_bpr_instance_target', -- Placeholder for actual table name managed by job_control
        'COMPLETED',                      -- Or 'FAILED' if error handling leads here
        'FULL',                           -- Or 'INCREMENTAL' based on actual logic
        v_stichtag_date,
        v_stichtag_date,
        'ETL',
        CASE WHEN p_wiederanlaufWert > 0 THEN 'Y' ELSE 'N' END,
        v_records,
        'Processing for BPR instance data based on Stichtag ' || p_Stichtag
    );
    */

EXCEPTION WHEN ERROR THEN
    -- Re-raise the error to ensure the orchestration system or caller knows it failed
    RAISE;
END;