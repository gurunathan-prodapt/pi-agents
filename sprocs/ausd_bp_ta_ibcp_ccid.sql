-- Target BigQuery Stored Procedure: Orchestrator
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.ausd_bp_ta_ibcp_ccid`(
    IN p_stichtag_str STRING,    -- Input snapshot date in 'DDMMYYYY' format
    IN p_wiederanlaufWert_in INT64 -- Input restart value, optional
)
OPTIONS(
  description="Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh. Orchestrates data preparation for BERT's demand scoring system."
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_ibcp_ccid';
    DECLARE v_stichtag_date DATE;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_error_stack STRING;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_run_id = GENERATE_UUID(); -- Generate a unique ID for this run

    -- Default p_wiederanlaufWert to 0 if not provided
    SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert_in, 0);

    -- Default p_stichtag to current system date if not provided or empty
    IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
        SET v_stichtag_date = CURRENT_DATE();
        SET p_stichtag_str = FORMAT_DATE('%d%m%Y', v_stichtag_date); -- Update the string representation for logging
    ELSE
        -- Attempt to parse the input stichtag string
        BEGIN
            SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag_str);
        EXCEPTION WHEN ERROR THEN
            SET v_error_message = 'Invalid p_stichtag_str format. Expected DDMMYYYY.';
            SET v_error_code = @@error.code;
            SET v_error_stack = @@error.stack_trace;
            RAISE USING MESSAGE v_error_message; -- Re-raise to be caught by outer block
        END;
    END IF;

    -- Parameter Validation: Ensure stichtag_date is not NULL after all attempts
    IF v_stichtag_date IS NULL THEN
        SET v_error_message = 'Stichtag cannot be NULL after defaulting. This indicates an internal error or unhandled parse failure.';
        SET v_error_code = 'SP-PARAM-001';
        SET v_error_stack = @@error.stack_trace;
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Log job start
    INSERT INTO `my_project.my_dataset.job_audit` (job_id, run_id, start_timestamp, status, stichtag, wiederanlauf_wert, message)
    VALUES (v_job_name, v_run_id, v_start_timestamp, 'RUNNING', v_stichtag_date, v_wiederanlaufWert, 'Job started');

    INSERT INTO `my_project.my_dataset.job_log` (run_id, log_timestamp, log_level, procedure_name, message)
    VALUES (v_run_id, CURRENT_TIMESTAMP(), 'INFO', v_job_name, FORMAT("Job %s started with Stichtag: %s, WiederanlaufWert: %d", v_run_id, p_stichtag_str, v_wiederanlaufWert));

    BEGIN
        -- Call the kernel stored procedure
        CALL `my_project.my_dataset.k_ausd_bp_ta_bcp_iccid`(v_stichtag_date, v_wiederanlaufWert);

        -- Log job success
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        SET v_status = 'OK';
        SET v_message = 'Job completed successfully';

        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = v_end_timestamp, status = v_status, message = v_message
        WHERE run_id = v_run_id;

        INSERT INTO `my_project.my_dataset.job_log` (run_id, log_timestamp, log_level, procedure_name, message)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'INFO', v_job_name, v_message);

    EXCEPTION WHEN ERROR THEN
        -- Catch and log any errors
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        SET v_status = 'ERROR';
        SET v_error_message = @@error.message;
        SET v_error_code = @@error.code;
        SET v_error_stack = @@error.stack_trace;
        SET v_message = FORMAT("Job failed: %s", v_error_message);

        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = v_end_timestamp, status = v_status, message = v_message
        WHERE run_id = v_run_id;

        INSERT INTO `my_project.my_dataset.job_log` (run_id, log_timestamp, log_level, procedure_name, message)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), 'ERROR', v_job_name, v_message);

        INSERT INTO `my_project.my_dataset.job_error_log` (run_id, error_timestamp, procedure_name, error_code, error_message, error_stack_trace, stichtag, wiederanlauf_wert)
        VALUES (v_run_id, CURRENT_TIMESTAMP(), v_job_name, v_error_code, v_error_message, v_error_stack, v_stichtag_date, v_wiederanlaufWert);

        RAISE; -- Re-raise the error to signal job failure upstream
    END;

END;