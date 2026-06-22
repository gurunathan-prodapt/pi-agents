-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_r_ausd_bp_ta_bcp_msisdn`(
    IN p_stichtag STRING,
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_sysdate_ddmmyyyy STRING;
    DECLARE v_job_status STRING DEFAULT 'RUNNING';
    DECLARE v_error_message STRING;
    DECLARE v_stichtag_actual STRING;
    DECLARE v_wiederanlaufWert_actual INT64;

    -- Generate a unique job run ID
    SET v_job_run_id = GENERATE_UUID();

    -- Initialize actual parameters with input values
    SET v_stichtag_actual = p_stichtag;
    SET v_wiederanlaufWert_actual = p_wiederanlaufWert;

    -- Log job start
    INSERT INTO `project.dataset.job_control` (job_run_id, job_name, start_time, status, stichtag_param, wiederanlauf_wert_param)
    VALUES (v_job_run_id, 'r_ausd_bp_ta_bcp_msisdn', CURRENT_TIMESTAMP(), v_job_status, v_stichtag_actual, v_wiederanlaufWert_actual);

    INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
    VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'Job started.', 'Initialization');

    -- Date Determination: Get current date in DDMMYYYY format
    SET v_sysdate_ddmmyyyy = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Parameter Defaulting
    IF v_wiederanlaufWert_actual IS NULL THEN
        SET v_wiederanlaufWert_actual = 0;
        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'p_wiederanlaufWert not provided, defaulting to 0.', 'Parameter Defaulting');
    END IF;

    IF v_stichtag_actual IS NULL OR TRIM(v_stichtag_actual) = '' THEN
        SET v_stichtag_actual = v_sysdate_ddmmyyyy;
        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'p_stichtag not provided, defaulting to current system date: ' || v_stichtag_actual, 'Parameter Defaulting');
    END IF;

    -- Basic parameter validation (DDMMYYYY format for stichtag)
    IF NOT REGEXP_CONTAINS(v_stichtag_actual, r'^\d{8}$') THEN
        SET v_error_message = 'Parameter Validation Error: Invalid p_stichtag format. Expected DDMMYYYY.';
        INSERT INTO `project.dataset.job_error_log` (error_id, job_run_id, error_time, error_type, error_message, source_file)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'PARAMETER_VALIDATION', v_error_message, 'sp_r_ausd_bp_ta_bcp_msisdn');
        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'ERROR', v_error_message, 'Parameter Validation');
        
        -- Update job control with failure
        UPDATE `project.dataset.job_control`
        SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', error_message = v_error_message
        WHERE job_run_id = v_job_run_id;
        
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- BEGIN EXCEPTION block for kernel call and subsequent logic
    BEGIN
        -- Kernel Script Invocation
        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'Calling kernel stored procedure sp_ausd_bp_ta_bcp_msisdn_kernel with stichtag: ' || v_stichtag_actual || ', wiederanlaufWert: ' || CAST(v_wiederanlaufWert_actual AS STRING) || '.', 'Kernel Call');

        CALL `project.dataset.sp_ausd_bp_ta_bcp_msisdn_kernel`(v_stichtag_actual, v_wiederanlaufWert_actual);

        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'Kernel stored procedure sp_ausd_bp_ta_bcp_msisdn_kernel completed successfully.', 'Kernel Call');

        -- Update job status to SUCCESS
        UPDATE `project.dataset.job_control`
        SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS'
        WHERE job_run_id = v_job_run_id;

        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', 'Job completed successfully.', 'Finalization');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        INSERT INTO `project.dataset.job_error_log` (error_id, job_run_id, error_time, error_type, error_message, stack_trace, source_file)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'RUNTIME_ERROR', v_error_message, @@error.stack_trace, 'sp_r_ausd_bp_ta_bcp_msisdn');
        INSERT INTO `project.dataset.job_log` (log_id, job_run_id, log_time, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_run_id, CURRENT_TIMESTAMP(), 'ERROR', 'Job failed due to runtime error: ' || v_error_message, 'Runtime Error Handling');

        -- Update job control with failure
        UPDATE `project.dataset.job_control`
        SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', error_message = v_error_message
        WHERE job_run_id = v_job_run_id;

        -- Re-raise the error to the caller (e.g., Airflow)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;
END;