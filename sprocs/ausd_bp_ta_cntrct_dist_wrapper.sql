-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh

-- IMPORTANT: Replace `project_id.dataset_id` with your actual Google Cloud Project ID and BigQuery Dataset ID.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.ausd_bp_ta_cntrct_dist_wrapper`(
    IN p_stichtag STRING,           -- Cutoff date in DDMMYYYY format (e.g., '31122023')
    IN p_wiederanlaufWert INT64     -- Restart value (e.g., 0)
)
BEGIN
    -- This stored procedure acts as the wrapper for the BERT base product contract distribution.
    -- It handles parameter parsing, defaulting, validation, logging, and orchestrates
    -- the call to the core kernel logic.

    DECLARE v_job_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stichtag_str STRING;
    DECLARE v_stichtag_dt DATE;
    DECLARE v_wiederanlaufwert_int INT64;
    DECLARE v_sysdate_dt DATE;
    DECLARE v_sysdate_str STRING;
    DECLARE v_parameters JSON;

    -- Initialize job parameters
    SET v_job_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_parameters = TO_JSON(STRUCT(p_stichtag AS p_stichtag_raw, p_wiederanlaufWert AS p_wiederanlaufWert_raw));

    -- Determine current system date for defaulting
    SET v_sysdate_dt = CURRENT_DATE();
    SET v_sysdate_str = FORMAT_DATE('%d%m%Y', v_sysdate_dt);

    -- Log job start into control and log tables
    INSERT INTO `project_id.dataset_id.job_control` (job_id, job_name, start_time, status, parameters)
    VALUES (v_job_id, 'ausd_bp_ta_cntrct_dist_wrapper', v_start_time, v_status, v_parameters);

    INSERT INTO `project_id.dataset_id.job_log` (log_id, job_id, log_level, message, step, details)
    VALUES (GENERATE_UUID(), v_job_id, 'INFO', 'Job execution started', 'Initialization', v_parameters);

    BEGIN
        -- Parameter Handling and Validation
        -- 1. `p_wiederanlaufWert`: Defaults to 0 if not provided (NULL).
        SET v_wiederanlaufwert_int = IFNULL(p_wiederanlaufWert, 0);

        -- 2. `p_stichtag`: Defaults to current system date (DDMMYYYY) if not provided (NULL) or empty string.
        SET v_stichtag_str = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate_str);

        -- 3. Validate if `Stichtag` is set after defaulting. It must not be NULL or empty.
        IF v_stichtag_str IS NULL OR TRIM(v_stichtag_str) = '' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Stichtag parameter is not set and could not be defaulted to system date.';
        END IF;

        -- 4. Attempt to convert `Stichtag` string to DATE type to validate its format.
        BEGIN
            SET v_stichtag_dt = PARSE_DATE('%d%m%Y', v_stichtag_str);
        EXCEPTION WHEN ERROR THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("ERROR: Invalid Stichtag format. Expected 'DDMMYYYY', got '%s'.", v_stichtag_str);
        END;

        -- Log processed parameters
        INSERT INTO `project_id.dataset_id.job_log` (log_id, job_id, log_level, message, step, details)
        VALUES (GENERATE_UUID(), v_job_id, 'INFO', 'Parameters processed and validated', 'Parameter Handling',
                TO_JSON(STRUCT(v_stichtag_str AS stichtag_ddmmyyyy, v_stichtag_dt AS stichtag_date, v_wiederanlaufwert_int AS wiederanlaufwert)));

        -- Call the kernel stored procedure for core logic
        -- NOTE: The procedure `ausd_bp_ta_cntrct_dist_kernel` needs to be created
        --       separately, encapsulating the migrated logic from `k_ausd_bp_ta_cntrct_dist.ksh`.
        CALL `project_id.dataset_id.ausd_bp_ta_cntrct_dist_kernel`(v_stichtag_dt, v_wiederanlaufwert_int);

        -- If the kernel call completes successfully
        SET v_status = 'SUCCESS';
        SET v_end_time = CURRENT_TIMESTAMP();

        INSERT INTO `project_id.dataset_id.job_log` (log_id, job_id, log_level, message, step)
        VALUES (GENERATE_UUID(), v_job_id, 'INFO', 'Job completed successfully', 'Finalization');

    EXCEPTION WHEN ERROR THEN
        -- Error handling block
        SET v_status = 'FAILED';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;

        -- Log detailed error information
        INSERT INTO `project_id.dataset_id.job_error_log` (error_id, job_id, error_code, error_message, stack_trace)
        VALUES (GENERATE_UUID(), v_job_id, 'BIGQUERY_PROC_ERROR', v_error_message, @@error.stack_trace);

        INSERT INTO `project_id.dataset_id.job_log` (log_id, job_id, log_level, message, step, details)
        VALUES (GENERATE_UUID(), v_job_id, 'ERROR', 'Job failed during execution', 'Error Handling',
                TO_JSON(STRUCT(v_error_message AS error_message, @@error.stack_trace AS stack_trace)));

    END;

    -- Update job control table with final status and timestamps
    UPDATE `project_id.dataset_id.job_control`
    SET
        end_time = v_end_time,
        status = v_status,
        error_message = v_error_message
    WHERE job_id = v_job_id;

END;