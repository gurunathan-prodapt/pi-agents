-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- This BigQuery Stored Procedure acts as a wrapper, handling parameter parsing,
-- auditing, and invoking the core processing logic, replacing r_ausd_bp_ta_msisdn_his.ksh.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`(
    IN p_stichtag_input STRING,
    IN p_wiederanlaufWert_input INT64
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_msisdn_his';
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_details JSON;

    -- Initialize job_id and start timestamp
    SET v_job_id = GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();

    -- 1. Parameter Handling and Defaulting
    -- Default p_stichtag to current date in DDMMYYYY format if not provided
    IF p_stichtag_input IS NULL OR TRIM(p_stichtag_input) = '' THEN
        SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    ELSE
        SET v_stichtag = p_stichtag_input;
    END IF;

    -- Default p_wiederanlaufWert to 0 if not provided
    IF p_wiederanlaufWert_input IS NULL THEN
        SET v_wiederanlaufWert = 0;
    ELSE
        SET v_wiederanlaufWert = p_wiederanlaufWert_input;
    END IF;

    -- Log start of job in job_audit table
    INSERT INTO `project.dataset.job_audit` (job_id, job_name, start_timestamp, status, parameters, message)
    VALUES (
        v_job_id,
        v_job_name,
        v_start_timestamp,
        'RUNNING',
        TO_JSON(STRUCT(v_stichtag AS stichtag, v_wiederanlaufWert AS wiederanlaufwert)),
        'Job started with parsed parameters.'
    );

    -- Exception handling block for core logic
    BEGIN
        -- 2. Validate parameters (optional, core SP will also validate Stichtag format)
        IF SAFE_CAST(v_stichtag AS DATE FORMAT 'DDMMYYYY') IS NULL THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Stichtag format provided: %s. Expected DDMMYYYY.', v_stichtag);
        END IF;

        -- 3. Invoke Core Stored Procedure
        CALL `project.dataset.ausd_bp_ta_msisdn_his_core_sp`(v_job_id, v_stichtag, v_wiederanlaufWert);

        -- If core SP completes successfully
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        SET v_status = 'SUCCESS';
        SET v_message = FORMAT('Job completed successfully for Stichtag: %s, Wiederanlaufwert: %d', v_stichtag, v_wiederanlaufWert);

        -- Update job_audit with success status
        UPDATE `project.dataset.job_audit`
        SET
            end_timestamp = v_end_timestamp,
            status = v_status,
            message = v_message
        WHERE job_id = v_job_id;

    EXCEPTION WHEN ERROR THEN
        -- Handle any error that occurs during the core SP call or validation
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_error_details = TO_JSON(STRUCT(
            stack_trace = @@error.stack_trace,
            statement_text = @@error.statement_text,
            line = @@error.line
        ));
        SET v_message = FORMAT('Job failed for Stichtag: %s, Wiederanlaufwert: %d. Error: %s', v_stichtag, v_wiederanlaufWert, v_error_message);

        -- Update job_audit with failure status
        UPDATE `project.dataset.job_audit`
        SET
            end_timestamp = v_end_timestamp,
            status = v_status,
            message = v_message
        WHERE job_id = v_job_id;

        -- Log detailed error information
        INSERT INTO `project.dataset.job_error_log` (job_id, job_name, error_timestamp, error_message, error_details)
        VALUES (
            v_job_id,
            v_job_name,
            v_end_timestamp,
            v_error_message,
            v_error_details
        );

        -- Re-raise the error to propagate it to the caller (e.g., Airflow DAG)
        RAISE;
    END;
END;