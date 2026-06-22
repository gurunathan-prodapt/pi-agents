-- BigQuery Stored Procedure: sp_r_ausd_bp_ta_bcp_msisdn
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN
-- Purpose: Main orchestration procedure, handles initial parameter parsing, defaulting, and calls sp_k_ausd_bp_ta_bcp_msisdn.

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.sp_r_ausd_bp_ta_bcp_msisdn`(
    p_stichtag_str_in STRING,         -- Optional input for Stichtag (expected YYYYMMDD format)
    p_wiederanlaufWert_in INT64       -- Optional input for Wiederanlaufwert
)
BEGIN
    -- Declare variables for job metadata and parameters
    DECLARE v_job_name STRING DEFAULT 'DW.BERT_AUSD_BP_TA_BCP_MSISDN';
    DECLARE v_run_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_details STRING;

    DECLARE v_stichtag_actual_str STRING;
    DECLARE v_wiederanlaufWert_actual INT64;
    DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bcp_msisdnT'; -- Hardcoded from shell script's typeset -u JobKennung
    DECLARE v_eintrags_nr STRING; -- Corresponds to DW_EintragsNr, a unique identifier for this run

    -- Initialize run_id and start_time
    SET v_run_id = GENERATE_UUID();
    -- Using a part of the UUID for v_eintrags_nr to simulate DW_EintragsNr uniqueness
    SET v_eintrags_nr = SUBSTR(REPLACE(v_run_id, '-', ''), 1, 8);
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = 'Job execution started.';

    -- Determine actual p_wiederanlaufWert: use input if provided, otherwise default to 0
    SET v_wiederanlaufWert_actual = COALESCE(p_wiederanlaufWert_in, 0);

    -- Determine actual p_stichtag: if not provided, use current system date formatted as YYYYMMDD
    -- This replaces the shell script's DWDate_Gib_Zeitraum and conditional assignment logic.
    IF p_stichtag_str_in IS NULL OR p_stichtag_str_in = '' THEN
        SET v_stichtag_actual_str = FORMAT_DATE('%Y%m%d', CURRENT_DATE());
    ELSE
        -- Validate date format for provided stichtag
        BEGIN
            SELECT PARSE_DATE('%Y%m%d', p_stichtag_str_in); -- Just to validate the format
            SET v_stichtag_actual_str = p_stichtag_str_in;
        EXCEPTION WHEN ERROR THEN
            RAISE USING MESSAGE = FORMAT('Invalid date format for input p_stichtag_str: %s. Expected YYYYMMDD. Error: %s', p_stichtag_str_in, @@error.message);
        END;
    END IF;

    -- Log job start details into the audit table
    INSERT INTO `your_project.your_dataset.job_audit_log`
    (job_name, run_id, start_time, status, message, stichtag, wiederanlaufwert)
    VALUES (v_job_name, v_run_id, v_start_time, v_status, v_message, PARSE_DATE('%Y%m%d', v_stichtag_actual_str), v_wiederanlaufWert_actual);

    BEGIN
        -- Call the next BigQuery Stored Procedure in the chain, passing all necessary parameters.
        -- This replaces the shell script's invocation of k_ausd_bp_ta_bcp_msisdn.ksh.
        CALL `your_project.your_dataset.sp_k_ausd_bp_ta_bcp_msisdn`(
            v_job_kennung,
            v_eintrags_nr,
            v_stichtag_actual_str,
            v_wiederanlaufWert_actual
        );

        -- If the call succeeds, update the audit log status to SUCCESS
        SET v_status = 'SUCCESS';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_message = 'Job completed successfully.';

        UPDATE `your_project.your_dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE
            run_id = v_run_id;

    EXCEPTION WHEN ERROR THEN
        -- If an error occurs during any step, catch it and update the audit log with failure details
        SET v_status = 'FAILED';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_details = @@error.message;
        SET v_message = CONCAT('Job failed with error: ', v_error_details);

        UPDATE `your_project.your_dataset.job_audit_log`
        SET
            end_time = v_end_time,
            status = v_status,
            error_details = v_error_details,
            message = v_message
        WHERE
            run_id = v_run_id;

        RAISE; -- Re-raise the error for Airflow to catch and manage retries/notifications
    END;
END;