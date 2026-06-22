--
-- BigQuery Stored Procedure for orchestrating the initial provisioning process.
-- This procedure replaces the KornShell script
-- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh.
-- It handles parameter parsing, validation, default values, job auditing,
-- and invokes the core data transformation logic.
--
CREATE OR REPLACE PROCEDURE `project.dataset.sp_r_ausd_bp_ta_bpr_evn`(
    IN p_stichtag_in STRING,
    IN p_wiederanlaufWert_in INT64
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_job_name STRING DEFAULT 'sp_r_ausd_bp_ta_bpr_evn';
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_stichtag_date DATE;
    DECLARE v_error_code INT64;
    DECLARE v_error_arg STRING;
    DECLARE v_message STRING;
    DECLARE v_status STRING;

    -- Generate a unique job ID for auditing
    SET v_job_id = GENERATE_UUID();

    -- Default p_wiederanlaufWert_in to 0 if not provided
    IF p_wiederanlaufWert_in IS NULL THEN
        SET v_wiederanlaufWert = 0;
    ELSE
        SET v_wiederanlaufWert = p_wiederanlaufWert_in;
    END IF;

    -- Default p_stichtag_in to current system date (DDMMYYYY) if not provided
    IF p_stichtag_in IS NULL OR p_stichtag_in = '' THEN
        SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    ELSE
        SET v_stichtag = p_stichtag_in;
    END IF;

    -- Convert v_stichtag string (DDMMYYYY) to a DATE type for validation and processing
    BEGIN
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', v_stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_error_code = 193; -- Corresponds to original script's error for Stichtag not set/invalid
        SET v_error_arg = 'Invalid Stichtag format. Expected DDMMYYYY.';
        SET v_message = 'Failed to parse Stichtag. Check format.';
        SET v_status = 'FAILED';

        INSERT INTO `project.dataset.job_audit` (
            job_id, job_name, status, stichtag_param, restart_value_param,
            stichtag_processed, restart_value_processed, error_code, error_arg, message, created_at, updated_at
        ) VALUES (
            v_job_id, v_job_name, v_status, p_stichtag_in, p_wiederanlaufWert_in,
            NULL, v_wiederanlaufWert, v_error_code, v_error_arg, v_message, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
        RAISE; -- Re-raise the error
    END;

    -- Parameter Validation: Ensure Stichtag is set and valid
    IF v_stichtag_date IS NULL THEN
        SET v_error_code = 193;
        SET v_error_arg = 'Stichtag is missing or invalid after defaulting.';
        SET v_message = 'Parameter "Stichtag" (Stichtag) is not set or invalid. Job cannot proceed.';
        SET v_status = 'FAILED';

        INSERT INTO `project.dataset.job_audit` (
            job_id, job_name, status, stichtag_param, restart_value_param,
            stichtag_processed, restart_value_processed, error_code, error_arg, message, created_at, updated_at
        ) VALUES (
            v_job_id, v_job_name, v_status, p_stichtag_in, p_wiederanlaufWert_in,
            NULL, v_wiederanlaufWert, v_error_code, v_error_arg, v_message, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
        RAISE; -- Re-raise the error
    END IF;

    -- Log job start and parameters
    INSERT INTO `project.dataset.job_audit` (
        job_id, job_name, status, stichtag_param, restart_value_param,
        stichtag_processed, restart_value_processed, message, created_at, updated_at
    ) VALUES (
        v_job_id, v_job_name, 'RUNNING', p_stichtag_in, p_wiederanlaufWert_in,
        v_stichtag_date, v_wiederanlaufWert, 'Job started with parameters.', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

    BEGIN
        -- Call the core data transformation procedure
        CALL `project.dataset.sp_k_ausd_bp_ta_bpr_evn`(v_stichtag, v_wiederanlaufWert);

        -- If successful, update job audit status
        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully.';

        UPDATE `project.dataset.job_audit`
        SET
            status = v_status,
            message = v_message,
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = v_job_id;

    EXCEPTION WHEN ERROR THEN
        -- Catch any errors from the core procedure or other statements
        SET v_error_code = @@error.code;
        SET v_error_arg = @@error.message;
        SET v_message = 'Job failed due to an error during core processing.';
        SET v_status = 'FAILED';

        UPDATE `project.dataset.job_audit`
        SET
            status = v_status,
            error_code = v_error_code,
            error_arg = v_error_arg,
            message = v_message,
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = v_job_id;

        RAISE; -- Re-raise the original error for external systems to catch
    END;

END;