--
-- BigQuery Stored Procedure for wrapper logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
--
-- This procedure orchestrates the reconciliation process, handling parameters,
-- logging, error trapping, and invoking the core reconciliation logic.
--

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.vertragsdatenabgleich`(
    IN p_stichtag STRING,     -- Corresponds to shell script's -s parameter (e.g., YYYYMMDD)
    IN p_log_level STRING,    -- Corresponds to shell script's -l parameter
    OUT p_status STRING       -- Output status: 'SUCCESS' or 'FAILED'
)
BEGIN
    -- Declare variables
    DECLARE v_job_id STRING;
    DECLARE v_entry_number INT64 DEFAULT 0;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_log_file_name STRING;
    DECLARE v_parameters JSON;
    DECLARE v_error_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_stichtag_info STRING;
    DECLARE v_run_status STRING DEFAULT 'RUNNING';

    -- Initialize parameters and job metadata
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_job_id = GENERATE_UUID(); -- Or use a more deterministic job ID if available from Airflow
    SET v_parameters = TO_JSON(STRUCT(p_stichtag, p_log_level));
    SET v_log_file_name = FORMAT('r_ausd_v_ta_p_discount_rr_%s.log', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_start_time));
    SET v_stichtag_info = FORMAT('Processing data for Stichtag: %s', p_stichtag);

    -- Log job start
    SET v_entry_number = v_entry_number + 1;
    INSERT INTO `my_gcp_project.my_bq_dataset.job_audit_log`
    (job_id, entry_number, log_file_name, start_timestamp, status, parameters, message, stichtag_info)
    VALUES
    (v_job_id, v_entry_number, v_log_file_name, v_start_time, v_run_status, v_parameters, 'Job started.', v_stichtag_info);

    -- Parameter validation (simplified example)
    IF p_stichtag IS NULL OR LENGTH(p_stichtag) != 8 THEN
        SET v_error_code = '192'; -- Custom error code for parameter validation failure
        SET v_error_message = 'Invalid or missing p_stichtag parameter. Expected YYYYMMDD.';
        RAISE SCRIPT EXCEPTION IF TRUE; -- Force error handling path
    END IF;

    -- Main processing block with error handling
    BEGIN
        -- Log before calling core logic
        SET v_entry_number = v_entry_number + 1;
        INSERT INTO `my_gcp_project.my_bq_dataset.job_audit_log`
        (job_id, entry_number, log_file_name, status, message)
        VALUES
        (v_job_id, v_entry_number, v_log_file_name, v_run_status, 'Calling core reconciliation logic.');

        -- CALL THE CORE RECONCILIATION STORED PROCEDURE
        -- The actual name and parameters of this SP will depend on its migration.
        -- For now, this is a placeholder.
        CALL `my_gcp_project.my_bq_dataset.core_discount_rr_process`(p_stichtag, p_log_level);

        SET v_run_status = 'SUCCESS';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET p_status = 'SUCCESS';

        -- Log job success
        SET v_entry_number = v_entry_number + 1;
        INSERT INTO `my_gcp_project.my_bq_dataset.job_audit_log`
        (job_id, entry_number, log_file_name, start_timestamp, end_timestamp, status, message, stichtag_info)
        VALUES
        (v_job_id, v_entry_number, v_log_file_name, v_start_time, v_end_time, v_run_status, 'Job completed successfully.', v_stichtag_info);

    EXCEPTION WHEN ERROR THEN
        SET v_run_status = 'FAILED';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;
        IF v_error_code IS NULL THEN
            SET v_error_code = '193'; -- Default error code for unexpected errors
        END IF;
        SET p_status = 'FAILED';

        -- Log job failure
        SET v_entry_number = v_entry_number + 1;
        INSERT INTO `my_gcp_project.my_bq_dataset.job_audit_log`
        (job_id, entry_number, log_file_name, start_timestamp, end_timestamp, status, error_code, error_message, message, stichtag_info)
        VALUES
        (v_job_id, v_entry_number, v_log_file_name, v_start_time, v_end_time, v_run_status, v_error_code, v_error_message, 'Job failed.', v_stichtag_info);

        -- Re-raise the error to signal failure to the caller (e.g., Airflow)
        RAISE;
    END;
END;