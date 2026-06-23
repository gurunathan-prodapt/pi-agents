-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery Stored Procedure for orchestrating the data processing job.
-- This procedure handles parameter validation, job control, error logging,
-- and invokes the core data processing logic.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset_id.r_ausd_vertrag`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr INT64
)
OPTIONS(
    description="Migrated orchestration logic for k_ausd_v_ta_p_discount_rr.ksh. Controls execution of discount data processing."
)
BEGIN
    -- Declare variables
    DECLARE v_job_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_vertrag';
    DECLARE v_status STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_error_message STRING;
    DECLARE v_processed_records INT64;

    -- Generate a unique run ID for this job instance
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Initialize status to STARTING
    SET v_status = 'STARTING';

    -- 1. Parameter Validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_message = 'ERROR: Required parameter p_job_kennung is missing or empty.';
        -- Log error and exit
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (log_id, job_run_id, job_name, job_kennung, eintrags_nr, error_time, error_message)
        VALUES (GENERATE_UUID(), v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), v_error_message);
        RAISE BQEXCEPTION MESSAGE v_error_message;
    END IF;

    -- 2. Job Control - Check for active jobs
    -- If an identical job (by job_kennung and eintrags_nr) is already running,
    -- the legacy script implies it should be ignored.
    BEGIN
        DECLARE active_job_count INT64;
        SELECT COUNT(1)
        INTO active_job_count
        FROM `your_gcp_project_id.your_bigquery_dataset_id.job_control`
        WHERE job_name = v_job_name
          AND job_kennung = p_job_kennung
          AND eintrags_nr = p_eintrags_nr
          AND status IN ('STARTING', 'RUNNING');

        IF active_job_count > 0 THEN
            SET v_error_message = FORMAT('Job with p_job_kennung=%s and p_eintrags_nr=%d is already active. Ignoring current request.', p_job_kennung, p_eintrags_nr);
            -- Log as ignored in job_control, but do not raise an exception to exit cleanly (as per "ignoring active jobs")
            INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_control` (job_run_id, job_name, job_kennung, eintrags_nr, status, start_time, end_time, last_updated, message)
            VALUES (v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, 'IGNORED', v_start_time, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), v_error_message);
            RETURN; -- Exit stored procedure
        END IF;
    EXCEPTION WHEN ERROR THEN
        -- Log any error during active job check
        SET v_error_message = 'Error during active job check: ' || @@error.message;
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (log_id, job_run_id, job_name, job_kennung, eintrags_nr, error_time, error_message, error_stacktrace)
        VALUES (GENERATE_UUID(), v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);
        RAISE BQEXCEPTION MESSAGE v_error_message;
    END;

    -- Insert initial job control record (STARTING)
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_control` (job_run_id, job_name, job_kennung, eintrags_nr, status, start_time, last_updated, message)
    VALUES (v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, v_status, v_start_time, CURRENT_TIMESTAMP(), 'Job started.');

    -- Update job status to RUNNING
    UPDATE `your_gcp_project_id.your_bigquery_dataset_id.job_control`
    SET status = 'RUNNING',
        last_updated = CURRENT_TIMESTAMP(),
        message = 'Job is running core processing logic.'
    WHERE job_run_id = v_job_run_id;

    -- 3. Core Data Processing Call
    BEGIN
        CALL `your_gcp_project_id.your_bigquery_dataset_id.d_ausd_v_ta_p_discount_rr`(p_job_kennung, p_eintrags_nr, v_processed_records);

        -- If the data processing call completes successfully
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'COMPLETED';

        -- Update job control to COMPLETED
        UPDATE `your_gcp_project_id.your_bigquery_dataset_id.job_control`
        SET status = v_status,
            end_time = v_end_time,
            last_updated = CURRENT_TIMESTAMP(),
            message = FORMAT('Job completed successfully. Processed %d records.', v_processed_records)
        WHERE job_run_id = v_job_run_id;

        -- Log audit information
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_audit` (audit_id, job_run_id, job_name, job_kennung, eintrags_nr, start_time, end_time, status, processed_records, message)
        VALUES (GENERATE_UUID(), v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, v_start_time, v_end_time, v_status, v_processed_records, 'Main data processing completed.');

    EXCEPTION WHEN ERROR THEN
        -- Handle errors from d_ausd_v_ta_p_discount_rr or other processing steps
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_error_message = 'Error during data processing: ' || @@error.message;

        -- Log error
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (log_id, job_run_id, job_name, job_kennung, eintrags_nr, error_time, error_message, error_stacktrace)
        VALUES (GENERATE_UUID(), v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), v_error_message, @@error.stack_trace);

        -- Update job control to FAILED
        UPDATE `your_gcp_project_id.your_bigquery_dataset_id.job_control`
        SET status = v_status,
            end_time = v_end_time,
            last_updated = CURRENT_TIMESTAMP(),
            message = v_error_message
        WHERE job_run_id = v_job_run_id;

        -- Log audit information for failure
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_audit` (audit_id, job_run_id, job_name, job_kennung, eintrags_nr, start_time, end_time, status, processed_records, message)
        VALUES (GENERATE_UUID(), v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, v_start_time, v_end_time, v_status, 0, v_error_message);

        -- Re-raise the exception to signal failure to the caller
        RAISE BQEXCEPTION MESSAGE v_error_message;
    END;

END;