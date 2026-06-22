-- BigQuery Stored Procedure: sp_ausd_bp_ta_iccid_einzeln_wrapper
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- This procedure orchestrates the initial provisioning of selected basic products for BERT,
-- handling parameter parsing, date determination, validation, and logging,
-- then invoking the core kernel logic.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`(
    IN p_stichtag STRING,           -- Optional: Snapshot date in DDMMYYYY format
    IN p_wiederanlaufWert STRING    -- Optional: Restart value, defaults to '0'
)
BEGIN
    -- Declare variables
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_iccid_einzeln.ksh';
    DECLARE v_log_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stichtag_final STRING;
    DECLARE v_wiederanlaufwert_final STRING;
    DECLARE v_system_date_ddmmyyyy STRING;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_log_id = GENERATE_UUID();
    SET v_status = 'RUNNING';

    -- Initial log entry for job start
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert)
    VALUES (v_log_id, v_job_name, v_start_time, 'INFO', 'Job started.', p_stichtag, p_wiederanlaufWert);

    -- Update job status to RUNNING
    MERGE INTO `your_gcp_project.your_bq_dataset.job_status` AS target
    USING (SELECT v_job_name AS job_name) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET last_run_timestamp = v_start_time, status = v_status, last_error_message = NULL, last_stichtag = p_stichtag, last_wiederanlaufwert = p_wiederanlaufWert
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_run_timestamp, status, last_stichtag, last_wiederanlaufwert)
        VALUES (v_job_name, v_start_time, v_status, p_stichtag, p_wiederanlaufWert);

    BEGIN -- Start of main logic block for error handling
        -- Determine system date (DDMMYYYY)
        SET v_system_date_ddmmyyyy = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

        -- Default p_wiederanlaufWert: if NULL or empty, set to '0'
        SET v_wiederanlaufwert_final = COALESCE(NULLIF(p_wiederanlaufWert, ''), '0');

        -- Default p_stichtag: if NULL or empty, set to system date
        SET v_stichtag_final = COALESCE(NULLIF(p_stichtag, ''), v_system_date_ddmmyyyy);

        -- Log determined parameters
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert)
        VALUES (GENERATE_UUID(), v_job_name, CURRENT_TIMESTAMP(), 'INFO',
                FORMAT('Parameters determined: Stichtag = %s, Wiederanlaufwert = %s', v_stichtag_final, v_wiederanlaufwert_final),
                v_stichtag_final, v_wiederanlaufwert_final);

        -- Parameter Validation: Stichtag must not be empty after defaulting
        IF v_stichtag_final IS NULL OR v_stichtag_final = '' THEN
            SET v_error_message = 'ERROR: Stichtag parameter is missing after defaulting. Cannot proceed.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- Log validation success
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert)
        VALUES (GENERATE_UUID(), v_job_name, CURRENT_TIMESTAMP(), 'INFO', 'Parameter validation successful.', v_stichtag_final, v_wiederanlaufwert_final);

        -- Call the kernel procedure for actual data processing
        -- NOTE: The kernel procedure `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`
        -- must be created separately as per the design document.
        CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`(
            v_stichtag_final,
            v_wiederanlaufwert_final
        );

        SET v_status = 'SUCCEEDED';
        SET v_end_time = CURRENT_TIMESTAMP();

        -- Log job completion
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert)
        VALUES (GENERATE_UUID(), v_job_name, v_end_time, 'INFO', 'Job completed successfully.', v_stichtag_final, v_wiederanlaufwert_final);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;

        -- Log error
        INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert, error_details)
        VALUES (GENERATE_UUID(), v_job_name, v_end_time, 'ERROR', 'Job failed.', v_stichtag_final, v_wiederanlaufwert_final, v_error_message);

        -- Re-raise the error to propagate it to the caller (e.g., Cloud Composer)
        RAISE USING MESSAGE v_error_message;

    FINALLY
        -- Always update job status, regardless of success or failure
        MERGE INTO `your_gcp_project.your_bq_dataset.job_status` AS target
        USING (SELECT v_job_name AS job_name) AS source
        ON target.job_name = source.job_name
        WHEN MATCHED THEN
            UPDATE SET last_run_timestamp = v_end_time, status = v_status, last_error_message = v_error_message, last_stichtag = v_stichtag_final, last_wiederanlaufwert = v_wiederanlaufwert_final
        WHEN NOT MATCHED THEN
            INSERT (job_name, last_run_timestamp, status, last_error_message, last_stichtag, last_wiederanlaufwert)
            VALUES (v_job_name, v_end_time, v_status, v_error_message, v_stichtag_final, v_wiederanlaufwert_final);
    END;

END;