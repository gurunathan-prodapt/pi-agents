--
-- Main BigQuery Stored Procedure for orchestrating the data processing.
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
CREATE OR REPLACE PROCEDURE `bq_dataset.control_k_ausd_v_ta_cntrct_crs2`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_job_active BOOL;
    DECLARE v_records_processed INT64;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_error_message STRING;

    SET v_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Validate input parameters
    IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
        SET v_error_message = 'Parameter p_job_kennung cannot be NULL or empty.';
        CALL `bq_dataset.sp_log_error`(v_run_id, COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_message);
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Check if the job is active
    CALL `bq_dataset.sp_job_prepare`(p_job_kennung, 'CHECK_ACTIVE', v_job_active);

    IF NOT v_job_active THEN
        SET v_error_message = 'Job ' || p_job_kennung || ' is not active. Aborting.';
        CALL `bq_dataset.sp_log_error`(v_run_id, p_job_kennung, COALESCE(p_eintrags_nr, 'UNKNOWN'), v_error_message);
        RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Log job start
    INSERT INTO `bq_dataset.job_run_log`
        (run_id, job_kennung, eintrags_nr, start_time, status, records_processed, error_message)
    VALUES
        (v_run_id, p_job_kennung, p_eintrags_nr, v_start_time, 'RUNNING', NULL, NULL);

    BEGIN
        -- Execute the core transformation logic
        CALL `bq_dataset.sp_d_ausd_v_ta_cntrct_crs2`(p_job_kennung, p_eintrags_nr, v_records_processed);

        SET v_status = 'SUCCESS';
        SET v_end_time = CURRENT_TIMESTAMP();

        -- Update job log with success details
        UPDATE `bq_dataset.job_run_log`
        SET
            end_time = v_end_time,
            status = v_status,
            records_processed = v_records_processed
        WHERE run_id = v_run_id;

        SELECT 'Job ' || p_job_kennung || ' completed successfully. Processed ' || v_records_processed || ' records.' AS message;

    EXCEPTION WHEN ERROR THEN
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;
        SET v_status = 'FAILED';

        -- Update job log with error details
        UPDATE `bq_dataset.job_run_log`
        SET
            end_time = v_end_time,
            status = v_status,
            error_message = v_error_message
        WHERE run_id = v_run_id;

        RAISE USING MESSAGE = 'Job ' || p_job_kennung || ' failed: ' || v_error_message;
    END;

END;