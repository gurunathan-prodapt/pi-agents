-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Purpose: Main control BigQuery Stored Procedure, replacing k_ausd_v_ta_cntrct_valid.ksh.
-- Handles parameter validation, job control, and invocation of the core SQL logic.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.r_ausd_vertrag_control`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_job_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'k_ausd_v_ta_cntrct_valid_bq';
    DECLARE v_records_processed INT64;
    DECLARE v_process_id INT64; -- Simulate shell PID for active job check
    DECLARE v_start_timestamp TIMESTAMP;

    -- Generate a unique run ID for this execution
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_process_id = CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S%f', CURRENT_TIMESTAMP()) AS INT64) % 1000000; -- Simple PID emulation

    -- --- Parameter Validation (from h_alis_parameter.ksh logic) ---
    IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
        CALL `your_project_id.your_dataset.log_message`(
            'ERROR', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
            'Required parameter "Jobkennung" (-j) is missing or empty.', 193, '-j'
        );
        RAISE; -- Exit procedure
    END IF;

    IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
        CALL `your_project_id.your_dataset.log_message`(
            'ERROR', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
            'Required parameter "EintragsNr" (-f) is missing or empty.', 193, '-f'
        );
        RAISE; -- Exit procedure
    END IF;

    -- Log the start of the job
    CALL `your_project_id.your_dataset.log_message`(
        'INFO', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, 'Job execution started.'
    );

    -- --- Job Control: Deactivate older active jobs ---
    -- This section attempts to "deactivate" or mark as "FAILED" any jobs
    -- that might have been left in an 'ACTIVE' state for a prolonged period.
    -- The criteria for "older" might need to be refined (e.g., jobs older than X minutes).
    UPDATE `your_project_id.your_dataset.job_control`
    SET
        status = 'FAILED_TIMEOUT',
        end_timestamp = CURRENT_TIMESTAMP(),
        message = 'Job marked as FAILED_TIMEOUT due to being active for too long or left in active state.'
    WHERE
        job_name = v_job_name
        AND status = 'ACTIVE'
        AND start_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR); -- Example: jobs active for more than 1 hour

    -- Log the deactivation of older jobs
    CALL `your_project_id.your_dataset.log_message`(
        'INFO', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
        'Attempted to deactivate older active jobs.'
    );

    -- --- Job Control: Check for actively running jobs and ignore if active (a) ---
    -- This logic emulates "aktive Jobs werden ignoriert" from the ksh script.
    IF EXISTS (
        SELECT 1 FROM `your_project_id.your_dataset.job_control`
        WHERE job_name = v_job_name
        AND status = 'ACTIVE'
    ) THEN
        CALL `your_project_id.your_dataset.log_message`(
            'WARNING', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
            'Another instance of this job is currently active. Ignoring this execution.',
            NULL, NULL
        );
        -- Insert a record for this ignored run
        INSERT INTO `your_project_id.your_dataset.job_control` (
            job_run_id, job_name, job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, message, records_processed, process_id
        )
        VALUES (
            v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, v_start_timestamp, CURRENT_TIMESTAMP(), 'IGNORED', 'Another instance is active.', 0, v_process_id
        );
        RETURN; -- Exit procedure
    END IF;

    -- --- Job Control: Register this job as active (b) ---
    INSERT INTO `your_project_id.your_dataset.job_control` (
        job_run_id, job_name, job_kennung, eintrags_nr, start_timestamp, status, process_id
    )
    VALUES (
        v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, v_start_timestamp, 'ACTIVE', v_process_id
    );

    -- Log job registration
    CALL `your_project_id.your_dataset.log_message`(
        'INFO', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr, 'Job registered as ACTIVE.'
    );

    -- --- Execute Core SQL Script (d_ausd_v_ta_cntrct_valid_bq) ---
    BEGIN
        CALL `your_project_id.your_dataset.d_ausd_v_ta_cntrct_valid_bq`(
            p_eintrags_nr,
            p_job_kennung,
            v_records_processed
        );

        -- Update job control with successful completion
        UPDATE `your_project_id.your_dataset.job_control`
        SET
            status = 'COMPLETED',
            end_timestamp = CURRENT_TIMESTAMP(),
            records_processed = v_records_processed,
            message = FORMAT("Successfully completed. Records processed: %d", v_records_processed)
        WHERE
            job_run_id = v_job_run_id;

        CALL `your_project_id.your_dataset.log_message`(
            'INFO', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
            FORMAT('Job completed successfully. Records processed: %d', v_records_processed)
        );

    EXCEPTION WHEN ERROR THEN
        -- Handle errors from d_ausd_v_ta_cntrct_valid_bq
        DECLARE error_message STRING;
        DECLARE error_stack STRING;
        DECLARE error_line INT64;

        SET error_message = @@error.message;
        SET error_stack = @@error.stack;
        SET error_line = @@error.statement_xids[OFFSET(0)]; -- Get line of error

        CALL `your_project_id.your_dataset.log_message`(
            'ERROR', v_job_run_id, v_job_name, p_job_kennung, p_eintrags_nr,
            FORMAT('Job failed during core processing. Error: %s at line %d. Stack: %s', error_message, error_line, error_stack),
            NULL, error_message
        );

        -- Update job control with failure status
        UPDATE `your_project_id.your_dataset.job_control`
        SET
            status = 'FAILED',
            end_timestamp = CURRENT_TIMESTAMP(),
            message = FORMAT('Failed during core processing: %s', error_message)
        WHERE
            job_run_id = v_job_run_id;

        RAISE; -- Re-raise the error to propagate it
    END;

END;