--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
--
-- Purpose: Main wrapper procedure for provisioning selected base products for BERT.
-- Handles parameter parsing, validation, defaults, logging, and orchestration of the core logic.
--
CREATE OR REPLACE PROCEDURE `project.dataset.bereitstellung_basisprodukte_bert`(
    p_stichtag STRING,          -- Input Stichtag in DDMMYYYY format (e.g., '31122023')
    p_wiederanlaufWert INT64    -- Restart value for processing contracts
)
OPTIONS (
    description = 'Main wrapper procedure for provisioning selected base products for BERT (Legacy: r_ausd_bp_ta_rn_da_vda_tk.ksh)'
)
BEGIN
    -- 1. Declare variables
    DECLARE v_job_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
    DECLARE v_job_version STRING DEFAULT 'V2.0.0';
    DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_rn_da_vda_tk';
    DECLARE v_sysdate_ddmmyyyy STRING;
    DECLARE v_stichtag_processed STRING;
    DECLARE v_restart_value_processed INT64;
    DECLARE v_job_entry_nr INT64;
    DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- For error handling
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;
    DECLARE v_error_state STRING;

    -- 2. Initialize restart value: If p_wiederanlaufWert is NULL, set to 0.
    SET v_restart_value_processed = COALESCE(p_wiederanlaufWert, 0);

    -- 3. Get current system date in DDMMYYYY format
    SET v_sysdate_ddmmyyyy = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- 4. Determine Stichtag: If p_stichtag is not provided or is an empty string, default to current system date.
    SET v_stichtag_processed = COALESCE(NULLIF(p_stichtag, ''), v_sysdate_ddmmyyyy);

    -- 5. Validate Stichtag
    -- Ensure Stichtag is not NULL or empty after defaulting
    IF v_stichtag_processed IS NULL OR v_stichtag_processed = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag (p_stichtag) cannot be NULL or empty after defaulting.';
    END IF;

    -- Basic format validation for DDMMYYYY
    IF SAFE.PARSE_DATE('%d%m%Y', v_stichtag_processed) IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Stichtag format. Expected DDMMYYYY, got: %s', v_stichtag_processed);
    END IF;

    -- 6. Generate a unique job entry number for this run. Using microseconds timestamp as a unique INT64.
    SET v_job_entry_nr = UNIX_MICROS(v_current_timestamp);

    -- 7. Log job start and control parameters to respective tables
    INSERT INTO `project.dataset.job_log` (job_name, job_version, job_kennung, log_level, log_message, created_at)
    VALUES (
        v_job_name,
        v_job_version,
        v_job_kennung,
        'INFO',
        FORMAT('Job started with Stichtag: %s, Restart Value: %d, Job Entry Nr: %d', v_stichtag_processed, v_restart_value_processed, v_job_entry_nr),
        v_current_timestamp
    );

    INSERT INTO `project.dataset.job_control` (job_kennung, stichtag, sysdate_ddmmyyyy, restart_value, created_at)
    VALUES (
        v_job_kennung,
        v_stichtag_processed,
        v_sysdate_ddmmyyyy,
        v_restart_value_processed,
        v_current_timestamp
    );

    INSERT INTO `project.dataset.job_status` (job_kennung, job_entry_nr, status, updated_at)
    VALUES (
        v_job_kennung,
        v_job_entry_nr,
        'RUNNING',
        v_current_timestamp
    );

    -- 8. Core logic execution within a BEGIN-EXCEPTION block for robust error handling
    BEGIN
        -- Call the kernel script stored procedure which contains the actual data processing logic
        CALL `project.dataset.k_ausd_bp_ta_rn_da_vda_tk`(
            v_job_kennung,
            v_stichtag_processed,
            v_job_entry_nr,
            v_restart_value_processed
        );

        -- If the kernel procedure completes successfully, update the job status to SUCCESS
        UPDATE `project.dataset.job_status`
        SET status = 'SUCCESS', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = v_job_kennung AND job_entry_nr = v_job_entry_nr;

        -- Log job completion
        INSERT INTO `project.dataset.job_log` (job_name, job_version, job_kennung, log_level, log_message, created_at)
        VALUES (
            v_job_name,
            v_job_version,
            v_job_kennung,
            'INFO',
            'Job completed successfully.',
            CURRENT_TIMESTAMP()
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture error details from the BigQuery exception
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        SET v_error_state = @@error.statement_text;

        -- Log the error details
        INSERT INTO `project.dataset.job_log` (job_name, job_version, job_kennung, log_level, log_message, created_at)
        VALUES (
            v_job_name,
            v_job_version,
            v_job_kennung,
            'ERROR',
            FORMAT('Job failed. Error: %s, Stack: %s, Statement: %s', v_error_message, v_error_stack, v_error_state),
            CURRENT_TIMESTAMP()
        );

        -- Update job status to FAILED
        UPDATE `project.dataset.job_status`
        SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = v_job_kennung AND job_entry_nr = v_job_entry_nr;

        -- Re-raise the error to signal failure to any calling orchestration system (e.g., Cloud Composer)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Job %s (Entry %d) failed: %s', v_job_kennung, v_job_entry_nr, v_error_message);
    END;

END;