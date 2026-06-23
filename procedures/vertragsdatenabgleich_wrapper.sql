-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

-- BigQuery Stored Procedure to replace the KornShell wrapper script.
-- It handles parameter parsing, job metadata, logging, and error handling,
-- and orchestrates the call to the core processing logic.
CREATE OR REPLACE PROCEDURE `project.dataset.vertragsdatenabgleich_wrapper`(
    p_job_kennung STRING,
    p_show_help BOOL DEFAULT FALSE,
    p_some_param_s STRING DEFAULT NULL, -- Placeholder for '-s' option, if any
    p_some_param_l STRING DEFAULT NULL  -- Placeholder for '-l' option, if any
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'r_ausd_v_ta_cntrct_crs';
    DECLARE v_prog_version STRING DEFAULT '1.0';
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_job_kennung_internal STRING;
    DECLARE v_sys_date DATE;
    DECLARE v_log_file_name STRING;
    DECLARE v_name_kernskript STRING DEFAULT 'k_ausd_v_ta_cntrct_crs';
    DECLARE v_status STRING DEFAULT 'RUNNING';
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_error_message STRING;
    DECLARE v_error_details STRING;

    -- Help documentation, analogous to 'usage' function in ksh
    IF p_show_help THEN
        SELECT 'Usage: CALL `project.dataset.vertragsdatenabgleich_wrapper`(p_job_kennung => ''<job_kennung>'', [p_show_help => FALSE], [p_some_param_s => NULL], [p_some_param_l => NULL]);' AS Usage;
        SELECT '  p_job_kennung STRING: Job identifier (mandatory).' AS Option_J;
        SELECT '  p_some_param_s STRING: Some parameter S (optional, not used in legacy script).' AS Option_S;
        SELECT '  p_some_param_l STRING: Some parameter L (optional, not used in legacy script).' AS Option_L;
        SELECT '  p_show_help BOOL: Display this help message.' AS Option_Help;
        LEAVE;
    END IF;

    -- Parameter Validation: Check if p_job_kennung is provided
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_message = 'JobKennung is mandatory.';
        SET v_error_details = 'Parameter `p_job_kennung` was not provided or was empty.';
        INSERT INTO `project.dataset.job_error_log` (job_id, error_timestamp, error_code, error_message, error_details)
        VALUES (0, CURRENT_TIMESTAMP(), 'PARAM_ERROR', v_error_message, v_error_details);
        RAISE USING MESSAGE = 'FATAL ERROR: ' || v_error_message || ' ' || v_error_details;
    END IF;

    SET v_job_kennung_internal = p_job_kennung; -- Assign validated JobKennung

    -- Determine DW_EintragsNr (unique job identifier)
    -- In BigQuery, this can be a sequence, UUID, or derived from MAX+1.
    -- Following design, we use MAX(job_id) + 1 from job_control.
    SELECT COALESCE(MAX(job_id), 0) + 1 INTO v_dw_eintrags_nr
    FROM `project.dataset.job_control`;

    -- Conceptual log file name (actual logging goes to BigQuery tables)
    SET v_log_file_name = FORMAT('bigquery_job_%d_%s.log', v_dw_eintrags_nr, FORMAT_DATE('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP()));

    -- Start job control entry and initial log message
    INSERT INTO `project.dataset.job_control` (job_id, job_kennung, program_name, program_version, start_time, status, log_file_name)
    VALUES (v_dw_eintrags_nr, v_job_kennung_internal, v_prog_name, v_prog_version, v_start_time, v_status, v_log_file_name);

    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
    VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('%s (Version %s) started for JobKennung: %s, DW_EintragsNr: %d', v_prog_name, v_prog_version, v_job_kennung_internal, v_dw_eintrags_nr));

    -- Set system date (Stichtag) and update job_control
    SET v_sys_date = CURRENT_DATE();
    UPDATE `project.dataset.job_control`
    SET reference_date = v_sys_date
    WHERE job_id = v_dw_eintrags_nr;

    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
    VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Reference Date (Stichtag): %s', FORMAT_DATE('%Y-%m-%d', v_sys_date)));

    -- Main logic block with BigQuery's EXCEPTION handling
    BEGIN
        -- Invoke the core processing script (migrated to a BigQuery Stored Procedure)
        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('Invoking core script: %s', v_name_kernskript));

        CALL `project.dataset.k_ausd_v_ta_cntrct_crs`(v_job_kennung_internal, v_dw_eintrags_nr);

        -- If core script completes successfully
        SET v_status = 'OK';
        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'INFO', FORMAT('%s completed successfully for JobKennung: %s', v_prog_name, v_job_kennung_internal));

    EXCEPTION WHEN ERROR THEN
        -- Error handling block, replaces ksh trap and error functions
        SET v_status = 'ERROR';
        SET v_error_message = @@error.message;
        SET v_error_details = CONCAT('Code: ', @@error.code, ', Stack Trace: ', @@error.stack_trace);

        INSERT INTO `project.dataset.job_error_log` (job_id, error_timestamp, error_code, error_message, error_details)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), @@error.code, v_error_message, v_error_details);

        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message)
        VALUES (v_dw_eintrags_nr, CURRENT_TIMESTAMP(), 'ERROR', FORMAT('%s failed for JobKennung: %s with error: %s', v_prog_name, v_job_kennung_internal, v_error_message));

        -- Re-raise the error to signal failure to external orchestrator (e.g., Cloud Composer)
        RAISE USING MESSAGE = FORMAT('JOB FAILED: %s for JobKennung: %s. Details: %s', v_prog_name, v_job_kennung_internal, v_error_message);
    END;

    -- Final update of job control status and end time
    UPDATE `project.dataset.job_control`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status
    WHERE job_id = v_dw_eintrags_nr;

END;