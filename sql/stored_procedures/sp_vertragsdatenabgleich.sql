-- BigQuery Stored Procedure for r_ausd_v_ta_cntrct_crs2.ksh
-- Replaces the KornShell wrapper script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This procedure handles environment setup, parameter parsing, job and error logging,
-- and invocation of the core processing script (sp_k_ausd_v_ta_cntrct_crs2).

CREATE OR REPLACE PROCEDURE `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(
    p_job_kennung STRING,             -- Identifier for the job, e.g., 'TA_CNTRCT_CRS2'
    p_s STRING DEFAULT NULL,          -- Corresponds to original -s parameter (if required)
    p_l STRING DEFAULT NULL,          -- Corresponds to original -l parameter (if required)
    p_h BOOL DEFAULT FALSE            -- Display help message
)
BEGIN
    -- Declare variables to mimic shell script behavior
    DECLARE v_program_name STRING;
    DECLARE v_kernel_script_name STRING;
    DECLARE v_job_execution_nr INT64;
    DECLARE v_error_number INT64 DEFAULT 0;
    DECLARE v_message STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_start_ts TIMESTAMP;
    DECLARE v_end_ts TIMESTAMP;

    -- Error handling block for the entire procedure
    BEGIN
        -- 1. Handle help parameter (-h)
        IF p_h THEN
            SELECT 'Usage: CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`('
                   || 'p_job_kennung => ''JOB_ID'', p_s => ''optional_s_param'', p_l => ''optional_l_param'', p_h => TRUE);' AS usage;
            SELECT '  p_job_kennung: Identifier for the job (e.g., TA_CNTRCT_CRS2) - MANDATORY' AS desc_job_kennung;
            SELECT '  p_s: Placeholder for original -s parameter - MANDATORY' AS desc_s;
            SELECT '  p_l: Placeholder for original -l parameter - MANDATORY' AS desc_l;
            SELECT '  p_h: Display this help message' AS desc_h;
            RETURN;
        END IF;

        -- 2. Validate p_job_kennung and retrieve job configuration
        IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
            SET v_error_number = 1;
            SET v_message = 'ERROR: Parameter p_job_kennung is mandatory and cannot be empty.';
            INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (job_id, error_timestamp, error_code, error_message)
            VALUES ('UNKNOWN_JOB', CURRENT_TIMESTAMP(), v_error_number, v_message);
            RAISE SCRIPT_EXCEPTION(v_message);
        END IF;

        SELECT program_name, kernel_script_name
        INTO v_program_name, v_kernel_script_name
        FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control`
        WHERE job_kennung = p_job_kennung;

        IF v_program_name IS NULL THEN
            SET v_error_number = 2;
            SET v_message = 'ERROR: Job Kennung not found in `config_job_control`: ' || p_job_kennung;
            INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (job_id, error_timestamp, error_code, error_message)
            VALUES (p_job_kennung, CURRENT_TIMESTAMP(), v_error_number, v_message);
            RAISE SCRIPT_EXCEPTION(v_message);
        END IF;

        -- 3. Parameter validation (-s, -l) as per design document (pseudo-code suggests they can be missing)
        -- Assuming these are mandatory as implied by "sets ErrNr if missing"
        IF p_s IS NULL THEN
            SET v_error_number = 3;
            SET v_message = 'ERROR: Parameter -s is mandatory.';
            INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (job_id, error_timestamp, error_code, error_message)
            VALUES (p_job_kennung, CURRENT_TIMESTAMP(), v_error_number, v_message);
            RAISE SCRIPT_EXCEPTION(v_message);
        END IF;

        IF p_l IS NULL THEN
            SET v_error_number = 4;
            SET v_message = 'ERROR: Parameter -l is mandatory.';
            INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (job_id, error_timestamp, error_code, error_message)
            VALUES (p_job_kennung, CURRENT_TIMESTAMP(), v_error_number, v_message);
            RAISE SCRIPT_EXCEPTION(v_message);
        END IF;

        -- 4. Initialize Stichtag (reference date)
        SET v_stichtag = CURRENT_DATE();

        -- 5. Start logging and get job execution number (DW_EintragsNr)
        SET v_start_ts = CURRENT_TIMESTAMP();
        -- Generate a unique INT64 for the entry_number. FARM_FINGERPRINT is a common way to get a hash.
        SET v_job_execution_nr = ABS(FARM_FINGERPRINT(GENERATE_UUID()));

        INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_execution_log` (
            job_id, entry_number, start_timestamp, status, message, parameters_json, stichtag
        ) VALUES (
            p_job_kennung, v_job_execution_nr, v_start_ts, 'STARTED', 'Job execution started.',
            TO_JSON(STRUCT(p_s, p_l, p_h)), v_stichtag
        );

        -- 6. Print job metadata to console (simulated with SELECT statements)
        SELECT FORMAT('INFO: Job %s started at %s', p_job_kennung, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', v_start_ts)) AS job_start_info;
        SELECT FORMAT('INFO: Program Name: %s', v_program_name) AS prog_name_info;
        SELECT FORMAT('INFO: Stichtag: %s', FORMAT_DATE('%Y-%m-%d', v_stichtag)) AS stichtag_info;
        SELECT FORMAT('INFO: Log Entry Number: %d', v_job_execution_nr) AS log_entry_info;

        -- 7. Call the migrated core procedure
        -- The v_kernel_script_name variable determines which procedure to call.
        -- BigQuery does not support dynamic CALL statements directly from a variable.
        -- Assuming v_kernel_script_name will always be 'sp_k_ausd_v_ta_cntrct_crs2' for this job_kennung.
        -- If multiple kernel scripts were possible for p_job_kennung, a CASE statement would be needed here.
        -- For this migration, the design implies a direct mapping.
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs2`(
            p_job_kennung, v_job_execution_nr, p_s, p_l
        );

        -- 8. Success logging
        SET v_end_ts = CURRENT_TIMESTAMP();
        SET v_message = 'Job execution completed successfully.';

        UPDATE `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_execution_log`
        SET end_timestamp = v_end_ts,
            status = 'OK',
            message = v_message
        WHERE job_id = p_job_kennung AND entry_number = v_job_execution_nr;

        SELECT FORMAT('INFO: %s', v_message) AS success_message;

    EXCEPTION WHEN OTHERS THEN
        SET v_end_ts = CURRENT_TIMESTAMP();
        SET v_error_number = COALESCE(ERROR_CODE(), -1); -- Get error code if available, else -1
        SET v_message = 'ERROR: Job execution failed: ' || COALESCE(ERROR_MESSAGE(), 'Unknown error.');

        -- Update job_execution_log to FAILED if an entry exists
        -- This block ensures a FAILED status even if the insert above failed, as long as entry_number was set.
        IF v_job_execution_nr IS NOT NULL THEN
            UPDATE `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_execution_log`
            SET end_timestamp = v_end_ts,
                status = 'FAILED',
                message = v_message
            WHERE job_id = p_job_kennung AND entry_number = v_job_execution_nr;
        END IF;

        -- Insert into job_error_log
        INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (
            job_id, entry_number, error_timestamp, error_code, error_message, stack_trace
        ) VALUES (
            p_job_kennung, v_job_execution_nr, v_end_ts, v_error_number, v_message, @@error.stack_trace
        );

        SELECT FORMAT('ERROR: %s', v_message) AS error_output;
        RAISE; -- Re-raise the exception to signal failure to the caller
    END;
END;