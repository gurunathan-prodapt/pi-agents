-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- This BigQuery Stored Procedure orchestrates the data processing for ta_cntrct_crs2.
-- It replaces the control flow, parameter handling, and job management logic of the original KornShell script.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung STRING,
    p_eintrags_nr STRING
)
BEGIN
    DECLARE v_error_code INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_processed_records INT64;
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Helper procedure for error logging
    CREATE TEMPORARY PROCEDURE LogError(err_nr INT64, err_arg STRING, message STRING, job_k STRING, entry_n STRING, stack STRING)
    BEGIN
        INSERT INTO `project.dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, error_message, stack_trace, created_at)
        VALUES (job_k, entry_n, err_nr, err_arg, message, stack, CURRENT_TIMESTAMP());
    END;

    -- Parameter Validation
    IF p_job_kennung IS NULL OR p_eintrags_nr IS NULL THEN
        SET v_error_code = 1;
        SET v_error_message = 'ERROR: Required parameters p_job_kennung or p_eintrags_nr are not set.';
        CALL LogError(v_error_code, 'PARAMETER_MISSING', v_error_message, p_job_kennung, p_eintrags_nr, @@error.stack_trace);
        RAISE USING MESSAGE v_error_message;
    END IF;

    BEGIN
        -- Deactivate older active jobs for the same job_kennung
        UPDATE `project.dataset.job_table`
        SET
            active_flag = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE
            job_kennung = p_job_kennung
            AND active_flag = TRUE
            AND eintrags_nr != p_eintrags_nr; -- Ensure we don't deactivate the current job if it already exists

        -- Register the current job as active
        MERGE `project.dataset.job_table` T
        USING (SELECT p_job_kennung AS job_k, p_eintrags_nr AS entry_n) S
        ON T.job_kennung = S.job_k AND T.eintrags_nr = S.entry_n
        WHEN MATCHED THEN
            UPDATE SET T.active_flag = TRUE, T.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (job_kennung, eintrags_nr, active_flag, created_at, updated_at)
            VALUES (S.job_k, S.entry_n, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

        -- Execute the core data transformation procedure
        CALL `project.dataset.d_ausd_v_ta_cntrct_crs2_sp`(p_job_kennung, p_eintrags_nr);

        -- Capture processed records (assuming d_ausd_v_ta_cntrct_crs2_sp returns this or uses a temp table)
        -- For now, we assume the previous call sets @@row_count implicitly or we get it from a SELECT in the SP
        -- If d_ausd_v_ta_cntrct_crs2_sp returns a value, we would capture it like:
        -- SELECT records_processed FROM `project.dataset.d_ausd_v_ta_cntrct_crs2_sp_result_temp_table`;
        -- For demonstration, let's assume @@row_count from the *last DML operation within this orchestrator*
        -- or directly from the placeholder SP.
        SET v_processed_records = (SELECT records_processed FROM `project.dataset.d_ausd_v_ta_cntrct_crs2_sp`(p_job_kennung, p_eintrags_nr)); -- This is a simplification; a procedure returns its last SELECT.

        -- Update the current job status to inactive after successful completion
        UPDATE `project.dataset.job_table`
        SET
            active_flag = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE
            job_kennung = p_job_kennung
            AND eintrags_nr = p_eintrags_nr;

        -- Audit the job run
        INSERT INTO `project.dataset.job_run_audit` (job_kennung, eintrags_nr, tab_name, records_processed, start_timestamp, end_timestamp, status, created_at)
        VALUES (p_job_kennung, p_eintrags_nr, 'ta_cntrct_crs2', v_processed_records, v_start_timestamp, CURRENT_TIMESTAMP(), 'SUCCESS', CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        SET v_error_code = -1; -- Generic error for unexpected exceptions
        SET v_error_message = @@error.message;

        -- Log the error
        CALL LogError(v_error_code, 'UNEXPECTED_EXCEPTION', v_error_message, p_job_kennung, p_eintrags_nr, @@error.stack_trace);

        -- Update job status to inactive and failed
        UPDATE `project.dataset.job_table`
        SET
            active_flag = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE
            job_kennung = p_job_kennung
            AND eintrags_nr = p_eintrags_nr;

        -- Audit the failed job run
        INSERT INTO `project.dataset.job_run_audit` (job_kennung, eintrags_nr, tab_name, records_processed, start_timestamp, end_timestamp, status, created_at)
        VALUES (p_job_kennung, p_eintrags_nr, 'ta_cntrct_crs2', 0, v_start_timestamp, CURRENT_TIMESTAMP(), 'FAILED', CURRENT_TIMESTAMP());

        -- Re-raise the error to propagate it
        RAISE USING MESSAGE v_error_message;
    END;

END;