-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE OR REPLACE PROCEDURE project.dataset.sp_vertragsdatenabgleich(
    p_show_help BOOL,
    p_param_s STRING,
    p_param_l STRING
)
BEGIN
    -- Declare variables
    DECLARE v_prog_name STRING DEFAULT 'r_ausd_v_ta_cntrct_templ.ksh';
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_CNTRCT_TEMPL';
    DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_cntrct_templ.ksh';
    DECLARE v_job_nr INT64;
    DECLARE v_log_message STRING;
    DECLARE v_job_status STRING;
    DECLARE v_current_timestamp TIMESTAMP;

    -- Error handling variables
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;
    DECLARE v_error_code STRING;

    -- Get current timestamp
    SET v_current_timestamp = CURRENT_TIMESTAMP();

    -- Get a unique job number
    SELECT IFNULL(MAX(job_nr), 0) + 1 INTO v_job_nr FROM project.dataset.job_registry;

    -- Initialize job status to RUNNING
    SET v_job_status = 'RUNNING';

    -- Insert initial job record into job_registry
    INSERT INTO project.dataset.job_registry (job_nr, job_kennung, script_name, status, start_timestamp, last_update_timestamp)
    VALUES (v_job_nr, v_job_kennung, v_script_name, v_job_status, v_current_timestamp, v_current_timestamp);

    -- Log job start
    SET v_log_message = FORMAT("Job %d (%s) started for script %s.", v_job_nr, v_job_kennung, v_script_name);
    INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
    VALUES (v_job_nr, v_job_kennung, 'INFO', v_log_message, v_current_timestamp);

    -- Parameter validation and help message
    IF p_show_help THEN
        SET v_log_message = "Help message: This procedure orchestrates the contract data reconciliation. Parameters: -h (show help), -s (param_s), -l (param_l).";
        INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
        VALUES (v_job_nr, v_job_kennung, 'INFO', v_log_message, CURRENT_TIMESTAMP());

        -- Update job status to indicate completion after showing help
        SET v_job_status = 'COMPLETED_WITH_HELP';
        UPDATE project.dataset.job_registry
        SET status = v_job_status,
            end_timestamp = CURRENT_TIMESTAMP(),
            last_update_timestamp = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

        SET v_log_message = FORMAT("Job %d (%s) completed after showing help.", v_job_nr, v_job_kennung);
        INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
        VALUES (v_job_nr, v_job_kennung, 'INFO', v_log_message, CURRENT_TIMESTAMP());

        RETURN;
    END IF;

    -- Main logic block with error handling
    BEGIN
        -- Call the core processing stored procedure
        -- NOTE: project.dataset.sp_k_ausd_v_ta_cntrct_templ must be created separately
        CALL project.dataset.sp_k_ausd_v_ta_cntrct_templ(v_job_nr, v_job_kennung, p_param_s, p_param_l);

        -- If core procedure succeeds
        SET v_job_status = 'SUCCESS';
        SET v_log_message = FORMAT("Job %d (%s) completed successfully.", v_job_nr, v_job_kennung);
        INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
        VALUES (v_job_nr, v_job_kennung, 'INFO', v_log_message, CURRENT_TIMESTAMP());

        UPDATE project.dataset.job_registry
        SET status = v_job_status,
            end_timestamp = CURRENT_TIMESTAMP(),
            last_update_timestamp = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'ERROR';
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        SET v_error_code = @@error.statement_text; -- Or other relevant part of @@error

        -- Log error to job_error_log
        INSERT INTO project.dataset.job_error_log (job_nr, job_kennung, err_nr, err_arg, message, error_timestamp)
        VALUES (v_job_nr, v_job_kennung, NULL, v_error_code, FORMAT("Error in job %d: %s\nStack: %s", v_job_nr, v_error_message, v_error_stack), CURRENT_TIMESTAMP());

        -- Update job_registry with ERROR status
        UPDATE project.dataset.job_registry
        SET status = v_job_status,
            end_timestamp = CURRENT_TIMESTAMP(),
            last_update_timestamp = CURRENT_TIMESTAMP()
        WHERE job_nr = v_job_nr;

        SET v_log_message = FORMAT("Job %d (%s) failed with error: %s", v_job_nr, v_job_kennung, v_error_message);
        INSERT INTO project.dataset.job_log (job_nr, job_kennung, log_level, message, log_timestamp)
        VALUES (v_job_nr, v_job_kennung, 'ERROR', v_log_message, CURRENT_TIMESTAMP());

        -- Re-raise the error to notify caller or orchestration system
        RAISE USING MESSAGE = FORMAT("Job %d failed. Details in job_error_log and job_log. Error: %s", v_job_nr, v_error_message);
    END;

END;