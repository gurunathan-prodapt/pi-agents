-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
--
-- This BigQuery Stored Procedure serves as the orchestration wrapper, replacing the original KornShell script.
-- It handles parameter parsing, date defaulting, error management, and logging, then invokes
-- the core business logic procedure.
-- Replace `your_gcp_project.your_bq_dataset` with your actual GCP project ID and BigQuery dataset name.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.ausd_bp_ta_bpr_optionen_wrapper`(
    IN p_stichtag_raw STRING OPTIONS(description="Optional Stichtag (reference date) in 'YYYY-MM-DD' format. Defaults to current date."),
    IN p_wiederanlaufwert_raw STRING OPTIONS(description="Optional Wiederanlaufwert (restart value). Defaults to 0.")
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bpr_optionen.ksh';
    DECLARE v_job_run_id STRING DEFAULT GENERATE_UUID(); -- Unique ID for this specific run
    DECLARE v_process_id STRING DEFAULT CAST(CURRENT_PROCESS_ID() AS STRING);
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert INT64;

    -- 1. Parameter Parsing and Defaulting for Stichtag
    IF p_stichtag_raw IS NULL OR p_stichtag_raw = '' THEN
        SET v_stichtag = CURRENT_DATE();
        CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
            v_job_run_id, v_job_name, 'INFO',
            FORMAT('Stichtag not provided, defaulting to current date: %T', v_stichtag),
            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
        );
    ELSE
        BEGIN
            SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag_raw);
            CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
                v_job_run_id, v_job_name, 'INFO',
                FORMAT('Stichtag provided: %T', v_stichtag),
                v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
            );
        EXCEPTION WHEN ERROR THEN
            CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
                v_job_run_id, v_job_name, 'ERROR',
                FORMAT('Invalid Stichtag format provided: %s. Expected YYYY-MM-DD. Exiting.', p_stichtag_raw),
                v_stichtag, v_wiederanlaufwert, v_process_id, NULL, 'DATE_PARSE_ERROR', p_stichtag_raw
            );
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Stichtag format: %s', p_stichtag_raw);
        END;
    END IF;

    -- 2. Parameter Parsing and Defaulting for Wiederanlaufwert
    IF p_wiederanlaufwert_raw IS NULL OR p_wiederanlaufwert_raw = '' THEN
        SET v_wiederanlaufwert = 0;
        CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
            v_job_run_id, v_job_name, 'INFO',
            FORMAT('Wiederanlaufwert not provided, defaulting to 0: %d', v_wiederanlaufwert),
            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
        );
    ELSE
        BEGIN
            SET v_wiederanlaufwert = CAST(p_wiederanlaufwert_raw AS INT64);
            CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
                v_job_run_id, v_job_name, 'INFO',
                FORMAT('Wiederanlaufwert provided: %d', v_wiederanlaufwert),
                v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
            );
        EXCEPTION WHEN ERROR THEN
            CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
                v_job_run_id, v_job_name, 'ERROR',
                FORMAT('Invalid Wiederanlaufwert format provided: %s. Expected integer. Exiting.', p_wiederanlaufwert_raw),
                v_stichtag, v_wiederanlaufwert, v_process_id, NULL, 'INT_PARSE_ERROR', p_wiederanlaufwert_raw
            );
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid Wiederanlaufwert format: %s', p_wiederanlaufwert_raw);
        END;
    END IF;

    -- 3. Initialize job status to RUNNING
    CALL `your_gcp_project.your_bq_dataset.f_alis_update_job_status`(
        v_job_run_id, v_job_name, 'RUNNING', v_stichtag, v_wiederanlaufwert, v_start_timestamp
    );

    BEGIN
        -- 4. Log job start
        CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
            v_job_run_id, v_job_name, 'INFO',
            FORMAT('Job %s started with Stichtag=%t, Wiederanlaufwert=%d', v_job_name, v_stichtag, v_wiederanlaufwert),
            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
        );

        -- 5. Invoke Core Business Logic Procedure
        CALL `your_gcp_project.your_bq_dataset.k_ausd_bp_ta_bpr_optionen`(
            v_job_run_id, v_stichtag, v_wiederanlaufwert
        );

        -- 6. Log job success
        CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
            v_job_run_id, v_job_name, 'INFO',
            FORMAT('Job %s completed successfully.', v_job_name),
            v_stichtag, v_wiederanlaufwert, v_process_id, NULL, NULL, NULL
        );

        -- 7. Update job status to SUCCESS
        CALL `your_gcp_project.your_bq_dataset.f_alis_update_job_status`(
            v_job_run_id, v_job_name, 'SUCCESS', v_stichtag, v_wiederanlaufwert
        );

    EXCEPTION WHEN ERROR THEN
        -- 8. Error Handling and Logging
        DECLARE v_error_message STRING;
        DECLARE v_stack_trace STRING;
        DECLARE v_error_code STRING;
        DECLARE v_error_line INT64;

        SET v_error_message = @@error.message;
        SET v_stack_trace = @@error.stack_trace;
        SET v_error_code = @@error.code;
        SET v_error_line = @@error.statement_text_start; -- This might not be the exact line number in the current context, but it's the best available.

        CALL `your_gcp_project.your_bq_dataset.f_alis_log_message`(
            v_job_run_id, v_job_name, 'ERROR',
            FORMAT('Job %s failed. Error: %s. Stack Trace: %s', v_job_name, v_error_message, v_stack_trace),
            v_stichtag, v_wiederanlaufwert, v_process_id, v_error_line, v_error_code, NULL
        );

        -- 9. Update job status to FAILED
        CALL `your_gcp_project.your_bq_dataset.f_alis_update_job_status`(
            v_job_run_id, v_job_name, 'FAILED', v_stichtag, v_wiederanlaufwert
        );

        -- Re-raise the error to propagate failure to the caller/orchestrator
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Job %s failed: %s', v_job_name, v_error_message);
    END;

END;