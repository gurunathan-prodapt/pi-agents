-- Target: BigQuery Stored Procedure
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- Description: Orchestration wrapper for ta_p_discount data synchronization. Replaces r_ausd_v_ta_p_discount.ksh.

CREATE OR REPLACE PROCEDURE project.dataset.sp_bert_v_ta_p_discount(
    p_h BOOL, -- Help flag: If TRUE, displays usage information.
    p_s STRING, -- Source parameter (optional, passed through to core script if needed).
    p_l STRING  -- Language parameter (optional, passed through to core script if needed).
)
OPTIONS(
    description="Orchestration wrapper for ta_p_discount data synchronization. This procedure replaces the KornShell script r_ausd_v_ta_p_discount.ksh, handling environment setup, parameter parsing, error logging, and invoking the core data synchronization logic."
)
BEGIN
    -- Declare variables for job execution and logging
    DECLARE v_prog_name STRING DEFAULT 'sp_bert_v_ta_p_discount';
    DECLARE v_prog_version STRING DEFAULT '1.0'; -- Placeholder for version
    DECLARE v_job_kennung STRING DEFAULT 'r_ausd_v_ta_p_discount'; -- Derived from original script name
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_log_file_path STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_stack_trace STRING;

    -- --- Parameter Handling and Usage Display ---
    -- Mimics the 'usage()' function of the original KornShell script.
    IF p_h THEN
        SELECT 'Usage: CALL project.dataset.sp_bert_v_ta_p_discount(p_h => [TRUE|FALSE], p_s => ''<source_value>'', p_l => ''<language_value>'')' AS Usage_Info;
        SELECT '  p_h: BOOLEAN - Set to TRUE to display this help message.' AS Usage_Detail;
        SELECT '  p_s: STRING - Optional source parameter, passed to the core synchronization procedure.' AS Usage_Detail;
        SELECT '  p_l: STRING - Optional language parameter, passed to the core synchronization procedure.' AS Usage_Detail;
        RETURN;
    END IF;

    -- --- Main Job Logic with Error Handling ---
    BEGIN
        -- Step 1: Initialize Job Entry Number and Log File Path
        -- Determine the next sequential entry number for this job_kennung.
        SELECT IFNULL(MAX(dw_eintrags_nr), 0) + 1 INTO v_dw_eintrags_nr
        FROM project.dataset.dw_job_log
        WHERE job_kennung = v_job_kennung;

        -- Create a logical log file path identifier based on job_kennung and entry number.
        SET v_log_file_path = CONCAT(v_job_kennung, '_', CAST(v_dw_eintrags_nr AS STRING), '.log');

        -- Step 2: Record Job Start in dw_job_log
        INSERT INTO project.dataset.dw_job_log (
            job_kennung,
            dw_eintrags_nr,
            prog_name,
            prog_version,
            log_file_path,
            status,
            start_timestamp,
            message
        )
        VALUES (
            v_job_kennung,
            v_dw_eintrags_nr,
            v_prog_name,
            v_prog_version,
            v_log_file_path,
            'RUNNING', -- Set initial status to 'RUNNING'
            CURRENT_TIMESTAMP(),
            'Job orchestration started successfully.'
        );

        -- Step 3: Record Job Context Information in dw_job_context
        INSERT INTO project.dataset.dw_job_context (
            job_kennung,
            dw_eintrags_nr,
            stichtag,
            creation_timestamp
        )
        VALUES (
            v_job_kennung,
            v_dw_eintrags_nr,
            v_sysdate, -- Use the determined system date
            CURRENT_TIMESTAMP()
        );

        -- Step 4: Invoke the Core Data Synchronization Logic
        -- This calls the separate stored procedure for the actual data synchronization.
        CALL project.dataset.sp_k_ausd_v_ta_p_discount(v_job_kennung, v_dw_eintrags_nr, p_s, p_l);

        -- Step 5: Update Job Status to OK upon Successful Completion
        UPDATE project.dataset.dw_job_log
        SET
            status = 'OK', -- Set final status to 'OK'
            end_timestamp = CURRENT_TIMESTAMP(),
            message = 'Job orchestration and core synchronization completed successfully.'
        WHERE
            job_kennung = v_job_kennung AND dw_eintrags_nr = v_dw_eintrags_nr;

    EXCEPTION WHEN ERROR THEN
        -- --- Error Handling Block ---
        -- Captures BigQuery error details.
        SET v_error_message = @@ERROR_MESSAGE;
        SET v_error_code = @@ERROR_CODE;
        -- @@ERROR_STACK_TRACE is not directly available in standard BQSQL EXCEPTION blocks for a full stack trace.
        -- More comprehensive tracing would require structured logging or external mechanisms.
        SET v_stack_trace = CONCAT('BigQuery Error (Code: ', v_error_code, '): ', v_error_message);

        -- Log the error details into dw_error_log
        INSERT INTO project.dataset.dw_error_log (
            job_kennung,
            dw_eintrags_nr,
            error_timestamp,
            error_message,
            error_code,
            stack_trace
        )
        VALUES (
            v_job_kennung,
            v_dw_eintrags_nr,
            CURRENT_TIMESTAMP(),
            v_error_message,
            v_error_code,
            v_stack_trace
        );

        -- Update the job status to FAILED in dw_job_log
        UPDATE project.dataset.dw_job_log
        SET
            status = 'FAILED', -- Set final status to 'FAILED'
            end_timestamp = CURRENT_TIMESTAMP(),
            message = CONCAT('Job orchestration failed: ', v_error_message)
        WHERE
            job_kennung = v_job_kennung AND dw_eintrags_nr = v_dw_eintrags_nr;

        -- Re-raise the error to signal failure to any calling process or scheduler.
        RAISE EXCEPTION FORMAT 'Job (%s) run_id (%d) failed: %s', v_job_kennung, v_dw_eintrags_nr, v_error_message;
    END;

END;