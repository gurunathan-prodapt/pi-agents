-- BigQuery Stored Procedure for r_ausd_bp_ta_bcp_iccid
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Purpose: Orchestration wrapper for "Bereitstellung Basisprodukte BERT" ETL job.
-- Handles parameter parsing, logging, and invokes the core kernel procedure.

CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_ibcp_ccid(
    IN p_stichtag_str STRING, -- Optional: Stichtag in 'YYYY-MM-DD' format (e.g., '2023-10-26')
    IN p_wiederanlaufwert_str STRING -- Optional: Wiederanlaufwert as string (e.g., '10000')
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bcp_iccid';
    DECLARE v_run_id STRING;
    DECLARE v_log_id STRING; -- Unique ID for this specific log entry
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_details STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlaufwert INT64;

    -- Initialize run_id and start_time for overall job execution
    SET v_run_id = GENERATE_UUID();
    SET v_log_id = GENERATE_UUID(); -- Unique ID for the top-level log entry
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = 'Job execution started and parameters are being processed.';

    -- Insert initial log entry to mark the start of the job
    INSERT INTO project.dataset.dwmsg_log (log_id, job_name, run_id, start_time, status, message, parameters)
    VALUES (
        v_log_id,
        v_job_name,
        v_run_id,
        v_start_time,
        v_status,
        v_message,
        TO_JSON(STRUCT(p_stichtag_str AS raw_stichtag, p_wiederanlaufwert_str AS raw_wiederanlaufwert))
    );

    BEGIN
        -- Parse and default parameters from input strings
        -- Stichtag (Cutoff Date): defaults to current system date if not provided
        IF p_stichtag_str IS NOT NULL AND TRIM(p_stichtag_str) != '' THEN
            SET v_stichtag = PARSE_DATE('%Y-%m-%d', p_stichtag_str);
        ELSE
            SET v_stichtag = CURRENT_DATE();
        END IF;

        -- Wiederanlaufwert (Restart Value): defaults to 0 if not provided
        IF p_wiederanlaufwert_str IS NOT NULL AND TRIM(p_wiederanlaufwert_str) != '' THEN
            SET v_wiederanlaufwert = CAST(p_wiederanlaufwert_str AS INT64);
        ELSE
            SET v_wiederanlaufwert = 0;
        END IF;

        -- Update the initial log entry with the parsed and defaulted parameters
        UPDATE project.dataset.dwmsg_log
        SET
            message = 'Parameters parsed successfully. Calling kernel procedure.',
            parameters = TO_JSON(STRUCT(v_stichtag AS stichtag, v_wiederanlaufwert AS wiederanlaufwert))
        WHERE log_id = v_log_id;

        -- Call the core kernel stored procedure
        CALL project.dataset.k_ausd_bp_ta_bcp_iccid(v_stichtag, v_wiederanlaufwert);

        SET v_status = 'SUCCESS';
        SET v_message = 'Job executed successfully.';

    EXCEPTION WHEN ERROR THEN
        -- Capture error details if any exception occurs during execution
        SET v_status = 'FAILED';
        SET v_message = 'Job execution failed.';
        SET v_error_details = @@error.message; -- BigQuery SQL function to get the last error message
    END;

    -- Final update to the log table with end_time, final status, and error details (if any)
    SET v_end_time = CURRENT_TIMESTAMP();
    UPDATE project.dataset.dwmsg_log
    SET
        end_time = v_end_time,
        status = v_status,
        message = v_message,
        error_details = v_error_details
    WHERE log_id = v_log_id;

    -- Re-raise the exception if the job failed, to propagate the error to the caller/orchestrator
    IF v_status = 'FAILED' THEN
        RAISE;
    END IF;
END;