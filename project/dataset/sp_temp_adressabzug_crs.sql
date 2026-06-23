-- Migrated wrapper logic from r_ausd_adressen.ksh
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_temp_adressabzug_crs`(
    p_stichtag STRING,
    p_wiederanlaufWert STRING
)
OPTIONS(
    description="Orchestrates the address extraction process, handles parameters, logging, and error handling, replacing r_ausd_adressen.ksh."
)
BEGIN
    -- Declare variables
    DECLARE v_job_run_id STRING;
    DECLARE v_job_name STRING DEFAULT 'sp_temp_adressabzug_crs';
    DECLARE v_stichtag_date DATE;
    DECLARE v_wiederanlauf_wert_int INT64;
    DECLARE v_start_time TIMESTAMP;

    -- Generate a unique job run ID
    SET v_job_run_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Default and convert parameters
    -- p_wiederanlaufWert is defaulted to 0 if not provided
    SET v_wiederanlauf_wert_int = CAST(IFNULL(NULLIF(TRIM(p_wiederanlaufWert), ''), '0') AS INT64);

    -- p_stichtag is defaulted to current system date (DDMMYYYY) if missing
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', IFNULL(NULLIF(TRIM(p_stichtag), ''), FORMAT_DATE('%d%m%Y', CURRENT_DATE())));

    -- Log job start and initial parameters
    INSERT INTO `project.dataset.job_control` (job_run_id, job_name, start_time, status, stichtag, wiederanlauf_wert)
    VALUES (v_job_run_id, v_job_name, v_start_time, 'RUNNING', v_stichtag_date, v_wiederanlauf_wert_int);

    INSERT INTO `project.dataset.job_log` (job_run_id, log_timestamp, log_level, message)
    VALUES (v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Job %s started with Stichtag: %t, Wiederanlaufwert: %d", v_job_name, v_stichtag_date, v_wiederanlauf_wert_int));

    -- Validation: Check if p_stichtag is set (after defaulting)
    IF v_stichtag_date IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parameter p_stichtag cannot be null after defaulting. Please provide a valid date or ensure system date is available.';
    END IF;

    -- Main logic: Call the core extraction procedure
    CALL `project.dataset.sp_ausd_adressen`(v_stichtag_date, v_wiederanlauf_wert_int);

    -- Log job success
    INSERT INTO `project.dataset.job_log` (job_run_id, log_timestamp, log_level, message)
    VALUES (v_job_run_id, CURRENT_TIMESTAMP(), 'INFO', FORMAT("Job %s completed successfully.", v_job_name));

    -- Update job control table with success status
    UPDATE `project.dataset.job_control`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = 'OK'
    WHERE job_run_id = v_job_run_id;

EXCEPTION WHEN ERROR THEN
    DECLARE error_message STRING;
    DECLARE stack_trace STRING;
    SET error_message = @@error.message;
    SET stack_trace = @@error.stack_trace;

    -- Log error
    INSERT INTO `project.dataset.job_log` (job_run_id, log_timestamp, log_level, message)
    VALUES (v_job_run_id, CURRENT_TIMESTAMP(), 'ERROR', FORMAT("Job %s failed with error: %s", v_job_name, error_message));

    INSERT INTO `project.dataset.job_error_log` (job_run_id, error_timestamp, job_name, error_message, stack_trace)
    VALUES (v_job_run_id, CURRENT_TIMESTAMP(), v_job_name, error_message, stack_trace);

    -- Update job control table with failure status
    UPDATE `project.dataset.job_control`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = 'FAILED',
        error_message = error_message
    WHERE job_run_id = v_job_run_id;

    -- Re-raise the error to propagate it
    RAISE;
END;