-- BigQuery Stored Procedure: project.dataset.ausd_bp_ta_iccid_vertrag_wrapper
-- This procedure replicates the orchestration logic of the original KornShell script.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_iccid_vertrag_wrapper(
    IN p_stichtag STRING OPTIONS(description="Key date in DDMMYYYY format. Defaults to current date if NULL."),
    IN p_wiederanlaufWert INT64 OPTIONS(description="Restart value. Defaults to 0 if NULL.")
)
BEGIN
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_job_entry_nr INT64;
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_iccid_vertrag_wrapper_sp';
    DECLARE v_current_date_formatted STRING;

    -- Derive current system date for defaulting
    SET v_current_date_formatted = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Apply defaulting logic
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);
    SET v_stichtag = IFNULL(p_stichtag, v_current_date_formatted);

    -- Initialize job_entry_nr
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1 INTO v_job_entry_nr FROM project.dataset.job_control;

    -- Log initial status to job_control and job_run_log
    INSERT INTO project.dataset.job_control (job_entry_nr, job_name, stichtag, wiederanlaufwert, start_timestamp, status)
    VALUES (v_job_entry_nr, v_job_name, v_stichtag, v_wiederanlaufWert, CURRENT_TIMESTAMP(), 'RUNNING');

    INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'INFO', 'Job started: r_ausd_bp_ta_iccid_vertrag_wrapper_sp');

    INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'INFO', FORMAT('Parameters - Stichtag: %s, Wiederanlaufwert: %d', v_stichtag, v_wiederanlaufWert));

    -- Error handling block
    BEGIN
        -- Validation: Check if v_stichtag is a valid date (DDMMYYYY)
        -- Attempt to parse the date. If it fails, an error will be raised.
        SELECT PARSE_DATE('%d%m%Y', v_stichtag);
    EXCEPTION WHEN ERROR THEN
        -- Log validation error
        INSERT INTO project.dataset.job_error_log (error_timestamp, job_entry_nr, error_message, stichtag, wiederanlaufwert)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'Validation Error: Invalid Stichtag format. Expected DDMMYYYY.', p_stichtag, p_wiederanlaufWert);

        INSERT INTO project.dataset.job_usage_log (usage_timestamp, job_entry_nr, message, provided_stichtag, provided_wiederanlaufwert)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, FORMAT('Usage: CALL %s(p_stichtag => "DDMMYYYY", p_wiederanlaufWert => <INT64>). Invalid Stichtag provided: %s', v_job_name, p_stichtag), p_stichtag, CAST(p_wiederanlaufWert AS STRING));

        INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'ERROR', FORMAT('Validation Failed: Invalid Stichtag format "%s". Terminating.', v_stichtag));

        -- Update job_control status to ERROR
        UPDATE project.dataset.job_control
        SET end_timestamp = CURRENT_TIMESTAMP(), status = 'ERROR'
        WHERE job_entry_nr = v_job_entry_nr;

        RAISE USING MESSAGE = FORMAT('Invalid Stichtag parameter "%s". Expected DDMMYYYY format.', v_stichtag);
    END;

    -- Core script invocation
    BEGIN
        INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'INFO', FORMAT('Calling core procedure k_ausd_bp_ta_iccid_vertrag with Stichtag: %s, Wiederanlaufwert: %d', v_stichtag, v_wiederanlaufWert));

        CALL project.dataset.k_ausd_bp_ta_iccid_vertrag(v_job_entry_nr, v_stichtag, v_wiederanlaufWert);

        INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'INFO', 'Core procedure k_ausd_bp_ta_iccid_vertrag completed successfully.');

        -- Update job_control status to OK
        UPDATE project.dataset.job_control
        SET end_timestamp = CURRENT_TIMESTAMP(), status = 'OK'
        WHERE job_entry_nr = v_job_entry_nr;

    EXCEPTION WHEN ERROR THEN
        -- Log execution error
        INSERT INTO project.dataset.job_error_log (error_timestamp, job_entry_nr, error_message, stack_trace, stichtag, wiederanlaufwert)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, @@ERROR.MESSAGE, @@ERROR.STACKTRACE, v_stichtag, v_wiederanlaufWert);

        INSERT INTO project.dataset.job_run_log (log_timestamp, job_entry_nr, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_job_entry_nr, 'ERROR', FORMAT('Job failed: %s. Error: %s', v_job_name, @@ERROR.MESSAGE));

        -- Update job_control status to ERROR
        UPDATE project.dataset.job_control
        SET end_timestamp = CURRENT_TIMESTAMP(), status = 'ERROR'
        WHERE job_entry_nr = v_job_entry_nr;

        RAISE; -- Re-raise the error to terminate the wrapper procedure with an error status
    END;

END;