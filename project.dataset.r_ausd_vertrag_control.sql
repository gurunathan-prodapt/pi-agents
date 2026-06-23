-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

-- Description: This BigQuery Stored Procedure migrates the orchestration logic of k_ausd_geschaeftspartner.ksh.
-- It handles parameter parsing, validation, job status management using a `job_log` table,
-- and orchestrates the execution of the core data transformation SP (`d_ausd_geschaeftspartner_sp`).
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING, -- Expected DDMMYYYY
    IN p_wiederanlaufWert INT64 -- Not directly used in this SP, passed for compatibility
)
BEGIN
    DECLARE v_stichtag_date DATE;
    DECLARE v_records_count INT64;
    DECLARE v_status STRING DEFAULT 'STARTED';
    DECLARE v_message STRING DEFAULT 'Job started successfully.';
    DECLARE v_tab_name STRING DEFAULT 'sof_ta_p_gesch_part'; -- Main output table of the core SP
    DECLARE v_active_job_count INT64;

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_status = 'FAILED';
        SET v_message = 'JobKennung parameter is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        SET v_status = 'FAILED';
        SET v_message = 'Stichtag parameter is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Validate and parse p_Stichtag from DDMMYYYY string to DATE type
    BEGIN
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Invalid Stichtag date format: ', p_Stichtag, '. Expected DDMMYYYY.');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END;

    -- Check for active jobs (mimics "ignore active jobs" from original script)
    SELECT COUNT(*)
    INTO v_active_job_count
    FROM `project.dataset.job_log`
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr -- Assuming EintragsNr helps identify a specific run instance
      AND stichtag = v_stichtag_date
      AND status = 'STARTED'; -- Or 'RUNNING' if that's a defined status in job_log

    IF v_active_job_count > 0 THEN
        SET v_message = CONCAT('Job ', p_JobKennung, ' for EintragsNr ', p_EintragsNr, ' and Stichtag ', p_Stichtag, ' is already active. Skipping execution.');
        -- Log this skipping event
        INSERT INTO `project.dataset.job_log`
        (job_kennung, eintrags_nr, tab_name, stichtag, status, message)
        VALUES
        (p_JobKennung, p_EintragsNr, v_tab_name, v_stichtag_date, 'SKIPPED', v_message);
        RETURN; -- Exit the procedure gracefully
    END IF;

    -- Log job start
    INSERT INTO `project.dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, stichtag, status, message)
    VALUES
    (p_JobKennung, p_EintragsNr, v_tab_name, v_stichtag_date, v_status, v_message);

    BEGIN
        -- Call the core SQL processing stored procedure
        CALL `project.dataset.d_ausd_geschaeftspartner_sp`(v_stichtag_date, v_records_count);

        SET v_status = 'COMPLETED';
        SET v_message = CONCAT('Job completed successfully. Processed records: ', CAST(v_records_count AS STRING));

        -- Update job_log with completion status and record count
        UPDATE `project.dataset.job_log`
        SET status = v_status, record_count = v_records_count, message = v_message, created_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND stichtag = v_stichtag_date AND status = 'STARTED';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Job failed during processing: ', @@error.message);

        -- Update job_log with failure status
        UPDATE `project.dataset.job_log`
        SET status = v_status, message = v_message, created_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND stichtag = v_stichtag_date AND status = 'STARTED';

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message; -- Re-raise the error to the caller
    END;

END;