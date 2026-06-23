-- BigQuery Stored Procedure: project.dataset.erzeugung_abzug_rechnungsdaten
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.erzeugung_abzug_rechnungsdaten`(
    IN p_input_stichtag STRING, -- Input as string to handle potential empty/null for fallback
    IN p_input_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'AURD_RECHSTAN'; -- Job identifier from original script context
    DECLARE v_sysdate_str STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_fehler_nr INT64 DEFAULT 0; -- Placeholder, derived from DWMSG_ErmittleNr logic if needed.

    -- Determine v_sysdate (current date formatted as 'DDMMYYYY')
    SET v_sysdate_str = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Determine p_stichtag with fallback logic if not provided
    IF p_input_stichtag IS NULL OR p_input_stichtag = '' THEN
        -- Fallback: LEAST(CURRENT_DATE(), MAX(ladedatum)) from the relevant source table
        -- NOTE: Replace `project.dataset.source_table` with the actual source table name containing `ladedatum`.
        SELECT LEAST(CURRENT_DATE(), MAX(ladedatum)) INTO v_stichtag
        FROM `project.dataset.source_table`;

        -- If source table is empty or has no ladedatum, default to current date
        IF v_stichtag IS NULL THEN
            SET v_stichtag = CURRENT_DATE();
        END IF;

        INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
        VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Stichtag not provided, falling back to: ' || FORMAT_DATE('%Y-%m-%d', v_stichtag), v_stichtag, p_input_wiederanlaufWert, 'PARAM_FALLBACK');
    ELSE
        -- Attempt to parse the provided Stichtag string
        BEGIN
            SET v_stichtag = PARSE_DATE('%d%m%Y', p_input_stichtag);
        EXCEPTION WHEN ERROR THEN
            -- Handle invalid date format
            INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
            VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'ERROR', 'Invalid Stichtag format provided: ' || p_input_stichtag || '. Expected DDMMYYYY.', NULL, p_input_wiederanlaufWert, 'PARAM_ERROR');
            RAISE ERROR 'Invalid Stichtag format provided. Expected DDMMYYYY.';
        END;
    END IF;

    -- Log job start and parameters
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Job erzeugung_abzug_rechnungsdaten started with Stichtag: ' || FORMAT_DATE('%Y-%m-%d', v_stichtag) || ' and Wiederanlaufwert: ' || p_input_wiederanlaufWert, v_stichtag, p_input_wiederanlaufWert, 'STARTED');

    -- Update job status to RUNNING (or insert if not exists)
    MERGE INTO `project.dataset.job_status` T
    USING (SELECT v_job_kennung AS job_id) S
    ON T.job_id = S.job_id
    WHEN MATCHED THEN
        UPDATE SET
            last_run_timestamp = CURRENT_TIMESTAMP(),
            overall_status = 'RUNNING',
            last_stichtag = v_stichtag,
            last_wiederanlauf_wert = p_input_wiederanlaufWert
    WHEN NOT MATCHED THEN
        INSERT (job_id, last_run_timestamp, overall_status, last_stichtag, last_wiederanlauf_wert)
        VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'RUNNING', v_stichtag, p_input_wiederanlaufWert);

    -- Call the core processing stored procedure
    CALL `project.dataset.k_aurd_rechstan`(v_job_kennung, v_stichtag, v_fehler_nr, p_input_wiederanlaufWert);

    -- Log job completion
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'INFO', 'Job erzeugung_abzug_rechnungsdaten completed successfully.', v_stichtag, p_input_wiederanlaufWert, 'COMPLETED');

    -- Update final job status to OK
    UPDATE `project.dataset.job_status`
    SET
        overall_status = 'OK',
        last_run_timestamp = CURRENT_TIMESTAMP(),
        last_stichtag = v_stichtag,
        last_wiederanlauf_wert = p_input_wiederanlaufWert
    WHERE job_id = v_job_kennung;

EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `project.dataset.job_log` (job_id, log_timestamp, log_level, message, stichtag, wiederanlauf_wert, status)
    VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'ERROR', 'Job erzeugung_abzug_rechnungsdaten failed: ' || ERROR_MESSAGE(), v_stichtag, p_input_wiederanlaufWert, 'FAILED');

    -- Update job status to FAILED
    MERGE INTO `project.dataset.job_status` T
    USING (SELECT v_job_kennung AS job_id) S
    ON T.job_id = S.job_id
    WHEN MATCHED THEN
        UPDATE SET
            last_run_timestamp = CURRENT_TIMESTAMP(),
            overall_status = 'FAILED',
            last_stichtag = v_stichtag,
            last_wiederanlauf_wert = p_input_wiederanlaufWert
    WHEN NOT MATCHED THEN
        INSERT (job_id, last_run_timestamp, overall_status, last_stichtag, last_wiederanlauf_wert)
        VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'FAILED', v_stichtag, p_input_wiederanlaufWert);
    RAISE; -- Re-raise the error to propagate it.
END;