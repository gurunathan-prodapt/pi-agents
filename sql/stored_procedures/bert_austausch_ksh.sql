-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Legacy Source: r_ausd_austausch.ksh, k_ausd_austausch.ksh

-- Placeholder for your project and dataset
-- REPLACE WITH YOUR ACTUAL PROJECT AND DATASET

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.BERT_AUSTAUSCH_KSH`(
    IN p_stichtag_raw STRING,        -- Input stichtag in DDMMYYYY format, optional
    IN p_wiederanlauf_wert_raw STRING -- Input restart value, optional
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'BERT_AUSTAUSCH_KSH';
    DECLARE v_job_nr STRING;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlauf_wert INT64;
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_error_message STRING;

    -- Generate a unique job number (e.g., using a sequence table or UUID)
    -- This is a simplified approach; in a real scenario, you might use a more robust ID generation.
    UPDATE `my_project.my_dataset.job_sequence`
    SET current_value = current_value + 1
    WHERE sequence_name = 'BERT_AUSTAUSCH';

    SELECT current_value INTO v_job_nr
    FROM `my_project.my_dataset.job_sequence`
    WHERE sequence_name = 'BERT_AUSTAUSCH';

    -- Initialize restart value
    IF p_wiederanlauf_wert_raw IS NULL OR p_wiederanlauf_wert_raw = '' THEN
        SET v_wiederanlauf_wert = 0;
    ELSE
        SET v_wiederanlauf_wert = CAST(p_wiederanlauf_wert_raw AS INT64);
    END IF;

    -- Determine Stichtag
    IF p_stichtag_raw IS NULL OR p_stichtag_raw = '' THEN
        -- Default to current system date if not provided, mimicking h_alis_date.ksh logic
        SET v_stichtag = v_sysdate;
    ELSE
        -- Parse the input stichtag (DDMMYYYY)
        SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_raw);
    END IF;

    -- Basic parameter validation (can be extended)
    IF v_stichtag IS NULL THEN
        SET v_error_message = 'ERROR: Invalid or missing Stichtag parameter.';
        INSERT INTO `my_project.my_dataset.job_audit_log` (job_nr, job_kennung, event_type, event_ts, stichtag, restart_value, message)
        VALUES (v_job_nr, v_job_kennung, 'ERROR', CURRENT_TIMESTAMP(), v_stichtag, v_wiederanlauf_wert, v_error_message);
        RAISE ERROR v_error_message;
    END IF;

    -- Log job start
    INSERT INTO `my_project.my_dataset.job_audit_log` (job_nr, job_kennung, event_type, event_ts, stichtag, restart_value, message)
    VALUES (v_job_nr, v_job_kennung, 'INFO', CURRENT_TIMESTAMP(), v_stichtag, v_wiederanlauf_wert, CONCAT('Job started. Stichtag: ', FORMAT_DATE('%Y-%m-%d', v_stichtag), ', Restart Value: ', CAST(v_wiederanlauf_wert AS STRING)));

    -- Call the core transformation stored procedure
    CALL `my_project.my_dataset.k_ausd_austausch`(v_job_kennung, v_job_nr, v_stichtag, v_wiederanlauf_wert);

    -- Log job completion
    INSERT INTO `my_project.my_dataset.job_audit_log` (job_nr, job_kennung, event_type, event_ts, stichtag, restart_value, message)
    VALUES (v_job_nr, v_job_kennung, 'INFO', CURRENT_TIMESTAMP(), v_stichtag, v_wiederanlauf_wert, 'Job completed successfully.');

EXCEPTION WHEN ERROR THEN
    SET v_error_message = CONCAT('Job failed: ', ERROR_MESSAGE());
    INSERT INTO `my_project.my_dataset.job_audit_log` (job_nr, job_kennung, event_type, event_ts, stichtag, restart_value, message)
    VALUES (v_job_nr, v_job_kennung, 'ERROR', CURRENT_TIMESTAMP(), v_stichtag, v_wiederanlauf_wert, v_error_message);
    RAISE;
END;