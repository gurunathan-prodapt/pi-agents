-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
--
-- Migrated control script k_ausd_v_ta_cntrct_valid.ksh to a BigQuery Stored Procedure.
-- This procedure handles parameter validation, job state management, and orchestrates
-- the execution of the core data processing logic.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.r_ausd_vertrag_control`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    DECLARE v_job_status STRING DEFAULT 'RUNNING';
    DECLARE v_error_message STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_is_job_active BOOL;

    -- Log job start
    INSERT INTO `my_project.my_dataset.job_log` (log_time, job_id, entry_number, message, severity)
    VALUES (v_current_timestamp, p_JobKennung, p_EintragsNr, 'Job execution started.', 'INFO');

    BEGIN
        -- Parameter Validation (mimicking pruefeParameterGesetzt)
        -- The ksh script used ErrNr=193 for missing arguments.
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            RAISE USING MESSAGE = 'Parameter validation failed: p_JobKennung is mandatory.';
        END IF;
        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            RAISE USING MESSAGE = 'Parameter validation failed: p_EintragsNr is mandatory.';
        END IF;

        -- Check for active jobs and ignore if already running (mimicking "aktive Jobs werden ignoriert")
        SELECT
            COUNT(*) > 0
        INTO
            v_is_job_active
        FROM
            `my_project.my_dataset.job_table`
        WHERE
            job_id = p_JobKennung AND entry_number = p_EintragsNr AND is_active = TRUE;

        IF v_is_job_active THEN
            SET v_job_status = 'IGNORED';
            INSERT INTO `my_project.my_dataset.job_log` (log_time, job_id, entry_number, message, severity)
            VALUES (v_current_timestamp, p_JobKennung, p_EintragsNr, 'Job ignored, an active instance is already running.', 'WARNING');
            -- Exit the procedure early as per original script's intent to ignore active jobs
            RETURN;
        END IF;

        -- Deactivate older active jobs (mimicking "alte aktive Jobs werden einfach dekativiert")
        UPDATE `my_project.my_dataset.job_table`
        SET
            is_active = FALSE,
            status = 'DEACTIVATED',
            end_time = v_current_timestamp,
            last_updated = v_current_timestamp
        WHERE
            job_id = p_JobKennung
            AND is_active = TRUE;

        -- Register current job as active
        INSERT INTO `my_project.my_dataset.job_table` (job_id, entry_number, start_time, status, is_active, last_updated)
        VALUES (p_JobKennung, p_EintragsNr, v_current_timestamp, 'RUNNING', TRUE, v_current_timestamp);

        -- Execute the core SQL script
        CALL `my_project.my_dataset.d_ausd_v_ta_cntrct_valid`(p_EintragsNr, v_records_processed);

        SET v_job_status = 'COMPLETED';

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = CONCAT('ERROR: ', @@error.message);

        INSERT INTO `my_project.my_dataset.job_log` (log_time, job_id, entry_number, message, severity)
        VALUES (CURRENT_TIMESTAMP(), p_JobKennung, p_EintragsNr, v_error_message, 'ERROR');

    FINALLY
        -- Update job_table with final status and record count
        UPDATE `my_project.my_dataset.job_table`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = v_job_status,
            record_count = v_records_processed,
            error_message = v_error_message,
            is_active = FALSE, -- Mark as inactive once completed or failed
            last_updated = CURRENT_TIMESTAMP()
        WHERE
            job_id = p_JobKennung AND entry_number = p_EintragsNr AND start_time = v_current_timestamp; -- Use start_time to uniquely identify the current run

        INSERT INTO `my_project.my_dataset.job_log` (log_time, job_id, entry_number, message, severity)
        VALUES (CURRENT_TIMESTAMP(), p_JobKennung, p_EintragsNr, CONCAT('Job execution finished with status: ', v_job_status, '. Records processed: ', CAST(v_records_processed AS STRING)), 'INFO');

    END;
END;