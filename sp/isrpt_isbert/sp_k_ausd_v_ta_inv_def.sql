-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.isrpt_isbert.sp_k_ausd_v_ta_inv_def`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
OPTIONS(
    description="Migrated KornShell orchestration logic from k_ausd_v_ta_inv_def.ksh to BigQuery. Handles parameters, job control, and calls sp_d_ausd_v_ta_inv_def."
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_v_ta_inv_def';
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_records_processed INT64;
    DECLARE v_datum_str STRING;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';
    SET v_message = 'Job started.';

    -- Parameter validation (replaces h_alis_parameter.ksh and pruefeParameterGesetzt)
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_status = 'FAILED';
        SET v_message = 'FEHLER: Jobkennung parameter is missing or empty.';
        INSERT INTO `my_gcp_project.isrpt_isbert.job_status_log`
        (job_name, job_kennung, entry_number, status, start_time, end_time, message)
        VALUES(v_job_name, p_JobKennung, p_EintragsNr, v_status, v_start_time, CURRENT_TIMESTAMP(), v_message);
        RAISE ERROR(v_message);
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_status = 'FAILED';
        SET v_message = 'FEHLER: EintragsNr parameter is missing or empty.';
        INSERT INTO `my_gcp_project.isrpt_isbert.job_status_log`
        (job_name, job_kennung, entry_number, status, start_time, end_time, message)
        VALUES(v_job_name, p_JobKennung, p_EintragsNr, v_status, v_start_time, CURRENT_TIMESTAMP(), v_message);
        RAISE ERROR(v_message);
    END IF;

    -- Job Management: Ignore active jobs
    -- Checks if another job with the same JobKennung is currently running.
    IF EXISTS (SELECT 1 FROM `my_gcp_project.isrpt_isbert.job_status_log` WHERE job_kennung = p_JobKennung AND status = 'RUNNING') THEN
        SET v_status = 'IGNORED';
        SET v_message = 'Job with Jobkennung ' || p_JobKennung || ' is already running. Ignoring current invocation.';
        INSERT INTO `my_gcp_project.isrpt_isbert.job_status_log`
        (job_name, job_kennung, entry_number, status, start_time, end_time, message)
        VALUES(v_job_name, p_JobKennung, p_EintragsNr, v_status, v_start_time, CURRENT_TIMESTAMP(), v_message);
        RETURN; -- Exit procedure cleanly
    END IF;

    -- Job Management: Deactivate old active jobs
    -- Marks any previously 'RUNNING' jobs for the same job and job_kennung as 'DEACTIVATED'.
    UPDATE `my_gcp_project.isrpt_isbert.job_status_log`
    SET
        status = 'DEACTIVATED',
        end_time = CURRENT_TIMESTAMP(),
        message = 'Deactivated by new job invocation due to a new run starting.'
    WHERE
        job_name = v_job_name
        AND job_kennung = p_JobKennung
        AND status = 'RUNNING';

    -- Insert current job as RUNNING before execution
    INSERT INTO `my_gcp_project.isrpt_isbert.job_status_log`
    (job_name, job_kennung, entry_number, status, start_time, message)
    VALUES(v_job_name, p_JobKennung, p_EintragsNr, v_status, v_start_time, v_message);

    BEGIN
        -- Derive v_datum_str (similar to the logic in d_ausd_v_ta_inv_def.sql, using BigQuery date functions)
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        INTO v_datum_str
        FROM `my_gcp_project.isrpt_isbert.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        IF v_datum_str IS NULL THEN
            SET v_datum_str = '19000101'; -- Fallback if no records match
        END IF;

        -- Call the SQL transformation stored procedure (replaces starteSQLSkript)
        CALL `my_gcp_project.isrpt_isbert.sp_d_ausd_v_ta_inv_def`(v_datum_str, v_records_processed);

        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully. Records processed: ' || COALESCE(v_records_processed, 0);

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = 'Job failed: ' || @@error.message;
        -- Re-raise the error to propagate it to any calling orchestrator or for debugging.
        RAISE;
    FINALLY
        -- Always update the log entry for the current job invocation, regardless of success or failure.
        -- This ensures the job status is always finalized.
        UPDATE `my_gcp_project.isrpt_isbert.job_status_log`
        SET
            status = v_status,
            end_time = CURRENT_TIMESTAMP(),
            records_processed = COALESCE(v_records_processed, 0),
            message = v_message
        WHERE
            job_name = v_job_name
            AND job_kennung = p_JobKennung
            AND entry_number = p_EintragsNr
            AND start_time = v_start_time; -- Crucial to update the correct specific run instance
    END;
END;