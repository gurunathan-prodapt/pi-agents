-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- BigQuery Stored Procedure to orchestrate the migration logic.
-- This procedure replaces the KornShell wrapper script, handling parameter
-- parsing, validation, job logging, and calling the core SQL logic.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_TabName STRING DEFAULT 'ta_vvl_upgrade';
    DECLARE v_records INT64;
    DECLARE job_start_time TIMESTAMP;
    DECLARE job_end_time TIMESTAMP;
    DECLARE current_job_id STRING;
    DECLARE error_message STRING;
    DECLARE error_code STRING;

    SET job_start_time = CURRENT_TIMESTAMP();
    SET current_job_id = GENERATE_UUID();

    -- Log job start
    INSERT INTO `project.dataset.job_table` (job_id, job_kennung, eintrags_nr, tab_name, status, created_ts, updated_ts)
    VALUES (current_job_id, p_JobKennung, p_EintragsNr, v_TabName, 'RUNNING', job_start_time, job_start_time);

    BEGIN
        -- Parameter Validation
        IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
            SET error_message = 'Parameter p_JobKennung is mandatory and cannot be empty.';
            SET error_code = 'BQS001';
            RAISE USING MESSAGE error_message;
        END IF;

        IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
            SET error_message = 'Parameter p_EintragsNr is mandatory and cannot be empty.';
            SET error_code = 'BQS002';
            RAISE USING MESSAGE error_message;
        END IF;

        -- Call the core SQL migration procedure
        CALL `project.dataset.d_ausd_v_ta_vvl_upgrade_proc`(v_records);

        -- Log job result
        SET job_end_time = CURRENT_TIMESTAMP();
        INSERT INTO `project.dataset.job_result_log` (job_kennung, eintrags_nr, tab_name, records_processed, finished_ts)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, v_records, job_end_time);

        -- Update job status to COMPLETED
        UPDATE `project.dataset.job_table`
        SET status = 'COMPLETED', updated_ts = job_end_time
        WHERE job_id = current_job_id;

        SELECT FORMAT('Job completed successfully for JobKennung: %s, EintragsNr: %s. Processed %d records.', p_JobKennung, p_EintragsNr, v_records) AS message;

    EXCEPTION WHEN ERROR THEN
        SET error_message = ERROR_MESSAGE();
        SET error_code = IFNULL(error_code, 'BQS999'); -- Use specific error_code if set, otherwise a generic one
        SET job_end_time = CURRENT_TIMESTAMP();

        -- Log error details
        INSERT INTO `project.dataset.job_error_log` (error_ts, procedure_name, error_code, error_message, job_kennung, eintrags_nr)
        VALUES (job_end_time, 'r_ausd_vertrag_control', error_code, error_message, p_JobKennung, p_EintragsNr);

        -- Update job status to FAILED
        UPDATE `project.dataset.job_table`
        SET status = 'FAILED', updated_ts = job_end_time
        WHERE job_id = current_job_id;

        SELECT FORMAT('Job failed for JobKennung: %s, EintragsNr: %s. Error: %s', p_JobKennung, p_EintragsNr, error_message) AS message;
        -- Re-raise the error to signal failure to external orchestrator
        RAISE;
    END;
END;