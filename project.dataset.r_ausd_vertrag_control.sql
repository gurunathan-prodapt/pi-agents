-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.r_ausd_vertrag_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_records INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_run_id STRING;

    -- Generate a unique run ID
    SET v_run_id = GENERATE_UUID();
    SET v_start_timestamp = CURRENT_TIMESTAMP();

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_EintragsNr IS NULL THEN
        INSERT INTO `project_id.dataset_id.job_error_log` (error_timestamp, job_kennung, eintrags_nr, error_message)
        VALUES (CURRENT_TIMESTAMP(), p_JobKennung, p_EintragsNr, 'ERROR: Missing required parameters (p_JobKennung or p_EintragsNr).');
        SELECT FORMAT("ERROR: Missing required parameters: p_JobKennung='%t', p_EintragsNr='%t'", p_JobKennung, p_EintragsNr);
        RETURN;
    END IF;

    -- Job Management: Deactivate older active jobs
    UPDATE `project_id.dataset_id.job_table`
    SET
        active_flag = FALSE,
        last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE
        job_kennung = p_JobKennung
        AND eintrags_nr <> p_EintragsNr
        AND active_flag = TRUE;

    -- Job Management: Insert/Update current job
    MERGE INTO `project_id.dataset_id.job_table` AS T
    USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintrags_nr, TRUE AS active_flag) AS S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET
            T.active_flag = S.active_flag,
            T.last_update_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, active_flag, last_update_timestamp)
        VALUES (S.job_kennung, S.eintrags_nr, S.active_flag, CURRENT_TIMESTAMP());

    -- Execute the core SQL logic
    -- The core SQL logic was migrated from d_ausd_v_ta_inv_def.sql
    CALL `project_id.dataset_id.d_ausd_v_ta_inv_def`();

    -- Get record count after execution
    -- Assuming ta_inv_def_result is the target table where the previous SQL wrote its output
    SELECT COUNT(*) INTO v_records FROM `project_id.dataset_id.ta_inv_def_result`;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Log job run details
    INSERT INTO `project_id.dataset_id.job_run_log` (run_id, start_timestamp, end_timestamp, job_kennung, eintrags_nr, records_processed, status)
    VALUES (v_run_id, v_start_timestamp, v_end_timestamp, p_JobKennung, p_EintragsNr, v_records, 'SUCCESS');

    SELECT FORMAT("---------- ENDE Datenverarbeitung ---------- Processed %d records for JobKennung: %s, EintragsNr: %s", v_records, p_JobKennung, p_EintragsNr);

EXCEPTION WHEN ERROR THEN
    SET v_end_timestamp = CURRENT_TIMESTAMP();
    INSERT INTO `project_id.dataset_id.job_error_log` (error_timestamp, job_kennung, eintrags_nr, error_message)
    VALUES (CURRENT_TIMESTAMP(), p_JobKennung, p_EintragsNr, ERROR_MESSAGE());
    INSERT INTO `project_id.dataset_id.job_run_log` (run_id, start_timestamp, end_timestamp, job_kennung, eintrags_nr, records_processed, status)
    VALUES (v_run_id, v_start_timestamp, v_end_timestamp, p_JobKennung, p_EintragsNr, NULL, 'FAILED');
    SELECT FORMAT("ERROR: Job failed for JobKennung: %s, EintragsNr: %s. Message: %s", p_JobKennung, p_EintragsNr, ERROR_MESSAGE());
END;