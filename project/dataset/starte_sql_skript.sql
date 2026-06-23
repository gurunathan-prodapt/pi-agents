-- BigQuery Stored Procedure for job control wrapper logic.
-- Implements the logic for deactivating older jobs, checking for active jobs,
-- and calling the core SQL procedure `d_ausd_v_ta_apn_ve`.
-- Replaces parts of k_ausd_v_ta_apn_ve.ksh for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE OR REPLACE PROCEDURE project.dataset.starte_sql_skript(
    p_EintragsNr STRING,
    p_JobKennung STRING,
    OUT records_processed INT64,
    OUT job_status STRING
)
BEGIN
    DECLARE v_active_job_count INT64;
    DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_sql_records_processed INT64;

    SET records_processed = 0;
    SET job_status = 'FAILED';

    -- Deactivate older active jobs (logic based on description)
    UPDATE project.dataset.job_table
    SET status = 'INACTIVE',
        end_timestamp = v_current_timestamp,
        last_updated = v_current_timestamp
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr <> p_EintragsNr -- Deactivate others for this job_kennung
      AND status = 'ACTIVE'
      AND end_timestamp IS NULL; -- Only truly active ones

    -- Check for actively running jobs with the same JobKennung and EintragsNr
    SELECT COUNT(*)
    INTO v_active_job_count
    FROM project.dataset.job_table
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND status IN ('ACTIVE', 'RUNNING'); -- Consider a "RUNNING" state

    IF v_active_job_count > 0 THEN
        -- Job is already active, ignore as per original script's purpose
        SET job_status = 'IGNORED';
        RETURN; -- Exit procedure
    END IF;

    -- Mark current job as ACTIVE/RUNNING
    -- Upsert logic: if exists, update; else, insert
    MERGE INTO project.dataset.job_table AS T
    USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintrags_nr) AS S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET
            status = 'RUNNING',
            start_timestamp = v_current_timestamp,
            end_timestamp = NULL,
            last_updated = v_current_timestamp
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, status, start_timestamp, end_timestamp, last_updated)
        VALUES (S.job_kennung, S.eintrags_nr, 'RUNNING', v_current_timestamp, NULL, v_current_timestamp);

    BEGIN
        -- Call the core SQL processing procedure
        CALL project.dataset.d_ausd_v_ta_apn_ve(p_EintragsNr, p_JobKennung, v_sql_records_processed);
        SET records_processed = v_sql_records_processed;
        SET job_status = 'COMPLETED';

        -- Update job status to COMPLETED
        UPDATE project.dataset.job_table
        SET status = 'COMPLETED',
            end_timestamp = CURRENT_TIMESTAMP(),
            last_updated = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND status = 'RUNNING'; -- Only if it was running

    EXCEPTION WHEN ERROR THEN
        SET job_status = 'FAILED';
        -- Update job status to FAILED in case of error
        UPDATE project.dataset.job_table
        SET status = 'FAILED',
            end_timestamp = CURRENT_TIMESTAMP(),
            last_updated = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND status = 'RUNNING'; -- Only if it was running
        RAISE; -- Re-raise the error to propagate
    END;

END;