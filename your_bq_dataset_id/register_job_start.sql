-- Helper procedure for job registration
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_id.register_job_start`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    -- Deactivate any previously active job for this job_kennung to ensure single active instance
    UPDATE `your_gcp_project_id.your_bq_dataset_id.job_table`
    SET is_active = FALSE, last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung AND is_active = TRUE;

    -- Insert/Update job_table to mark current job as active
    MERGE INTO `your_gcp_project_id.your_bq_dataset_id.job_table` T
    USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintrags_nr) S
    ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
    WHEN MATCHED THEN
        UPDATE SET is_active = TRUE, last_update_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintrags_nr, is_active, last_update_timestamp)
        VALUES (S.job_kennung, S.eintrags_nr, TRUE, CURRENT_TIMESTAMP());

    -- Insert record into job_run_log for this execution
    INSERT INTO `your_gcp_project_id.your_bq_dataset_id.job_run_log`
        (job_kennung, eintrags_nr, start_timestamp, status)
    VALUES
        (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'RUNNING');
END;