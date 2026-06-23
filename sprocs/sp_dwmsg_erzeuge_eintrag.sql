-- Stored procedure to create a job entry and update job status
-- Replaces DWMSG_erzeuge_Eintrag function from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_dwmsg_erzeuge_eintrag`(
    IN p_job_id STRING,
    IN p_run_id STRING,
    IN p_job_name STRING,
    IN p_message STRING,
    IN p_status STRING
)
BEGIN
    INSERT INTO `project.dataset.dw_job_entries` (
        job_id, run_id, job_name, start_timestamp, status, message
    )
    VALUES (
        p_job_id, p_run_id, p_job_name, CURRENT_TIMESTAMP(), p_status, p_message
    );

    MERGE INTO `project.dataset.dw_job_status` AS T
    USING (SELECT p_job_id AS job_id, p_run_id AS run_id) AS S
    ON T.job_id = S.job_id AND T.run_id = S.run_id
    WHEN MATCHED THEN
        UPDATE SET status = p_status, last_update_timestamp = CURRENT_TIMESTAMP(), last_message = p_message
    WHEN NOT MATCHED THEN
        INSERT (job_id, run_id, status, last_update_timestamp, last_message)
        VALUES (p_job_id, p_run_id, p_status, CURRENT_TIMESTAMP(), p_message);
END;