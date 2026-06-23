-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to create an initial log entry for the job.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
    IN p_job_key STRING,
    IN p_message STRING,
    IN p_log_level STRING,
    IN p_status STRING
)
BEGIN
    DECLARE v_next_job_entry_nr INT64;

    CALL `your_project.your_dataset.DWMSG_ErmittleNr`(v_next_job_entry_nr);

    INSERT INTO `your_project.your_dataset.job_logging_table` (job_entry_nr, job_key, log_level, message, created_at, status)
    VALUES (v_next_job_entry_nr, p_job_key, p_log_level, p_message, CURRENT_TIMESTAMP(), p_status);
END;