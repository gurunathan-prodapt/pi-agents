-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure to record a status update for a job run.
-- Replaces functionality from f_alis_msgerr.ksh/DWMSG_SetzeStatusOK.
CREATE OR REPLACE PROCEDURE project.dataset.dwmsg_setzestatusok(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_status STRING,
    IN p_message STRING DEFAULT NULL
)
BEGIN
    INSERT INTO project.dataset.job_status (job_kennung, entry_nr, log_timestamp, status, message)
    VALUES (p_job_kennung, p_entry_nr, CURRENT_TIMESTAMP(), p_status, p_message);
END;