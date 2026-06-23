-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure to log a general message for a job run.
-- Replaces functionality from f_alis_msgerr.ksh/DWMSG_ErzeugeEintrag.
CREATE OR REPLACE PROCEDURE project.dataset.dwmsg_erzeugeeintrag(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_log_message STRING,
    IN p_log_level STRING DEFAULT 'INFO'
)
BEGIN
    INSERT INTO project.dataset.job_log (job_kennung, entry_nr, log_timestamp, log_message, log_level)
    VALUES (p_job_kennung, p_entry_nr, CURRENT_TIMESTAMP(), p_log_message, p_log_level);
END;