-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure to log an error message for a job run.
-- Replaces functionality from f_alis_msgerr.ksh/DWMSG_MeldeFehler.
CREATE OR REPLACE PROCEDURE project.dataset.dwmsg_meldefehler(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_error_message STRING,
    IN p_component STRING DEFAULT 'WRAPPER',
    IN p_error_code STRING DEFAULT 'GENERIC_ERROR'
)
BEGIN
    INSERT INTO project.dataset.job_error_log (job_kennung, entry_nr, log_timestamp, error_message, component, error_code)
    VALUES (p_job_kennung, p_entry_nr, CURRENT_TIMESTAMP(), p_error_message, p_component, p_error_code);
END;