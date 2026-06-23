-- Helper procedure for logging runtime messages
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_log_runtime_message(
    IN p_eintragsnr INT64,
    IN p_jobkennung STRING,
    IN p_log_level STRING,
    IN p_message STRING
)
OPTIONS(
    description="Helper procedure to insert messages into the job_runtime_log table."
)
BEGIN
    INSERT INTO project.dataset.job_runtime_log (eintragsnr, job_kennung, log_level, message, log_ts)
    VALUES (p_eintragsnr, p_jobkennung, p_log_level, p_message, CURRENT_TIMESTAMP());
END;