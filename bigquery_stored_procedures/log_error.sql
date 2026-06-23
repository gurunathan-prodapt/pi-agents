-- Helper Stored Procedure for logging errors
-- This procedure is called by r_ausd_vertrag_control to log detailed errors.

CREATE OR REPLACE PROCEDURE `project.dataset.log_error`(
    p_jobkennunG STRING,
    p_eintrags_nr STRING,
    p_error_code STRING,
    p_error_message STRING,
    p_severity STRING,
    p_source_script STRING
)
BEGIN
    INSERT INTO `project.dataset.error_log` (log_time, job_kennung, eintrags_nr, error_code, error_message, severity, source_script)
    VALUES (CURRENT_TIMESTAMP(), p_jobkennung, p_eintrags_nr, p_error_code, p_error_message, p_severity, p_source_script);

    -- Also update the job_table if the job failed
    UPDATE `project.dataset.job_table`
    SET status = 'FAILED', end_time = CURRENT_TIMESTAMP(), message = p_error_message
    WHERE job_kennung = p_jobkennung AND eintrags_nr = p_eintrags_nr AND status = 'RUNNING';
END;