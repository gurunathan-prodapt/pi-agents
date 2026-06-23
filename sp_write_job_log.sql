-- BigQuery SQL Reusable helper procedure for logging for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_write_job_log`(
  IN p_job_name STRING,
  IN p_job_nr INT64,
  IN p_log_level STRING,
  IN p_message STRING,
  IN p_stichtag STRING,
  IN p_restart_value INT64
)
BEGIN
  INSERT INTO `project.dataset.job_log`
    (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
  VALUES
    (p_job_name, p_job_nr, p_log_level, p_message, p_stichtag, p_restart_value, CURRENT_TIMESTAMP());
END;