-- Helper procedure for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Purpose: Centralizes logging into job_log table.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_log_job_event`(
  IN p_job_nr INT64,
  IN p_job_name STRING,
  IN p_job_status STRING,
  IN p_stichtag STRING,
  IN p_restart_value INT64,
  IN p_message STRING
)
BEGIN
  INSERT INTO `project.dataset.job_log` (
    job_nr,
    job_name,
    job_status,
    log_ts,
    stichtag,
    restart_value,
    message
  )
  VALUES (
    p_job_nr,
    p_job_name,
    p_job_status,
    CURRENT_TIMESTAMP(),
    p_stichtag,
    p_restart_value,
    p_message
  );
END;