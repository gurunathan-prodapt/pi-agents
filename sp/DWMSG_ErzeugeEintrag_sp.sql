-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP to create an initial log entry for a job.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_ErzeugeEintrag_sp`(
  p_job_nr INT64,
  p_job_kennung STRING,
  p_program_name STRING,
  p_log_file STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at, status)
  VALUES (p_job_nr, p_job_kennung, p_log_file, CONCAT('Job started: ', p_program_name), CURRENT_TIMESTAMP(), 'RUNNING');
END;