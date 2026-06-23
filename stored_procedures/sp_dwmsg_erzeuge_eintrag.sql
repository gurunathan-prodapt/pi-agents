-- Stored procedure for creating a job entry in the audit log
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_ErzeugeEintrag)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_erzeuge_eintrag`(
  p_entry_number INT64,
  p_job_id STRING,
  p_program_name STRING,
  p_log_file_name STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message, log_file_name)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, 'INFO', CONCAT('Job started: ', p_program_name), p_log_file_name);
END;