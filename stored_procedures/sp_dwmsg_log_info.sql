-- Stored procedure for logging informational messages
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_log_info)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_log_info`(
  p_entry_number INT64,
  p_job_id STRING,
  p_message STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, 'INFO', p_message);
END;