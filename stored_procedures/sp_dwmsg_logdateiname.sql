-- Stored procedure for constructing a log file name
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_Logdateiname)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_logdateiname`(
  OUT p_log_file_name STRING,
  p_job_id STRING,
  p_entry_number INT64
)
BEGIN
  -- In BigQuery, this simulates a log file name. Actual logs are in job_audit_log table.
  SET p_log_file_name = CONCAT(p_job_id, '_', CAST(p_entry_number AS STRING), '_log.json');
END;