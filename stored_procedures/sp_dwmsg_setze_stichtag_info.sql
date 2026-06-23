-- Stored procedure for setting the reference date information
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_SetzeStichtagInfo)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_setze_stichtag_info`(
  p_entry_number INT64,
  p_job_id STRING,
  p_reference_date STRING,
  p_date_format STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, 'INFO', CONCAT('Reference Date Set: ', p_reference_date, ' (Format: ', p_date_format, ')'));
END;