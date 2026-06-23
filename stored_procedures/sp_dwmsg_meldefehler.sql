-- Stored procedure for reporting errors
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_MeldeFehler)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_meldefehler`(
  p_entry_number INT64,
  p_job_id STRING,
  p_log_level STRING, -- 'E' for Error
  p_error_code INT64,
  p_error_argument STRING
)
BEGIN
  DECLARE v_message STRING;
  SET v_message = CONCAT('ERROR_CODE: ', CAST(p_error_code AS STRING), ', ARG: ', p_error_argument);

  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message, error_code, error_argument)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, p_log_level, v_message, p_error_code, p_error_argument);
END;