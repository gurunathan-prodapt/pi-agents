-- Stored procedure for error handling within a job
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via implicit error trap)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_fehlerbehandlung`(
  p_entry_number INT64,
  p_job_id STRING,
  p_error_message STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, 'ERROR', CONCAT('Job failed with error: ', p_error_message));

  -- Optionally, update a status for the entry_number to 'FAILED' in a job status table.
  -- For now, just logging.
END;