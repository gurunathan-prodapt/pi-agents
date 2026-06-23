-- Stored procedure for setting job status to OK
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (implicit success)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_setze_status_ok`(
  p_entry_number INT64,
  p_job_id STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_audit_log` (run_timestamp, job_id, entry_number, log_level, message)
  VALUES (CURRENT_TIMESTAMP(), p_job_id, p_entry_number, 'INFO', 'Job completed successfully.');

  -- Optionally, update a status for the entry_number to 'SUCCESS' in a job status table.
END;