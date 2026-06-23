-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP for error handling: updates job status to FAILED.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_Fehlerbehandlung_sp`(
  p_job_nr INT64
)
BEGIN
  -- Update the status of the current job in the log table
  UPDATE `project_id.dataset_id.job_log_table`
  SET status = 'FAILED',
      message = COALESCE(message, '') || '\nError: Job aborted due to failure.',
      created_at = CURRENT_TIMESTAMP()
  WHERE job_nr = p_job_nr
    AND status = 'RUNNING'; -- Only update if still running
  
  -- Also insert a specific error message
  INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, created_at, message, status)
  VALUES (p_job_nr, CURRENT_TIMESTAMP(), 'Fehlerbehandlung aktiv: Job failed.', 'ERROR_REPORTED');
END;