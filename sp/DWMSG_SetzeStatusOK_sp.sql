-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP to set job status to SUCCESS.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_SetzeStatusOK_sp`(
  p_job_nr INT64
)
BEGIN
  UPDATE `project_id.dataset_id.job_log_table`
  SET status = 'SUCCESS',
      message = COALESCE(message, '') || '\nJob completed successfully.',
      created_at = CURRENT_TIMESTAMP()
  WHERE job_nr = p_job_nr
    AND status = 'RUNNING';
  
  INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, created_at, message, status)
  VALUES (p_job_nr, CURRENT_TIMESTAMP(), 'Job status set to OK.', 'SUCCESS_REPORTED');
END;