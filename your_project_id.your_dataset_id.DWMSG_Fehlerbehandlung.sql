-- Target for: Helper Stored Procedures
-- Legacy Source: f_alis_msgerr.ksh (error handling framework)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_Fehlerbehandlung`(
  p_job_id STRING,
  p_entry_nr INT64,
  p_error_code STRING,
  p_error_message STRING,
  p_stack_trace STRING
)
BEGIN
  -- Log the detailed error
  CALL `your_project_id.your_dataset_id.DWMSG_MeldeFehler`(
    p_job_id,
    p_entry_nr,
    p_error_code,
    p_error_message,
    p_stack_trace
  );

  -- Update job status to FAILED in job_table
  UPDATE `your_project_id.your_dataset_id.job_table`
  SET
    status = 'FAILED',
    end_time = CURRENT_TIMESTAMP()
  WHERE job_id = p_job_id AND entry_nr = p_entry_nr;

  -- Optionally raise an error to stop execution in the calling context
  RAISE; -- Re-raises the last error encountered
END;