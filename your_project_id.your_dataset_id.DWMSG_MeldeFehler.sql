-- Target for: Helper Stored Procedures
-- Legacy Source: f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_MeldeFehler`(
  p_job_id STRING,
  p_entry_nr INT64,
  p_error_code STRING,
  p_error_message STRING,
  p_stack_trace STRING
)
BEGIN
  INSERT INTO `your_project_id.your_dataset_id.job_error_log` (
    job_id,
    entry_nr,
    error_code,
    error_message,
    stack_trace,
    timestamp
  )
  VALUES (
    p_job_id,
    p_entry_nr,
    p_error_code,
    p_error_message,
    p_stack_trace,
    CURRENT_TIMESTAMP()
  );

  -- Also log to general job log as an ERROR
  INSERT INTO `your_project_id.your_dataset_id.job_log` (
    job_id,
    entry_nr,
    log_level,
    message,
    timestamp
  )
  VALUES (
    p_job_id,
    p_entry_nr,
    'ERROR',
    p_error_message,
    CURRENT_TIMESTAMP()
  );
END;