-- DDL for job_audit_log table
-- Legacy Source: r_ausd_v_ta_inv_def.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_audit_log` (
  run_timestamp TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
  job_id STRING OPTIONS(description="Identifier for the job"),
  entry_number INT64 OPTIONS(description="Unique entry number for a job run (DW_EintragsNr)"),
  log_level STRING OPTIONS(description="Level of the log (INFO, ERROR, WARNING)"),
  message STRING OPTIONS(description="Log message content"),
  error_code INT64 OPTIONS(description="Error code if applicable"),
  error_argument STRING OPTIONS(description="Argument related to the error code"),
  log_file_name STRING OPTIONS(description="Name of the log file in legacy system")
)
PARTITION BY
  DATE(run_timestamp)
CLUSTER BY
  job_id, entry_number;