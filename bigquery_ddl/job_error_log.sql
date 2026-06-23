-- DDL for BigQuery table: your_project_id.your_dataset_id.job_error_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log`
(
  job_nr INT64 OPTIONS(description="Unique job run number, linking to job_audit_log"),
  job_kennung STRING OPTIONS(description="Identifier for the type of job (e.g., 'ausd_bp_ta_apn_carmen')"),
  error_nr INT64 OPTIONS(description="Numeric error code"),
  error_arg STRING OPTIONS(description="Argument or context related to the error (e.g., parameter name)"),
  log_ts TIMESTAMP OPTIONS(description="Timestamp when the error was logged"),
  message STRING OPTIONS(description="Detailed error message")
)
PARTITION BY DATE(log_ts)
CLUSTER BY job_kennung, error_nr
OPTIONS(
  description="Log of errors encountered during job executions."
);