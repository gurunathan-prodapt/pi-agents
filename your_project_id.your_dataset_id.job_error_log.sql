-- Target for: Logging and Status Tables (DDL)
-- Legacy Source: N/A (new BigQuery component)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log` (
  job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
  entry_nr INT64 NOT NULL OPTIONS(description="Entry number for the specific job run"),
  error_code STRING OPTIONS(description="Specific error code, if available"),
  error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
  stack_trace STRING OPTIONS(description="Stack trace or additional error context"),
  timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of when the error occurred")
);