-- BigQuery DDL for job_error_log table
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_error_log` (
  entry_no INT64 NOT NULL OPTIONS(description="Corresponding entry number from job_log"),
  job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job where the error occurred"),
  program_name STRING OPTIONS(description="Name of the program or script where the error occurred"),
  error_no INT64 OPTIONS(description="Numeric error code"),
  error_arg STRING OPTIONS(description="Argument or detail related to the error_no"),
  error_message STRING OPTIONS(description="Detailed error message"),
  created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error log entry was created")
);