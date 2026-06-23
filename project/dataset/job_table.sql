-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh
-- Description: DDL for the job status tracking table in BigQuery.

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
  job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job."),
  eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for a job run."),
  script_name STRING NOT NULL OPTIONS(description="Name of the script being executed."),
  tab_name STRING OPTIONS(description="Name of the primary target table for data manipulation."),
  status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'DONE', 'DEACTIVATED', 'FAILED')."),
  active_flag BOOL NOT NULL OPTIONS(description="Indicates if the job entry is currently active."),
  created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job record was created."),
  updated_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job record was last updated."),
  record_count INT64 OPTIONS(description="Number of records processed or affected by the job."),
  error_message STRING OPTIONS(description="Error message if the job failed.")
);