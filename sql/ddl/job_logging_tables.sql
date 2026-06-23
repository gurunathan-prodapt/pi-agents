-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
-- Target: BigQuery DDL for logging and audit tables

-- =========================================================
-- 1) Job Audit Table
-- =========================================================
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
  job_id STRING NOT NULL OPTIONS(description='Unique identifier for each job run'),
  start_timestamp TIMESTAMP NOT NULL OPTIONS(description='Timestamp when the job started'),
  end_timestamp TIMESTAMP OPTIONS(description='Timestamp when the job ended'),
  status STRING NOT NULL OPTIONS(description='Current status of the job (e.g., RUNNING, SUCCESS, FAILED)'),
  message STRING OPTIONS(description='Summary message for the job status'),
  parameters JSON OPTIONS(description='JSON representation of input parameters for the job'),
  source_job_name STRING OPTIONS(description='Name of the original legacy job (e.g., r_ausd_v_ta_inv_acc.ksh)')
)
OPTIONS (
  description = 'Audit table for tracking the lifecycle and status of BigQuery jobs.'
);

-- =========================================================
-- 2) Job Error Log Table
-- =========================================================
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
  job_id STRING NOT NULL OPTIONS(description='Foreign key referencing job_audit.job_id'),
  error_timestamp TIMESTAMP NOT NULL OPTIONS(description='Timestamp when the error occurred'),
  error_message STRING NOT NULL OPTIONS(description='Detailed error message'),
  error_details STRING OPTIONS(description='Stack trace or additional error context'),
  component STRING OPTIONS(description='Component where the error occurred (e.g., wrapper, core_logic)')
)
OPTIONS (
  description = 'Table for logging detailed error information from job executions.'
);

-- =========================================================
-- 3) Job Log Table
-- =========================================================
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_id STRING NOT NULL OPTIONS(description='Foreign key referencing job_audit.job_id'),
  log_timestamp TIMESTAMP NOT NULL OPTIONS(description='Timestamp when the log entry was created'),
  log_level STRING NOT NULL OPTIONS(description='Severity level of the log entry (e.g., INFO, WARN, DEBUG)'),
  message STRING NOT NULL OPTIONS(description='Informational log message'),
  component STRING OPTIONS(description='Component generating the log entry (e.g., wrapper, core_logic)')
)
OPTIONS (
  description = 'Table for general informational logging of job execution events.'
);