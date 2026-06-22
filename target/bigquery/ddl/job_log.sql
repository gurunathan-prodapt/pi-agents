--
-- DDL for BigQuery table: project.dataset.job_log
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
  job_nr INT64 OPTIONS(description="Unique job run number, often incremented from job_control"),
  job_kennung STRING OPTIONS(description="Identifier for the job type, e.g., 'ausd_bp_ta_cntrct_evn'"),
  log_ts TIMESTAMP OPTIONS(description="Timestamp when the log entry was created"),
  log_level STRING OPTIONS(description="Severity of the log entry (e.g., INFO, WARN, ERROR)"),
  message STRING OPTIONS(description="Log message text"),
  stichtag DATE OPTIONS(description="Cutoff date used for the job run"),
  restart_value INT64 OPTIONS(description="Restart value (e.g., DWH_VERTRAG_ID) used for incremental processing"),
  error_message STRING OPTIONS(description="Detailed error message if the log_level is ERROR")
)
OPTIONS(
  description="Audit log table for ETL job executions."
);