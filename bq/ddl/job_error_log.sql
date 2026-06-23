-- BigQuery DDL for job_error_log
-- Replaces: Error logging functionality within vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log`
(
  job_name STRING NOT NULL OPTIONS(description="Name of the job that failed"),
  entry_nr STRING OPTIONS(description="Entry number associated with the job run"),
  stichtag DATE OPTIONS(description="Key date (Stichtag) for the job run"),
  error_message STRING NOT NULL OPTIONS(description="Full error message"),
  created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error was logged")
)
OPTIONS(
  description="Logs errors and exceptions from BigQuery stored procedures."
);