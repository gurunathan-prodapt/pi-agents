-- BigQuery DDL for job_run_log
-- Replaces: Implied job run logging in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_run_log`
(
  tab_name STRING NOT NULL OPTIONS(description="Name of the primary table processed by the job"),
  job_kennung STRING NOT NULL OPTIONS(description="Job identifier"),
  eintrags_nr STRING OPTIONS(description="Entry number for the job run"),
  stichtag DATE OPTIONS(description="Key date (Stichtag) for the job run"),
  records_processed INT64 OPTIONS(description="Number of records processed or affected"),
  status STRING NOT NULL OPTIONS(description="Status of the job run (e.g., 'SUCCESS', 'FAILED')"),
  created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created")
)
OPTIONS(
  description="Logs successful job runs and metrics."
);