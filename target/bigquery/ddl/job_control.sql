--
-- DDL for BigQuery table: project.dataset.job_control
-- Replaces job control functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_control`
(
  job_nr INT64 NOT NULL OPTIONS(description="Unique job run number"),
  job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type, e.g., 'ausd_bp_ta_cntrct_evn'"),
  start_ts TIMESTAMP OPTIONS(description="Timestamp when the job run started"),
  end_ts TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
  status STRING OPTIONS(description="Current status of the job run (e.g., RUNNING, OK, ERROR)"),
  error_message STRING OPTIONS(description="Error message if the job failed")
)
OPTIONS(
  description="Control table for tracking ETL job executions and their status."
);