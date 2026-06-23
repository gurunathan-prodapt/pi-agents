-- BigQuery DDL for the job_log table
-- Replaces logging functionality of legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_name STRING NOT NULL,
  job_version STRING,
  job_number INT64,
  log_level STRING NOT NULL,
  log_message STRING,
  created_at TIMESTAMP NOT NULL,
  error_code STRING,
  error_arg STRING
);