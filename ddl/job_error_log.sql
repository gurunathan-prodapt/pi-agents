-- BigQuery DDL for job_error_log table
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
  job_name STRING,
  error_nr INT64,
  error_arg STRING,
  created_at TIMESTAMP
);