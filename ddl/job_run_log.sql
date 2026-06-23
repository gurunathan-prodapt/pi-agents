-- BigQuery DDL for job_run_log table
-- Replaces detailed logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_run_log` (
  job_id INT64,
  job_name STRING,
  log_file STRING,
  stichtag STRING,
  sysdate_value STRING,
  status STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);