-- BigQuery Table: project.dataset.job_log
-- Generated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
CREATE TABLE `project.dataset.job_log` (
  job_nr INT64,
  job_name STRING,
  log_level STRING,
  error_nr INT64,
  error_arg STRING,
  message STRING,
  created_at TIMESTAMP
);