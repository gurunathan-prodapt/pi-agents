-- BigQuery Table: project.dataset.job_control
-- Generated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
CREATE TABLE `project.dataset.job_control` (
  job_nr INT64,
  job_name STRING,
  script_name STRING,
  log_file STRING,
  stichtag_info STRING,
  status STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);