-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
  job_nr INT64,
  job_kennung STRING,
  script_name STRING,
  log_file STRING,
  sysdate DATE,
  stichtag DATE,
  restart_value INT64,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);