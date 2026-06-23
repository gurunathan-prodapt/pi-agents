-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Purpose: To store job execution details, parameters, and logging messages.
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_nr INT64,
  job_name STRING,
  job_status STRING,
  log_ts TIMESTAMP,
  stichtag STRING,
  restart_value INT64,
  message STRING
);