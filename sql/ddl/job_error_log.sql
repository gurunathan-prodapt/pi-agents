--
-- BigQuery DDL for job_error_log
-- Replaces: logging functionality in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error_log` (
  job_kennung STRING,
  eintrags_nr STRING,
  err_nr INT64,
  err_arg STRING,
  created_ts TIMESTAMP
);