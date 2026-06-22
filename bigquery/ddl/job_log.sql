-- Target BigQuery DDL for job_log table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- Generated for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_kennung STRING,
  job_entry_nr INT64,
  log_level STRING,
  message STRING,
  log_file_name STRING,
  created_at TIMESTAMP
);