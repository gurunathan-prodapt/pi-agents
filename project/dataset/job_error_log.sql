-- BigQuery DDL for job_error_log table
-- Replaces: part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
CREATE TABLE project.dataset.job_error_log (
  job_kennung STRING,
  entry_nr INT64,
  error_nr INT64,
  error_arg STRING,
  error_message STRING,
  created_ts TIMESTAMP
);