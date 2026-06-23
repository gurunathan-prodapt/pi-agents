-- BigQuery DDL for job_control table
-- Replaces: part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
CREATE TABLE project.dataset.job_control (
  entry_nr INT64,
  job_kennung STRING,
  script_name STRING,
  log_file STRING,
  status STRING,
  stichtag_info STRING,
  created_ts TIMESTAMP,
  finished_ts TIMESTAMP
);