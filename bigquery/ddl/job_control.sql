-- DDL for job_control table
-- Replaces logging and control mechanisms from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control` (
  job_entry_nr INT64 NOT NULL,
  job_name STRING NOT NULL,
  script_name STRING,
  log_file STRING,
  status STRING,
  stichtag STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);