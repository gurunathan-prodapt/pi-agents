-- DDL for job_log table
-- Replaces logging and control mechanisms from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
  job_entry_nr INT64 NOT NULL,
  job_name STRING NOT NULL,
  log_message STRING,
  created_at TIMESTAMP
);