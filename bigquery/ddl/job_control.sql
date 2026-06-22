-- Target BigQuery DDL for job_control table
-- Replaces job status tracking functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- Generated for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
  job_kennung STRING,
  job_entry_nr INT64,
  stichtag STRING,
  stichtag_format STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);