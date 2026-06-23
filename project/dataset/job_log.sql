-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Purpose: Create BigQuery DDL for job_log table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  entry_no INT64,
  job_name STRING,
  log_level STRING,
  message STRING,
  stichtag STRING,
  sysdate STRING,
  created_at TIMESTAMP
);