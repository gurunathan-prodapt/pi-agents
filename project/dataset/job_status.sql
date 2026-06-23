-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Purpose: Create BigQuery DDL for job_status table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
  entry_no INT64,
  job_name STRING,
  status STRING,
  updated_at TIMESTAMP
);