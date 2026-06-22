-- BigQuery DDL for job_tracking table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- This table is used for tracking job execution details and metrics.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_tracking`
(
  timestamp         TIMESTAMP,
  job_name          STRING,
  job_kennung       STRING,
  eintrags_nr       STRING,
  stichtag          DATE,
  records_processed INT64,
  description       STRING
);