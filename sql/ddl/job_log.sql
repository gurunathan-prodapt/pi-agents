-- DDL for BigQuery audit/log table
-- Legacy Source: logging within vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
  job_name STRING NOT NULL,
  log_level STRING NOT NULL,
  message STRING NOT NULL,
  created_at TIMESTAMP NOT NULL
);