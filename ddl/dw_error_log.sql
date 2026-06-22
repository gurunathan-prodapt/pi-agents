-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- This file creates the dw_error_log table for specific error details.
-- Replace 'your_gcp_project_id.your_bq_dataset_name' with your actual project ID and dataset name.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_name.dw_error_log` (
  entry_nr INT64 NOT NULL,
  job_kennung STRING NOT NULL,
  error_message STRING,
  error_code STRING,
  created_at TIMESTAMP
);