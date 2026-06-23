-- DDL for error_log table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- This table will store error messages for the migrated BigQuery Stored Procedure.
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.error_log` (
    job_name STRING,
    error_nr INT64,
    error_arg STRING,
    error_message STRING,
    created_at TIMESTAMP
);