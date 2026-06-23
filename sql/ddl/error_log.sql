-- DDL for BigQuery error log table
-- Replaces f_alis_msgerr.ksh functionality.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.error_log`
(
    job_name STRING NOT NULL,
    entry_number STRING,
    error_code INT64,
    error_message STRING NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    run_id STRING
);