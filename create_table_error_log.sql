-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- DDL for a BigQuery error logging table.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.error_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    error_message STRING NOT NULL,
    stack_trace STRING
);