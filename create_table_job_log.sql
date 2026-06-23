-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- DDL for a BigQuery job logging table.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING NOT NULL,
    record_count INT64,
    message STRING
);