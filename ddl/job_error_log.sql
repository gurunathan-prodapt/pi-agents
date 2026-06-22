-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh
-- Description: DDL for the job error log table in BigQuery.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_logging_dataset.job_error_log` (
    job_name STRING,
    entry_nr STRING,
    stichtag STRING,
    error_message STRING,
    created_at TIMESTAMP
);