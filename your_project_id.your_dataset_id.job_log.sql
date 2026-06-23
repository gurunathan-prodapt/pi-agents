--
-- Target BigQuery DDL for job_log table
-- Replaces: temporary file logging and potential job management system interactions
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
--

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
    job_name STRING,
    entry_nr STRING,
    stichtag STRING,
    restart_value STRING,
    records_processed INT64,
    status STRING,
    created_at TIMESTAMP,
    error_message STRING
);