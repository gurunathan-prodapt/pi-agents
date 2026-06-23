-- BigQuery DDL for job logging table
-- Replaces legacy error logging and reporting for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_log` (
    log_time TIMESTAMP NOT NULL,
    job_run_id STRING,
    job_name STRING NOT NULL,
    job_kennung STRING,
    log_level STRING NOT NULL, -- e.g., 'INFO', 'WARNING', 'ERROR'
    message STRING,
    error_details JSON -- Store JSON representation of error details if available
)
OPTIONS(
    description = "Table for logging job execution details, warnings, and errors."
);