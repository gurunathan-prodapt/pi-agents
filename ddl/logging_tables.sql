-- Legacy Source: DDL for logging tables for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh

-- IMPORTANT: Replace `project_id.dataset_id` with your actual Google Cloud Project ID and BigQuery Dataset ID.

-- Table to track job status and metadata
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_control` (
    job_id STRING NOT NULL,
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    parameters JSON,
    error_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table for detailed job execution logs
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_log` (
    log_id STRING NOT NULL,
    job_id STRING NOT NULL,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    log_level STRING, -- e.g., 'INFO', 'WARNING', 'ERROR'
    message STRING,
    step STRING,
    details JSON
);

-- Table for capturing error details
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_error_log` (
    error_id STRING NOT NULL,
    job_id STRING NOT NULL,
    error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    error_code STRING,
    error_message STRING,
    error_details JSON,
    stack_trace STRING
);

-- Table for general job messages (e.g., specific business events)
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_message_log` (
    message_id STRING NOT NULL,
    job_id STRING NOT NULL,
    message_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    message_type STRING, -- e.g., 'START', 'END', 'STATUS', 'DATA_COUNT'
    message STRING,
    details JSON
);