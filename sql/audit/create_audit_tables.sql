-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This file creates the BigQuery audit and logging tables.

-- Create job_control table
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_control` (
    job_key STRING NOT NULL OPTIONS(description="Unique identifier for the job, e.g., BERT_V_TA_CNTRCT_CRS2"),
    entry_number INT64 NOT NULL OPTIONS(description="Unique execution identifier for a given job_key"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING OPTIONS(description="Current status of the job execution (e.g., RUNNING, OK, ERROR)"),
    parameters JSON OPTIONS(description="JSON representation of input parameters received by the job"),
    program_name STRING OPTIONS(description="Name of the program, e.g., 'Vertragsdatenabgleich'"),
    sysdate_info DATE OPTIONS(description="Processing date for the job (DDMMYYYY format)"),
    PRIMARY KEY(job_key, entry_number) NOT ENFORCED
);

-- Create job_log table
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_log` (
    job_key STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    entry_number INT64 NOT NULL OPTIONS(description="Unique execution identifier for a given job_key"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    message STRING NOT NULL OPTIONS(description="Log message content"),
    log_level STRING OPTIONS(description="Severity level of the log message (e.g., INFO, WARNING, ERROR)")
);

-- Create job_error_log table
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (
    job_key STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    entry_number INT64 NOT NULL OPTIONS(description="Unique execution identifier for a given job_key"),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    error_code STRING OPTIONS(description="Numeric or descriptive error code"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    stack_trace STRING OPTIONS(description="Stack trace or additional error context")
);