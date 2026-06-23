-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- This file defines the job error log table schema in BigQuery.

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    log_id STRING DEFAULT GENERATE_UUID() OPTIONS(description="Unique ID for the error log entry"),
    job_kennung STRING NOT NULL OPTIONS(description="Job Identifier associated with the error"),
    eintrags_nr STRING OPTIONS(description="Entry Number for the job run where the error occurred"),
    err_nr INT64 OPTIONS(description="Error Number from the legacy system or custom error code"),
    err_arg STRING OPTIONS(description="Additional argument or context for the error"),
    error_message STRING OPTIONS(description="Detailed error message"),
    stack_trace STRING OPTIONS(description="Stack trace or additional debugging information"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the error was logged")
)
OPTIONS(
    description="Table for logging errors encountered during job execution."
);