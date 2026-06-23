-- BigQuery DDL for the error logging table
-- Replaces error handling in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE SCHEMA IF NOT EXISTS prod_dw_logs;

CREATE TABLE IF NOT EXISTS prod_dw_logs.error_log
(
    log_timestamp TIMESTAMP OPTIONS(description="Timestamp of the error log entry"),
    job_id STRING OPTIONS(description="Identifier of the job that produced the error"),
    source_script STRING OPTIONS(description="Name of the script/procedure where the error occurred"),
    error_number INT64 OPTIONS(description="Numeric error code (if applicable)"),
    error_argument STRING OPTIONS(description="Additional argument related to the error"),
    error_message STRING OPTIONS(description="Detailed error message")
)
OPTIONS(
    description="Table to log errors from migrated processes."
);