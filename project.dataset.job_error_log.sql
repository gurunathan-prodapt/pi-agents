--
-- BigQuery DDL for job_error_log table
-- Replaces error logging functionality from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
--
CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
    error_code STRING OPTIONS(description="Error code (e.g., SQLSTATE)."),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message."),
    stack_trace STRING OPTIONS(description="Optional stack trace or additional error context.")
);