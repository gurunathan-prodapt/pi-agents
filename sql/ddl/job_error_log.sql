--
-- BigQuery DDL for job_error_log table
-- Replaces filesystem-based error logging from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job where the error occurred"),
    job_entry_number STRING NOT NULL OPTIONS(description="Unique identifier for the job execution instance"),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    error_code STRING OPTIONS(description="Custom error code if applicable"),
    stack_trace STRING OPTIONS(description="Stack trace or further error details"),
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the error log entry was inserted")
);