-- DDL for job_error_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table stores error logs for job execution.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job that produced the error"),
    error_code INT64 NOT NULL OPTIONS(description="Numeric error code"),
    error_arg STRING OPTIONS(description="Argument or detail related to the error"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error was logged")
)
OPTIONS(
    description="Log table for capturing job execution errors."
);