-- BigQuery DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- This table logs errors and messages from job executions.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_error_log`
(
    job_name STRING NOT NULL OPTIONS(description="Name of the job that produced the error"),
    run_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    severity STRING NOT NULL OPTIONS(description="Severity of the log entry (e.g., INFO, WARNING, ERROR)"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    stack_trace STRING OPTIONS(description="Full stack trace if available"),
    input_parameters JSON OPTIONS(description="JSON representation of input parameters at the time of error")
)
OPTIONS(
    description="Logs errors and important messages from BigQuery job executions."
);