-- DDL for job_error_log table
-- Legacy source: N/A (audit table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh)
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log`
(
    job_run_id      STRING      NOT NULL OPTIONS(description="Unique identifier for the job execution"),
    error_timestamp TIMESTAMP   NOT NULL OPTIONS(description="Timestamp of the error"),
    job_name        STRING      NOT NULL OPTIONS(description="Name of the job/stored procedure where error occurred"),
    error_message   STRING      NOT NULL OPTIONS(description="Detailed error message"),
    stack_trace     STRING              OPTIONS(description="Optional stack trace or additional error details")
)
OPTIONS(
    description="Audit table to specifically capture error events for job executions"
);