-- DDL for job_log table
-- Legacy source: N/A (audit table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh)
CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
    job_run_id      STRING      NOT NULL OPTIONS(description="Unique identifier for the job execution"),
    log_timestamp   TIMESTAMP   NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level       STRING      NOT NULL OPTIONS(description="Level of the log entry (INFO, WARN, ERROR)"),
    message         STRING      NOT NULL OPTIONS(description="Log message content")
)
OPTIONS(
    description="Audit table to store detailed log messages for job executions"
);