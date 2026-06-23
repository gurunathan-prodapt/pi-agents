-- BigQuery DDL for job_log table
-- Replaces file-based logging in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log (e.g., INFO, WARNING, ERROR)"),
    message STRING OPTIONS(description="Log message details"),
    stichtag DATE OPTIONS(description="Cutoff date relevant to the job execution"),
    wiederanlauf_wert INT64 OPTIONS(description="Restart value, if applicable"),
    status STRING OPTIONS(description="Current status of the job or step")
)
OPTIONS(
    description="Audit table to store job execution logs, status updates, and error messages."
);