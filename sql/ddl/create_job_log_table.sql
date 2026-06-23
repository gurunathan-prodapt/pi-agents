-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for job log table, storing informational messages for job runs.

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    log_id INT64 OPTIONS(description="Unique identifier for each log entry."),
    job_id STRING NOT NULL OPTIONS(description="Foreign key to `job_control.job_id`."),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created."),
    log_level STRING OPTIONS(description="Severity level of the log message (e.g., 'INFO', 'WARNING', 'ERROR')."),
    message STRING OPTIONS(description="The log message content."),
    component STRING OPTIONS(description="Component generating the log message (e.g., 'orchestration', 'kernel').")
)
OPTIONS(
    description="Table for storing general informational, warning, and error messages during job execution."
);