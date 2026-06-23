--
-- BigQuery DDL for job_audit_log table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for each log entry (UUID)."),
    job_nr INT64 OPTIONS(description="Reference to the job_control.job_nr."),
    job_kennung STRING OPTIONS(description="Identifier for the job/stored procedure."),
    log_file_name STRING OPTIONS(description="Placeholder for legacy log file name."),
    stichtag STRING OPTIONS(description="The effective cutoff date used for the job (DDMMYYYY)."),
    sysdate STRING OPTIONS(description="The system date when the job started (DDMMYYYY)."),
    message STRING OPTIONS(description="Detailed log message."),
    created_at TIMESTAMP OPTIONS(description="Timestamp when the log entry was created.")
);