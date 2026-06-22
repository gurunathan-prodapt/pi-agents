-- DDL for job_log table
-- Legacy Source: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG
-- Purpose: To store detailed log messages for job execution.

CREATE TABLE IF NOT EXISTS `project.audit_dataset.job_log` (
    log_id INT64 OPTIONS(description="Unique identifier for the log entry") DEFAULT GENERATE_UUID(),
    job_id STRING NOT NULL OPTIONS(description="Identifier of the job that generated the log"),
    log_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log (e.g., 'INFO', 'WARNING', 'ERROR')"),
    message STRING NOT NULL OPTIONS(description="Log message content")
);