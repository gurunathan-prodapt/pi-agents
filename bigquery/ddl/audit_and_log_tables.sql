-- BigQuery DDL for audit and error log tables
-- Replaces logging and audit mechanisms from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE SCHEMA IF NOT EXISTS project.dataset;

CREATE TABLE IF NOT EXISTS project.dataset.error_log (
    log_timestamp TIMESTAMP OPTIONS(description="Timestamp of the error"),
    job_name STRING OPTIONS(description="Name of the job that encountered the error"),
    error_message STRING OPTIONS(description="Detailed error message"),
    severity STRING OPTIONS(description="Severity of the error (e.g., ERROR, WARNING)"),
    additional_info JSON OPTIONS(description="Additional JSON information related to the error")
)
OPTIONS(
    description="Table for logging application errors and warnings."
);

CREATE TABLE IF NOT EXISTS project.dataset.job_audit (
    audit_timestamp TIMESTAMP OPTIONS(description="Timestamp of the job completion or audit event"),
    job_name STRING OPTIONS(description="Name of the job being audited"),
    job_id STRING OPTIONS(description="Unique identifier for the job run"),
    entry_number STRING OPTIONS(description="Entry number parameter for the job"),
    reference_date DATE OPTIONS(description="Reference date used for the job execution"),
    target_table STRING OPTIONS(description="Name of the primary target table affected by the job"),
    processed_record_count INT64 OPTIONS(description="Number of records processed or inserted/updated"),
    status STRING OPTIONS(description="Status of the job execution (e.g., SUCCESS, FAILED)"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended")
)
OPTIONS(
    description="Table for auditing job executions and their outcomes."
);