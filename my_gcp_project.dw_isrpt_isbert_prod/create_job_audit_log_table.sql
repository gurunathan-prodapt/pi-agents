-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

-- Create table for detailed audit trail messages.
CREATE TABLE IF NOT EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_audit_log`
(
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for each log entry"),
    job_run_id STRING NOT NULL OPTIONS(description="Foreign key to job_registry.job_run_id"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING OPTIONS(description="Severity level of the log (INFO, WARN, ERROR, DEBUG)"),
    message STRING NOT NULL OPTIONS(description="Log message content"),
    component STRING OPTIONS(description="Component or function generating the log entry"),
    line_number INT OPTIONS(description="Line number in the source code (if applicable)"),
    error_code INT OPTIONS(description="Specific error code related to the message"),
    error_args STRING OPTIONS(description="Arguments associated with the error code")
)
PARTITION BY
    DATE_TRUNC(log_timestamp, DAY)
CLUSTER BY
    job_run_id, log_level
OPTIONS(
    description = "Detailed audit log for ETL job executions."
);