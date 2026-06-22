--
-- BigQuery DDL for job_audit_log table
-- Replaces file-based logging and DWMSG_* functions from
-- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
--

CREATE TABLE IF NOT EXISTS `my_gcp_project.my_bq_dataset.job_audit_log`
(
    job_id STRING OPTIONS(description="Unique identifier for the job instance (e.g., job_kennung)"),
    entry_number INT64 OPTIONS(description="Sequential entry number for log messages within a job run"),
    log_file_name STRING OPTIONS(description="Original log file name for reference"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    error_code STRING OPTIONS(description="Custom error code if an error occurred (e.g., '192', '193')"),
    error_message STRING OPTIONS(description="Detailed error message"),
    stichtag_info STRING OPTIONS(description="Information related to the 'Stichtag' parameter (date of record)"),
    parameters JSON OPTIONS(description="Input parameters provided to the job (e.g., -s, -l)"),
    message STRING OPTIONS(description="General log message or description of event"),
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when this log entry was recorded")
)
PARTITION BY DATE(logged_at)
CLUSTER BY job_id, status
OPTIONS(
    description="Audit log for BigQuery job executions, replacing legacy shell script logging."
);