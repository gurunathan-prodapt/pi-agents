--
-- DDL for the job_audit_log table, replacing filesystem-based logging for
-- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    job_name STRING NOT NULL OPTIONS(description="Identifier for the job being executed."),
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each run of the job."),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job run started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job run ended."),
    status STRING NOT NULL OPTIONS(description="Status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')."),
    message STRING OPTIONS(description="Detailed message or log entry."),
    error_message STRING OPTIONS(description="Error message if the job failed."),
    stichtag DATE OPTIONS(description="Processing date for which the job is run, if applicable."),
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of this log entry.")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_name, status;