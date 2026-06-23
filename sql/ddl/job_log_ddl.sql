-- BigQuery DDL for job_log table
-- Replaces logging functionality of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_log` (
    job_nr INT64 NOT NULL OPTIONS(description="Unique job entry number"),
    job_kennung STRING OPTIONS(description="Job identifier from the source script"),
    script_name STRING OPTIONS(description="Name of the script executing"),
    log_identifier STRING OPTIONS(description="Identifier for grouping log entries (e.g., log file name)"),
    log_level STRING OPTIONS(description="Severity level of the log entry (e.g., INFO, WARN, ERROR)"),
    log_message STRING OPTIONS(description="Detailed log message"),
    log_ts TIMESTAMP OPTIONS(description="Timestamp when the log entry was recorded"),
    reference_date DATE OPTIONS(description="Reference date if set by the script"),
    status STRING OPTIONS(description="Overall status of the job (e.g., RUNNING, SUCCESS, FAILED)")
)
PARTITION BY DATE(log_ts)
CLUSTER BY job_nr, job_kennung, script_name;