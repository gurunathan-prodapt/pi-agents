-- BigQuery DDL for job_run_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
CREATE TABLE project.dataset.job_run_log (
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Job execution identifier (FK to job_control)"),
    log_level STRING NOT NULL OPTIONS(description="Log level (e.g., INFO, WARNING, ERROR)"),
    message STRING NOT NULL OPTIONS(description="Detailed log message")
)
OPTIONS(
    description="Table to store detailed job execution logs"
);