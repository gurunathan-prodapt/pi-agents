-- DDL for job_log_detail table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE TABLE project.dataset.job_log_detail (
    detail_id INT64 NOT NULL OPTIONS(description="Auto-incrementing unique identifier for each log detail entry"),
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for a job run, references job_log.job_id"),
    timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log message (e.g., 'INFO', 'WARNING', 'ERROR')"),
    message STRING NOT NULL OPTIONS(description="The log message content"),
    source_procedure STRING OPTIONS(description="The procedure or function that generated the log entry")
)
OPTIONS(
    description="Table for granular log messages and events during job execution"
);