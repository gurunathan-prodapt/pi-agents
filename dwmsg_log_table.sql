-- DDL for project.dataset.dwmsg_log
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Purpose: Captures job execution logs and status.

CREATE TABLE IF NOT EXISTS project.dataset.dwmsg_log (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for the log entry (e.g., UUID)"),
    job_name STRING NOT NULL OPTIONS(description="Name of the executed job"),
    run_id STRING OPTIONS(description="Unique identifier for a specific job execution run"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job execution started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING OPTIONS(description="Status of the job execution (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="General message or description of the log entry"),
    parameters JSON OPTIONS(description="JSON representation of input parameters used for the job run"),
    error_details STRING OPTIONS(description="Detailed error message if the job failed"),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the log entry was created")
)
OPTIONS(
    description="Table to store execution logs and status for BigQuery jobs."
);