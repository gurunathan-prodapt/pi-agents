--
-- BigQuery DDL for job_registry table.
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh
--
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_registry`
(
    job_id              STRING,      -- Unique identifier for each job execution
    job_name            STRING,      -- Name of the job, e.g., 'ausd_bp_ta_apn_vertrag'
    source_script       STRING,      -- Original legacy script name
    start_timestamp     TIMESTAMP,   -- Start time of the job execution
    end_timestamp       TIMESTAMP,   -- End time of the job execution
    status              STRING,      -- 'RUNNING', 'SUCCESS', 'FAILED'
    parameters          JSON,        -- JSON object storing input parameters (e.g., {'p_stichtag': 'DDMMYYYY', 'p_wiederanlaufWert': '0'})
    error_message       STRING       -- Detailed error message if job failed
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY job_name
OPTIONS(
    description = "Registry for tracking BigQuery job executions, replacing legacy job control."
);