--
-- BigQuery DDL for job_run_log table
-- Replaces logging functionality from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
--
CREATE TABLE IF NOT EXISTS project.dataset.job_run_log (
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance."),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed."),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started."),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution ended."),
    status STRING NOT NULL OPTIONS(description="Overall status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')."),
    message STRING OPTIONS(description="General message or brief summary of the job status.")
);