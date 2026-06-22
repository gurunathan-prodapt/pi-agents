-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Target: BigQuery DDL for job_log table

-- This script defines the schema for the job_log table in BigQuery.
-- Replace 'project' and 'dataset' with your actual GCP project ID and BigQuery dataset name.

CREATE SCHEMA IF NOT EXISTS project.dataset; -- Ensure the dataset exists

CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution run (e.g., a UUID)"),
    job_name STRING NOT NULL OPTIONS(description="Name of the BigQuery stored procedure or job that was executed"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution officially started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution officially completed"),
    status STRING NOT NULL OPTIONS(description="Current status of the job execution (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    error_message STRING OPTIONS(description="Detailed error message if the job failed or encountered an exception"),
    parameters_json JSON OPTIONS(description="Input parameters passed to the job, stored as a JSON object"),
    caller_process STRING OPTIONS(description="Name of the orchestrator that initiated the job (e.g., 'Cloud Scheduler', 'Cloud Composer', 'Manual Run')")
)
PARTITION BY DATE(start_timestamp) -- Partitions data by date of job start for efficient querying
CLUSTER BY job_name, status -- Clusters data by job_name and status for improved query performance
OPTIONS(
    description="Table to store execution logs for BigQuery jobs, replacing legacy custom logging framework (DWMSG_)."
);