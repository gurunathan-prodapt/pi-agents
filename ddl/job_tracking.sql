-- BigQuery DDL for job_tracking table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- This table tracks the status and statistics of job executions, replacing commented FOS job management.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_tracking`
(
    job_name STRING NOT NULL OPTIONS(description="Name of the job"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job run started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job run (e.g., RUNNING, SUCCEEDED, FAILED)"),
    records_processed INT64 OPTIONS(description="Number of records processed or inserted by the job"),
    error_details STRING OPTIONS(description="Details of any error encountered during the job run"),
    stichtag DATE OPTIONS(description="The reference date (Stichtag) for the job run")
)
OPTIONS(
    description="Tracks the status and statistics of BigQuery job executions."
);