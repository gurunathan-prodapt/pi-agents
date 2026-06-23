-- DDL for job_run_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table stores success logs and execution details for job runs.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_run_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job that ran"),
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job instance"),
    entry_nr STRING OPTIONS(description="Entry number for the job, if applicable"),
    business_date DATE NOT NULL OPTIONS(description="Business date for which the job was run"),
    record_count INT64 OPTIONS(description="Number of records processed or inserted"),
    status STRING NOT NULL OPTIONS(description="Status of the job run (e.g., 'SUCCESS', 'FAILURE')"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the run was logged")
)
OPTIONS(
    description="Log table for capturing successful job execution details."
);