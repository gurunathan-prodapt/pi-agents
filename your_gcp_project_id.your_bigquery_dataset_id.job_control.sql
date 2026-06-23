-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for job_control table for BigQuery migration.
-- This table tracks the status of jobs.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_control` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run instance, e.g., generated UUID."),
    job_name STRING NOT NULL OPTIONS(description="Name of the job or stored procedure being executed."),
    job_kennung STRING OPTIONS(description="Business identifier for the job, analogous to p_JobKennung in legacy system."),
    eintrags_nr INT64 OPTIONS(description="Entry number or identifier, analogous to p_EintragsNr in legacy system."),
    status STRING NOT NULL OPTIONS(description="Current status of the job: 'STARTING', 'RUNNING', 'COMPLETED', 'FAILED', 'IGNORED'."),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job completed or failed."),
    last_updated TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update to this record."),
    message STRING OPTIONS(description="Optional message or details regarding the job status.")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_name, job_kennung, eintrags_nr
OPTIONS(
    description="Table to control and monitor the status of data processing jobs, replacing legacy job control mechanisms."
);