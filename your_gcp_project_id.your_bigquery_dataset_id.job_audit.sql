-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for job_audit table for BigQuery migration.
-- This table records audit information and metrics for completed jobs.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_audit` (
    audit_id STRING NOT NULL OPTIONS(description="Unique identifier for each audit entry."),
    job_run_id STRING OPTIONS(description="Reference to job_control.job_run_id for the job instance."),
    job_name STRING NOT NULL OPTIONS(description="Name of the job or stored procedure."),
    job_kennung STRING OPTIONS(description="Business identifier for the job, analogous to p_JobKennung."),
    eintrags_nr INT64 OPTIONS(description="Entry number or identifier, analogous to p_EintragsNr."),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the audited part of the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the audited part of the job completed."),
    status STRING NOT NULL OPTIONS(description="Final status of the audited component: 'COMPLETED', 'FAILED'."),
    processed_records INT64 OPTIONS(description="Number of records processed by the job, if applicable."),
    message STRING OPTIONS(description="Optional message or summary for the audit entry.")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_name, job_kennung, eintrags_nr
OPTIONS(
    description="Table to store audit and metric information for job runs, including processed record counts."
);