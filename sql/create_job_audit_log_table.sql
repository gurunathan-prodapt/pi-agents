-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- This file creates the job_audit_log table required by the BigQuery stored procedure.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_audit_log`
(
    job_name STRING NOT NULL OPTIONS(description="Name of the job or stored procedure"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="Descriptive message or error details"),
    parameters JSON OPTIONS(description="JSON representation of input parameters"),
    inserted_rows INT64 OPTIONS(description="Number of rows inserted by the job (if applicable)"),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of last update to this log entry")
)
OPTIONS(
    description="Audit log table for tracking job executions within the data platform"
);