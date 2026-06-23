-- BigQuery DDL for the job tracking table
-- Replaces (optional) job tracking in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE SCHEMA IF NOT EXISTS prod_dw_logs;

CREATE TABLE IF NOT EXISTS prod_dw_logs.job_tracking
(
    track_timestamp TIMESTAMP OPTIONS(description="Timestamp of the job tracking entry"),
    job_id STRING OPTIONS(description="Identifier of the job"),
    entry_number STRING OPTIONS(description="Entry number for the job"),
    table_name STRING OPTIONS(description="Name of the main table processed"),
    status STRING OPTIONS(description="Status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')"),
    start_date DATE OPTIONS(description="Start date parameter for the job"),
    end_date DATE OPTIONS(description="End date parameter for the job (often same as start_date)"),
    record_count INT64 OPTIONS(description="Number of records processed/inserted"),
    notes STRING OPTIONS(description="Additional notes or description for the job status")
)
OPTIONS(
    description="Table to track the execution and status of migrated jobs."
);