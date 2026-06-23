-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- This file defines the job run audit table schema in BigQuery.

CREATE TABLE IF NOT EXISTS `project.dataset.job_run_audit` (
    audit_id STRING DEFAULT GENERATE_UUID() OPTIONS(description="Unique ID for the audit entry"),
    job_kennung STRING NOT NULL OPTIONS(description="Job Identifier for the audited run"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry Number for the audited job run"),
    tab_name STRING OPTIONS(description="Name of the table affected by the job run"),
    records_processed INT64 OPTIONS(description="Number of records processed by the job run"),
    start_timestamp TIMESTAMP OPTIONS(description="Start time of the job run"),
    end_timestamp TIMESTAMP OPTIONS(description="End time of the job run"),
    status STRING OPTIONS(description="Status of the job run (e.g., 'SUCCESS', 'FAILED')"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the audit record was created")
)
OPTIONS(
    description="Table to store audit information for job runs, including records processed."
);