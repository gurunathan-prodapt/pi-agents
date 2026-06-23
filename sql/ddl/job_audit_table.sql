-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset>.job_audit_table` (
    job_id STRING NOT NULL OPTIONS(description="Identifier for the ETL job."),
    entry_number STRING OPTIONS(description="Unique entry number for the job run."),
    stichtag DATE OPTIONS(description="Reference date for the data being processed."),
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the job started."),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    status STRING OPTIONS(description="Status of the job run (e.g., 'SUCCESS', 'FAILED')."),
    record_count INT64 OPTIONS(description="Number of records processed or inserted."),
    error_message STRING OPTIONS(description="Detailed error message if the job failed.")
)
OPTIONS(
    description="Audit table to track ETL job executions, similar to FOSJobErzeugeEintrag."
);