-- Legacy Source: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Job: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Description: DDL for the BigQuery job audit table.

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_audit_table` (
    job_identifier STRING NOT NULL OPTIONS(description="Unique identifier for the job (p_JobKennung)"),
    entry_number STRING OPTIONS(description="Entry number for the job (p_EintragsNr)"),
    as_of_date DATE OPTIONS(description="As-of date for data processing (p_Stichtag)"),
    restart_value INT64 OPTIONS(description="Restart value (p_wiederanlaufWert)"),
    start_timestamp TIMESTAMP OPTIONS(description="Job start timestamp"),
    end_timestamp TIMESTAMP OPTIONS(description="Job end timestamp"),
    status STRING OPTIONS(description="Job status (e.g., 'SUCCESS', 'FAILED')"),
    error_message STRING OPTIONS(description="Error message if job failed"),
    records_processed INT64 OPTIONS(description="Number of records processed"),
    audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of audit record creation")
);