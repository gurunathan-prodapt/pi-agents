-- DDL for job_status table
-- Legacy Source: Part of the custom DWMSG_ logging framework in r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_status` (
    job_name STRING NOT NULL OPTIONS(description="Identifier for the ETL job"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Unique entry number for a specific job run"),
    status STRING NOT NULL OPTIONS(description="Current status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    updated_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job status was last updated")
);