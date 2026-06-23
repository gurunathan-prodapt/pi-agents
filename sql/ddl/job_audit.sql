-- BigQuery DDL for the job_audit table
-- Replaces implicit job table updates from the legacy system.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit` (
    job_id STRING NOT NULL,
    entry_number STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL,
    records_processed INT66,
    error_message STRING
);