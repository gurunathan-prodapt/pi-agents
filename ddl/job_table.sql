-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh
-- Description: DDL for the job execution log table in BigQuery.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_logging_dataset.job_table` (
    job_name STRING,
    status_a STRING,
    status_i STRING,
    start_date DATE,
    end_date DATE,
    job_type STRING,
    restart_flag STRING,
    record_count INT64,
    description STRING,
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag STRING,
    wiederanlaufwert STRING,
    created_at TIMESTAMP
);