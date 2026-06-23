-- DDL for job_log table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- This table will store execution logs for the migrated BigQuery Stored Procedure.
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.job_log` (
    tab_name STRING,
    status STRING,
    mode STRING,
    stichtag_from DATE,
    stichtag_to DATE,
    job_type STRING,
    restart_flag STRING,
    record_count INT64,
    description STRING,
    job_kennung STRING,
    eintragsnr STRING,
    created_at TIMESTAMP
);