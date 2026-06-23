-- BigQuery DDL for the job_audit table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table stores execution metrics, replacing the temporary file-based record count.
--
-- Please replace `project.dataset` with your actual GCP Project ID and BigQuery Dataset ID.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag STRING, -- Original Stichtag string (DDMMYYYY)
    records INT64,
    created_at TIMESTAMP,
    tab_name STRING
);