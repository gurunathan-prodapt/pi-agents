-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- BigQuery DDL for job_error_log

CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    err_nr STRING,
    err_arg STRING,
    created_ts TIMESTAMP,
    message STRING
);