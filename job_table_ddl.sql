-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- BigQuery DDL for job_table

CREATE TABLE IF NOT EXISTS project.dataset.job_table (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    status STRING,
    record_count INT64,
    created_ts TIMESTAMP,
    finished_ts TIMESTAMP
);