-- DDL for BigQuery job_table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    active_flag BOOLEAN NOT NULL,
    created_ts TIMESTAMP NOT NULL,
    updated_ts TIMESTAMP,
    completed_ts TIMESTAMP,
    record_count INT64
);