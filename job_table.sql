-- DDL for job_table, part of migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_kennung STRING,
    eintrags_nr STRING,
    tab_name STRING,
    status STRING,
    record_count INT64,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
);