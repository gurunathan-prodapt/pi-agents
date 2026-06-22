-- BigQuery DDL for logging and control tables
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh

CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_kennung STRING,
    eintrags_nr INT64,
    log_level STRING,
    message STRING,
    created_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING,
    eintrags_nr INT64,
    error_nr INT64,
    error_arg STRING,
    error_message STRING,
    created_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project.dataset.job_control (
    eintrags_nr INT64,
    job_kennung STRING,
    script_name STRING,
    log_name STRING,
    stichtag_info DATE,
    status STRING,
    created_ts TIMESTAMP,
    finished_ts TIMESTAMP
);