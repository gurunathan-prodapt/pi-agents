-- BigQuery DDL for logging tables
-- Replaces: Logging mechanisms in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset`;

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.error_log`
(
    job_kennung      STRING,
    eintrags_nr      STRING,
    error_code       INT64,
    error_argument   STRING,
    message          STRING,
    log_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log`
(
    job_kennung       STRING,
    eintrags_nr       STRING,
    start_timestamp   TIMESTAMP,
    end_timestamp     TIMESTAMP,
    status            STRING, -- 'SUCCESS' or 'FAILURE'
    records_processed INT64,
    message           STRING,
    log_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);