-- DDL for BigQuery error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    error_ts TIMESTAMP NOT NULL,
    error_code INT64 NOT NULL,
    error_arg STRING,
    procedure_name STRING NOT NULL,
    message STRING
);