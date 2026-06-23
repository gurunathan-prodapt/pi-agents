-- BigQuery DDL for error_log table
-- Replaces error logging mechanisms from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    error_ts TIMESTAMP NOT NULL,
    error_code INT64 NOT NULL,
    error_arg STRING,
    job_kennung STRING,
    eintrags_nr STRING,
    message STRING NOT NULL
);