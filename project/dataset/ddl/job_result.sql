-- BigQuery DDL for job_result table
-- Replaces job execution result storage from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_result` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING NOT NULL,
    records INT64 NOT NULL,
    result_ts TIMESTAMP NOT NULL
);