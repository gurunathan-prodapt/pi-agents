-- BigQuery DDL for job_control table
-- Replaces job tracking mechanisms from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING NOT NULL,
    status STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL,
    finished_ts TIMESTAMP,
    record_count INT64
);