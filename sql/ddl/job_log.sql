-- DDL for job_log, replacing temporary file mechanism in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    status STRING NOT NULL,
    record_count INT64,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP
);