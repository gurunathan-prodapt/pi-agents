-- DDL for job_table, replacing implicit job management in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    active_flag BOOL NOT NULL,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP
);