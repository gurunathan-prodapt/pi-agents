-- DDL for job_run_result table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_run_result` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    record_count INT64,
    created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);