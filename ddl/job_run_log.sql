-- DDL for job_run_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_run_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    script_name STRING,
    status STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);