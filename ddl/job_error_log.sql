-- DDL for job_error_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    err_nr INT64,
    err_arg STRING,
    created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);