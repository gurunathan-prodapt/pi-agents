-- DDL for job_error_log
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_error_log` (
    error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    job_kennung STRING,
    eintrags_nr STRING,
    error_message STRING NOT NULL
);