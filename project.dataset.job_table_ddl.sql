-- DDL for job_table
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    active_flag BOOLEAN NOT NULL,
    last_update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);