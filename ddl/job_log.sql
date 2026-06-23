-- DDL for project_id.dataset_id.job_log
-- Replaces filesystem logging for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_log` (
    job_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    log_level STRING NOT NULL,
    message STRING,
    created_at TIMESTAMP NOT NULL
);