-- DDL for project_id.dataset_id.job_control
-- Replaces job control logic for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_control` (
    job_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    stichtag DATE,
    resume_value INT64,
    status STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP
);