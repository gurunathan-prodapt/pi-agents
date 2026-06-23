-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

-- DDL for job control and logging tables in Google BigQuery.

CREATE TABLE IF NOT EXISTS `project.admin_dataset.job_control`
(
    job_name STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCEEDED', 'FAILED'
    run_id STRING NOT NULL,
    stichtag DATE,
    wiederanlaufwert INT64,
    message STRING
)
;

CREATE TABLE IF NOT EXISTS `project.admin_dataset.job_log`
(
    log_time TIMESTAMP NOT NULL,
    job_name STRING NOT NULL,
    run_id STRING NOT NULL,
    level STRING NOT NULL, -- e.g., 'INFO', 'WARNING', 'ERROR'
    message STRING NOT NULL
)
;