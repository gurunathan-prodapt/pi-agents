-- DDL for project.dataset.job_log
-- Purpose: Centralized job execution logging for BigQuery jobs
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
    job_kennung  STRING,
    eintrags_nr  STRING,
    tab_name     STRING,
    record_count INT64,
    created_at   TIMESTAMP
);