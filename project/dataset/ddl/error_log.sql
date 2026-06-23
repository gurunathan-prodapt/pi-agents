-- DDL for project.dataset.error_log
-- Purpose: Centralized error logging for BigQuery jobs
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log`
(
    error_code    INT64,
    error_message STRING,
    job_kennung   STRING,
    eintrags_nr   STRING,
    created_at    TIMESTAMP
);