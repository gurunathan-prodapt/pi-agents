-- BigQuery DDL for the target table sof_ta_cntrct_valid
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE SCHEMA IF NOT EXISTS `project.dataset`;
CREATE SCHEMA IF NOT EXISTS `project.isbert_schema`;
CREATE SCHEMA IF NOT EXISTS `project.source_dataset`;

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_valid`
(
    cntrct_validity_id      INT64,
    first_period_id         INT64,
    following_period_id     INT64,
    first_notice_period_id  INT64,
    follow_notice_period_id INT64,
    bfc_age                 DATETIME -- Maps from insert_at from source
);

-- Optional: DDL for job_audit_log table
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log`
(
    job_kennung     STRING,
    entry_number    STRING,
    run_timestamp   DATETIME,
    records_loaded  INT64,
    status          STRING,
    message         STRING
);