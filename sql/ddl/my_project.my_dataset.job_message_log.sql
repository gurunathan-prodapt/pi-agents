-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- BigQuery DDL for job_message_log table.
CREATE TABLE my_project.my_dataset.job_message_log (
    job_name STRING NOT NULL,
    job_entry_nr INT64 NOT NULL,
    message_text STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
);