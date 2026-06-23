-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Target: BigQuery DDL for job_messages table
CREATE TABLE IF NOT EXISTS `project.dataset.job_messages` (
    job_entry_nr INT64,
    job_name STRING,
    message_type STRING,
    message_text STRING,
    created_at TIMESTAMP
);