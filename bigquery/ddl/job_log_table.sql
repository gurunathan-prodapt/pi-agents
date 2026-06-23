-- DDL for job_log_table, migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh
-- This table is for logging job entries, replacing the functionality of FOSJobErzeugeEintrag.

CREATE TABLE IF NOT EXISTS `default_project.default_dataset.job_log_table` (
    job_identifier STRING NOT NULL,
    entry_number STRING,
    status_code_1 STRING,
    status_code_2 STRING,
    key_date DATE,
    restart_value INT64,
    records_processed INT64,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);