-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Description: DDL for BigQuery audit log table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    job_kennung STRING NOT NULL,
    job_entry_nr INT64 NOT NULL,
    log_level STRING NOT NULL,
    message STRING,
    stichtag STRING,
    restart_value INT64,
    created_at TIMESTAMP NOT NULL
);