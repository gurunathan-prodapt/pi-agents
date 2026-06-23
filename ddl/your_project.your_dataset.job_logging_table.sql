-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery DDL for job logging table.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_logging_table` (
    job_entry_nr INT64 NOT NULL,
    job_key STRING NOT NULL,
    log_level STRING,
    message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status STRING
);