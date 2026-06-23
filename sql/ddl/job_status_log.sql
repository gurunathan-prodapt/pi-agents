-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Description: DDL for BigQuery job status log table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_status_log` (
    job_kennung STRING NOT NULL,
    job_entry_nr INT64 NOT NULL,
    status STRING NOT NULL,
    stichtag STRING,
    created_at TIMESTAMP NOT NULL
);