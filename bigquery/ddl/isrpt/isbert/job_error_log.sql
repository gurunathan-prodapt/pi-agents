-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh
-- This DDL creates the BigQuery logging table for migration of k_drop_temp_table.ksh.

CREATE TABLE IF NOT EXISTS dataset.job_error_log (
    job_name STRING,
    error_nr INT64,
    error_arg STRING,
    created_at TIMESTAMP
);