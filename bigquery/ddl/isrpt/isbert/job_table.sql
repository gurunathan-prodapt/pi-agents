-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_drop_temp_table.ksh
-- This DDL creates the BigQuery job control table for migration of k_drop_temp_table.ksh.
-- This table is intended for reactivating commented job management logic from the original script.

CREATE TABLE IF NOT EXISTS dataset.job_table (
    tab_name STRING,
    status STRING,
    mode STRING,
    stichtag_from DATE,
    stichtag_to DATE,
    job_type STRING,
    active_flag STRING,
    records INT64,
    description STRING
);