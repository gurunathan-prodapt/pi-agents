-- DDL for dw_job_entries table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_entries` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    job_name STRING,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING,
    message STRING
);