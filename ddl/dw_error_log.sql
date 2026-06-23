-- DDL for dw_error_log table
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.dw_error_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    error_code STRING,
    error_message STRING,
    source_component STRING,
    stack_trace STRING
);