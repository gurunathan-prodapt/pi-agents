-- BigQuery DDL for job_log table
-- Replaces logging components from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_utils_dataset.job_log` (
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    message STRING,
    exit_code INT64
);