-- Target: BigQuery DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log`
(
    error_ts      TIMESTAMP,
    procedure_name STRING,
    err_nr        INT64,
    err_arg       STRING,
    job_kennung   STRING,
    eintrags_nr   STRING
);