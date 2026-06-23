-- Target: BigQuery DDL for job_run_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_run_log`
(
    log_ts           TIMESTAMP,
    procedure_name   STRING,
    job_kennung      STRING,
    eintrags_nr      STRING,
    tab_name         STRING,
    records_processed INT64,
    status           STRING
);