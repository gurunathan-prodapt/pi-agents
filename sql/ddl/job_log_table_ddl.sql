-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery DDL for the job logging table.
CREATE TABLE IF NOT EXISTS `my_project_id.my_dataset_id.job_log_table` (
    job_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    log_message STRING,
    log_ts TIMESTAMP NOT NULL,
    severity STRING NOT NULL -- e.g., 'INFO', 'WARNING', 'ERROR'
);