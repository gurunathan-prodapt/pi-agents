-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery DDL for the job status table.
CREATE TABLE IF NOT EXISTS `my_project_id.my_dataset_id.job_status_table` (
    job_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    last_update_ts TIMESTAMP NOT NULL
);