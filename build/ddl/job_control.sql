-- BigQuery DDL for job control table
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_control` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'INACTIVE'
    record_count INT64,
    error_message STRING
);